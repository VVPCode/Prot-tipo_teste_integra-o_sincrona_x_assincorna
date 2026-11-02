"""Amostragem de consumo de recursos e de ocupacao da fila durante a medicao.

Consumo de recursos: uso de CPU e de memoria dos conteineres do produtor, do
consumidor, do RabbitMQ, do PostgreSQL e do gerador de carga (Quadro 5),
amostrado a cada segundo. O comando `docker stats` em modo continuo entrega uma
atualizacao por segundo; `--no-stream` levaria de 2 a 5 segundos por amostra, o
que nao atenderia a cadencia que o artigo declara.

Ocupacao da fila: mensagens prontas e nao confirmadas, obtidas da API de
gerenciamento do RabbitMQ. Nos cenarios C6 e C7 essa serie e o que torna
visivel o acumulo durante o pico ou a interrupcao.

Uso: python coleta/amostrar.py --execucao-id C3_sincrono_500b_r1 --intervalo 1
Encerramento: sinal de termino (cada amostra e descarregada ao ser lida).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

FORMATO = "{{.Name}};{{.CPUPerc}};{{.MemUsage}};{{.MemPerc}}"
# O docker stats em modo continuo redesenha a tela com sequencias de controle.
ANSI = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]")
_encerrar = False


def _tratar_sinal(*_args) -> None:
    global _encerrar
    _encerrar = True


def _para_mib(texto: str) -> float:
    texto = texto.strip()
    unidades = {
        "B": 1 / 1048576, "KIB": 1 / 1024, "MIB": 1.0, "GIB": 1024.0,
        "KB": 1 / 1024, "MB": 1.0, "GB": 1024.0,
    }
    for sufixo, fator in sorted(unidades.items(), key=lambda p: -len(p[0])):
        if texto.upper().endswith(sufixo):
            try:
                return float(texto[: -len(sufixo)].strip()) * fator
            except ValueError:
                return -1.0
    return -1.0


def _interpretar(linha: str):
    """Converte uma linha de `docker stats` em (nome, cpu, mem_mib, mem_pct)."""
    linha = ANSI.sub("", linha).strip()
    partes = linha.split(";")
    if len(partes) != 4:
        return None
    nome, cpu, mem, memperc = partes
    nome = nome.strip()
    if not nome or nome.upper().startswith("NAME"):
        return None
    try:
        cpu_v = float(cpu.strip().rstrip("%"))
    except ValueError:
        return None
    try:
        memperc_v = float(memperc.strip().rstrip("%"))
    except ValueError:
        memperc_v = -1.0
    return nome, cpu_v, _para_mib(mem.split("/")[0]), memperc_v


class FluxoRecursos:
    """Le `docker stats` em modo continuo em uma linha de execucao propria.

    O comando emite um bloco por segundo com uma linha por conteiner. As linhas
    sao acumuladas e entregues ao chamador, que as grava com o instante da
    leitura. Quando o conjunto de conteineres muda, como na interrupcao do
    consumidor em C7, o proprio comando passa a emitir menos linhas.
    """

    def __init__(self) -> None:
        self._processo: subprocess.Popen | None = None
        self._amostras: list[tuple[float, str, float, float, float]] = []
        self._trava = threading.Lock()
        self._linha = None

    def iniciar(self) -> bool:
        try:
            self._processo = subprocess.Popen(
                ["docker", "stats", "--format", FORMATO],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, bufsize=1,
            )
        except FileNotFoundError:
            return False
        self._linha = threading.Thread(target=self._ler, daemon=True)
        self._linha.start()
        return True

    def _ler(self) -> None:
        assert self._processo is not None and self._processo.stdout is not None
        for linha in self._processo.stdout:
            if _encerrar:
                break
            interpretado = _interpretar(linha)
            if interpretado is None:
                continue
            nome, cpu, mem, memperc = interpretado
            with self._trava:
                self._amostras.append((time.time() * 1000.0, nome, cpu, mem, memperc))

    def coletar(self):
        with self._trava:
            amostras, self._amostras = self._amostras, []
        return amostras

    def encerrar(self) -> None:
        if self._processo is not None:
            self._processo.terminate()
            self._processo = None


def amostrar_recursos_pontual():
    """Alternativa quando o modo continuo nao esta disponivel."""
    try:
        saida = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", FORMATO],
            capture_output=True, text=True, timeout=30, check=False,
        ).stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []
    agora = time.time() * 1000.0
    linhas = []
    for linha in saida.strip().splitlines():
        interpretado = _interpretar(linha)
        if interpretado:
            nome, cpu, mem, memperc = interpretado
            linhas.append((agora, nome, cpu, mem, memperc))
    return linhas


def amostrar_fila(base: str, usuario: str, senha: str, filas: list[str]):
    credencial = base64.b64encode(f"{usuario}:{senha}".encode()).decode()
    resultado = []
    for fila in filas:
        url = f"{base}/api/queues/%2F/{fila}"
        pedido = urllib.request.Request(url, headers={"Authorization": f"Basic {credencial}"})
        try:
            with urllib.request.urlopen(pedido, timeout=5) as resposta:
                dados = json.load(resposta)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            continue
        resultado.append(
            (
                fila,
                int(dados.get("messages", 0) or 0),
                int(dados.get("messages_ready", 0) or 0),
                int(dados.get("messages_unacknowledged", 0) or 0),
            )
        )
    return resultado


def main() -> int:
    sinal_disponivel = getattr(signal, "SIGTERM", None)
    if sinal_disponivel is not None:
        signal.signal(sinal_disponivel, _tratar_sinal)
    signal.signal(signal.SIGINT, _tratar_sinal)

    p = argparse.ArgumentParser()
    p.add_argument("--execucao-id", required=True)
    p.add_argument("--dir", default="resultados")
    p.add_argument("--intervalo", type=float, default=1.0)
    p.add_argument("--duracao", type=float, default=0.0, help="0 = ate receber sinal de termino")
    p.add_argument("--rabbitmq", default="http://localhost:15672")
    p.add_argument("--usuario", default=os.getenv("RABBITMQ_USUARIO", "exp"))
    p.add_argument("--senha", default=os.getenv("RABBITMQ_SENHA", "exp"))
    p.add_argument("--sem-fluxo", action="store_true",
                   help="usa docker stats --no-stream em lugar do modo continuo")
    args = p.parse_args()

    destino = os.path.join(args.dir, args.execucao_id)
    os.makedirs(destino, exist_ok=True)
    caminho_rec = os.path.join(destino, "recursos.csv")
    caminho_fila = os.path.join(destino, "fila.csv")

    fluxo = None
    if not args.sem_fluxo:
        fluxo = FluxoRecursos()
        if not fluxo.iniciar():
            fluxo = None

    with open(caminho_rec, "w", encoding="utf-8", newline="") as f_rec, \
         open(caminho_fila, "w", encoding="utf-8", newline="") as f_fila:
        f_rec.write("instante_ms,conteiner,cpu_pct,mem_mib,mem_pct\n")
        f_fila.write("instante_ms,fila,mensagens,prontas,nao_confirmadas\n")
        inicio = time.time()
        while not _encerrar:
            if fluxo is not None:
                amostras = fluxo.coletar()
            else:
                amostras = amostrar_recursos_pontual()
            for instante, nome, cpu, mem, memperc in amostras:
                f_rec.write(f"{instante:.0f},{nome},{cpu:.2f},{mem:.2f},{memperc:.2f}\n")

            agora_ms = time.time() * 1000.0
            for fila, msgs, prontas, nao_conf in amostrar_fila(
                args.rabbitmq, args.usuario, args.senha, ["pedidos.novos", "pedidos.mortas"]
            ):
                f_fila.write(f"{agora_ms:.0f},{fila},{msgs},{prontas},{nao_conf}\n")
            f_rec.flush()
            f_fila.flush()
            if args.duracao and (time.time() - inicio) >= args.duracao:
                break
            time.sleep(args.intervalo)

        if fluxo is not None:
            for instante, nome, cpu, mem, memperc in fluxo.coletar():
                f_rec.write(f"{instante:.0f},{nome},{cpu:.2f},{mem:.2f},{memperc:.2f}\n")
            f_rec.flush()

    if fluxo is not None:
        fluxo.encerrar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
