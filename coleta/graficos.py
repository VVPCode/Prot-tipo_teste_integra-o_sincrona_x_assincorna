"""Graficos de linha que relacionam o nivel de carga a cada variavel dependente
(secao 3.5 do artigo), com uma serie por via de comunicacao e um grafico por
tamanho de mensagem, ja que a analise e feita separadamente para cada tamanho.

Requer matplotlib:  pip install -r coleta/requirements.txt
Uso: python coleta/graficos.py [--dir resultados] [--rotulo piloto]
"""
from __future__ import annotations

import argparse
import csv
import os

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

# Variavel dependente -> (coluna do resumo, rotulo do eixo, nome do arquivo)
SERIES = [
    ("lat_media_ms_mediana", "Latencia media (ms)", "latencia_media"),
    ("lat_p50_ms_mediana", "Latencia p50 (ms)", "latencia_p50"),
    ("lat_p95_ms_mediana", "Latencia p95 (ms)", "latencia_p95"),
    ("lat_p99_ms_mediana", "Latencia p99 (ms)", "latencia_p99"),
    ("vazao_util_mediana", "Vazao util (pedidos/s)", "vazao_util"),
    ("vazao_janela_mediana", "Vazao na saida do consumidor (pedidos/s)", "vazao"),
    ("taxa_erro_mediana", "Taxa de erro (proporcao)", "taxa_erro"),
    ("prop_apos_limite_mediana", "Concluidos depois do prazo (proporcao)", "apos_prazo"),
    ("cpu_consumidor_pct_mediana", "CPU do consumidor (%)", "cpu_consumidor"),
    ("cpu_postgres_pct_mediana", "CPU do PostgreSQL (%)", "cpu_postgres"),
    ("cpu_rabbitmq_pct_mediana", "CPU do RabbitMQ (%)", "cpu_rabbitmq"),
    ("cpu_produtor_pct_mediana", "CPU do produtor (%)", "cpu_produtor"),
    ("mem_consumidor_mib_mediana", "Memoria do consumidor (MiB)", "mem_consumidor"),
    ("fila_max_mediana", "Ocupacao maxima da fila (mensagens)", "fila"),
]

ROTULOS = {"sincrono": "Via sincrona (REST/HTTP)", "assincrono": "Via assincrona (AMQP/RabbitMQ)"}


def carregar(caminho):
    with open(caminho, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def numero(texto):
    try:
        return float(texto)
    except (TypeError, ValueError):
        return None


def desenhar(linhas, coluna, rotulo_y, nome, destino, tamanho):
    fig, eixo = plt.subplots(figsize=(7.0, 4.2), dpi=150)
    tracou = False
    for modo in ("sincrono", "assincrono"):
        pontos = []
        for linha in linhas:
            # C5 a C7 nao tem nivel de carga constante: ficam fora destes
            # graficos e sao tratados por rampa.py e eventos.py.
            if linha["modo"] != modo or linha["cenario"] in ("C5", "C6", "C7"):
                continue
            if str(linha.get("tamanho_mensagem", "")) != str(tamanho):
                continue
            x = numero(linha["carga_alvo_ops"])
            y = numero(linha.get(coluna, ""))
            if x is None or y is None:
                continue
            pontos.append((x, y))
        if not pontos:
            continue
        pontos.sort()
        eixo.plot(
            [p[0] for p in pontos],
            [p[1] for p in pontos],
            marker="o",
            label=ROTULOS.get(modo, modo),
        )
        tracou = True
    if not tracou:
        plt.close(fig)
        return None
    eixo.set_xlabel("Carga oferecida (pedidos/s)")
    eixo.set_ylabel(rotulo_y)
    eixo.set_title(f"Mensagem de {tamanho} bytes")
    eixo.grid(True, linewidth=0.4, alpha=0.5)
    eixo.legend()
    fig.tight_layout()
    caminho = os.path.join(destino, f"{nome}_{tamanho}b.png")
    fig.savefig(caminho)
    plt.close(fig)
    return caminho


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="resultados")
    p.add_argument("--rotulo", default="", help="rotulo das execucoes a representar")
    args = p.parse_args()

    if plt is None:
        print("matplotlib nao instalado. Execute: pip install -r coleta/requirements.txt")
        return 1

    complemento = "" if args.rotulo == "" else "_" + args.rotulo
    resumo = os.path.join(args.dir, "resumo" + complemento + ".csv")
    if not os.path.exists(resumo):
        print("execute coleta/consolidar.py antes de gerar os graficos")
        return 1

    linhas = carregar(resumo)
    destino = os.path.join(args.dir, "graficos" + complemento)
    os.makedirs(destino, exist_ok=True)

    tamanhos = sorted({str(l.get("tamanho_mensagem", "")) for l in linhas if l.get("tamanho_mensagem")})
    gerados = 0
    for tamanho in tamanhos:
        for coluna, rotulo, nome in SERIES:
            caminho = desenhar(linhas, coluna, rotulo, nome, destino, tamanho)
            if caminho:
                print("gerado: " + caminho)
                gerados += 1
    if not gerados:
        print("nenhum grafico gerado: faltam dados de cenarios de carga fixa")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
