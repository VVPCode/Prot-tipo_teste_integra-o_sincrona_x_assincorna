"""Registro por operacao concluida, no ponto de medicao definido na secao 3.3.

O marco de conclusao e, nas duas vias, o instante em que o consumidor termina
o processamento. A latencia e calculada contra o instante de emissao gerado
pelo produtor e propagado por identificador de correlacao (Hohpe; Woolf, 2003).
Produtor e consumidor compartilham o relogio do hospedeiro, pois executam em
conteineres sobre o mesmo nucleo.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import TextIO

from comum import config

CABECALHO = (
    "correlacao_id,modo,cenario,execucao_id,repeticao,tamanho_mensagem,"
    "t0_ms,t0_gerador_ms,t1_ms,latencia_ms,latencia_gerador_ms,"
    "tamanho_corpo,situacao\n"
)

SITUACOES_EMISSAO = ("aceita", "erro_tempo_limite", "erro_http", "erro_publicacao")
# Soma e contagem dos tamanhos recebidos, para a media por segundo.
CABECALHO_SERIE = ("instante_s,emitidas," + ",".join(SITUACOES_EMISSAO)
                   + ",soma_tamanho_corpo\n")


class RegistroOperacoes:
    """Gravador de linhas em CSV com descarga periodica em segundo plano."""

    def __init__(self, nome_arquivo: str = "consumidor.csv") -> None:
        self._dir = os.path.join(config.DIR_RESULTADOS, config.EXECUCAO_ID)
        self._caminho = os.path.join(self._dir, nome_arquivo)
        self._buffer: list[str] = []
        self._arquivo: TextIO | None = None
        self._tarefa: asyncio.Task | None = None
        self.contagens: dict[str, int] = {}

    async def abrir(self) -> None:
        os.makedirs(self._dir, exist_ok=True)
        novo = not os.path.exists(self._caminho) or os.path.getsize(self._caminho) == 0
        self._arquivo = open(self._caminho, "a", encoding="utf-8", newline="")
        if novo:
            self._arquivo.write(CABECALHO)
        self._tarefa = asyncio.create_task(self._descarregar_periodicamente())

    async def fechar(self) -> None:
        if self._tarefa:
            self._tarefa.cancel()
            try:
                await self._tarefa
            except asyncio.CancelledError:
                pass
        self._descarregar()
        if self._arquivo:
            self._arquivo.close()
            self._arquivo = None

    def renomear(self, sufixo: str) -> None:
        """Renomeia o arquivo corrente, preservando-o como dado descartado."""
        if not os.path.exists(self._caminho):
            return
        raiz, extensao = os.path.splitext(self._caminho)
        destino = f"{raiz}_{sufixo}{extensao}"
        if os.path.exists(destino):
            os.remove(destino)
        os.replace(self._caminho, destino)

    def anotar(
        self,
        correlacao_id: str,
        modo: str,
        t0_ms: float,
        situacao: str = "sucesso",
        t0_gerador_ms: float = 0.0,
        tamanho_corpo: int = 0,
    ) -> float:
        """Anota uma operacao e devolve a latencia observada em milissegundos.

        T1 e tomado aqui: na via assincrona e na sincrona, o chamador anota
        depois da confirmacao da gravacao no banco (Quadro 5).
        """
        t1_ms = time.time() * 1000.0
        latencia = t1_ms - t0_ms if t0_ms else -1.0
        latencia_gerador = t1_ms - t0_gerador_ms if t0_gerador_ms else -1.0
        self.contagens[situacao] = self.contagens.get(situacao, 0) + 1
        self._buffer.append(
            f"{correlacao_id},{modo},{config.CENARIO},{config.EXECUCAO_ID},"
            f"{config.REPETICAO},{config.TAMANHO_MENSAGEM},"
            f"{t0_ms:.3f},{t0_gerador_ms:.3f},{t1_ms:.3f},"
            f"{latencia:.3f},{latencia_gerador:.3f},{tamanho_corpo},{situacao}\n"
        )
        return latencia

    def _descarregar(self) -> None:
        if not self._buffer or self._arquivo is None:
            return
        linhas, self._buffer = self._buffer, []
        self._arquivo.writelines(linhas)
        self._arquivo.flush()

    async def _descarregar_periodicamente(self) -> None:
        while True:
            await asyncio.sleep(config.INTERVALO_FLUSH_S)
            self._descarregar()


class RegistroSerie:
    """Contagem de emissoes por segundo, no ponto de emissao (produtor).

    A taxa de erro exige o total emitido, que so o produtor conhece: operacoes
    recusadas ou com tempo esgotado nunca chegam ao consumidor. Agregar por
    segundo, em lugar de gravar uma linha por requisicao, mantem o custo
    desprezivel no servico cujo consumo de CPU esta sob medicao.
    """

    def __init__(self, nome_arquivo: str = "") -> None:
        self._dir = os.path.join(config.DIR_RESULTADOS, config.EXECUCAO_ID)
        # Um arquivo por processo: o produtor roda com varios workers do uvicorn,
        # que nao compartilham memoria. A analise reune os arquivos e filtra pela
        # janela de medicao, o que dispensa descartar o aquecimento por renomeio.
        if not nome_arquivo:
            nome_arquivo = f"produtor_serie_{os.getpid()}.csv"
        self._caminho = os.path.join(self._dir, nome_arquivo)
        self._baldes: dict[int, dict[str, int]] = {}
        self._arquivo: TextIO | None = None
        self._tarefa: asyncio.Task | None = None
        self.contagens: dict[str, int] = {}

    async def abrir(self) -> None:
        os.makedirs(self._dir, exist_ok=True)
        novo = not os.path.exists(self._caminho) or os.path.getsize(self._caminho) == 0
        self._arquivo = open(self._caminho, "a", encoding="utf-8", newline="")
        if novo:
            self._arquivo.write(CABECALHO_SERIE)
        self._tarefa = asyncio.create_task(self._descarregar_periodicamente())

    async def fechar(self) -> None:
        if self._tarefa:
            self._tarefa.cancel()
            try:
                await self._tarefa
            except asyncio.CancelledError:
                pass
        self._descarregar(tudo=True)
        if self._arquivo:
            self._arquivo.close()
            self._arquivo = None

    def renomear(self, sufixo: str) -> None:
        if not os.path.exists(self._caminho):
            return
        raiz, extensao = os.path.splitext(self._caminho)
        destino = f"{raiz}_{sufixo}{extensao}"
        if os.path.exists(destino):
            os.remove(destino)
        os.replace(self._caminho, destino)

    def anotar(self, situacao: str, tamanho_corpo: int = 0) -> None:
        segundo = int(time.time())
        balde = self._baldes.get(segundo)
        if balde is None:
            balde = {chave: 0 for chave in SITUACOES_EMISSAO}
            balde["emitidas"] = 0
            balde["soma_tamanho_corpo"] = 0
            self._baldes[segundo] = balde
        balde["emitidas"] += 1
        balde["soma_tamanho_corpo"] += tamanho_corpo
        if situacao in balde:
            balde[situacao] += 1
        self.contagens[situacao] = self.contagens.get(situacao, 0) + 1

    def _descarregar(self, tudo: bool = False) -> None:
        if self._arquivo is None or not self._baldes:
            return
        # O segundo corrente ainda pode receber emissoes: so e gravado no fim.
        corrente = int(time.time())
        fechados = sorted(s for s in self._baldes if tudo or s < corrente)
        if not fechados:
            return
        linhas = []
        for segundo in fechados:
            balde = self._baldes.pop(segundo)
            valores = ",".join(str(balde[chave]) for chave in SITUACOES_EMISSAO)
            linhas.append(
                f"{segundo},{balde['emitidas']},{valores},"
                f"{balde['soma_tamanho_corpo']}\n"
            )
        self._arquivo.writelines(linhas)
        self._arquivo.flush()

    async def _descarregar_periodicamente(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            self._descarregar()
