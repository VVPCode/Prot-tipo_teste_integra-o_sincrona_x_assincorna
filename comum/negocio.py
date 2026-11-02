"""Regra de negocio, identica nas duas vias de comunicacao (secao 3.2.1).

Tres etapas: validacao dos campos recebidos, tempo de processamento artificial
fixo de 50 ms e gravacao transacional do pedido no PostgreSQL. A conclusao da
operacao corresponde a confirmacao da gravacao, e nao ao fim do tempo
artificial: e esse instante que o consumidor registra como T1 (Quadro 5).

O tempo artificial representa as demais etapas de negocio que o experimento nao
reproduz. A parcela TEMPO_CPU_MS e consumida em laco ocupado e o restante em
espera de E/S, de modo que o mesmo codigo seja executado nas duas vias.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from comum import config
from comum.banco import Banco
from comum.modelos import Pedido


class FalhaValidacao(Exception):
    """Pedido recusado pela validacao do consumidor (Quadro 3)."""


class FalhaGravacao(Exception):
    """Pedido recusado pelo banco de dados (Quadro 5, taxa de erro)."""


def _queimar_cpu(ms: int) -> None:
    fim = time.perf_counter() + ms / 1000.0
    while time.perf_counter() < fim:
        pass


def validar(pedido: Pedido, recebido_em_ms: float) -> None:
    """Validacoes do Quadro 3 que dependem do instante de recebimento.

    As demais regras - tipos, faixas, canal enumerado e conferencia do valor
    total - sao aplicadas na desserializacao, por comum.modelos.
    """
    if not recebido_em_ms:
        return
    recebido = datetime.fromtimestamp(recebido_em_ms / 1000.0, tz=timezone.utc)
    # Quadro 3: data_hora_pedido nao pode ser posterior ao instante de
    # recebimento pelo servico produtor.
    if pedido.data_hora_pedido > recebido:
        raise FalhaValidacao("data_hora_pedido posterior ao recebimento")


async def esperar() -> None:
    """Tempo de processamento artificial fixo (Quadro 5, regra de negocio)."""
    cpu_ms = min(config.TEMPO_CPU_MS, config.TEMPO_PROCESSAMENTO_MS)
    espera_ms = config.TEMPO_PROCESSAMENTO_MS - cpu_ms
    if cpu_ms:
        _queimar_cpu(cpu_ms)
    if espera_ms:
        await asyncio.sleep(espera_ms / 1000.0)


async def processar(
    pedido: Pedido,
    banco: Banco,
    correlacao_id: str,
    modo: str,
    tamanho_corpo: int,
    recebido_em_ms: float = 0.0,
) -> bool:
    """Valida, consome o tempo artificial e grava. Devolve False se repetida.

    O retorno False indica reentrega da mesma mensagem: o pedido ja estava
    gravado e nao foi reprocessado (secao 3.2.1, idempotencia).
    """
    validar(pedido, recebido_em_ms)
    await esperar()
    try:
        return await banco.gravar(pedido, correlacao_id, modo, tamanho_corpo)
    except Exception as erro:  # noqa: BLE001 - qualquer falha de gravacao e erro
        raise FalhaGravacao(type(erro).__name__) from erro
