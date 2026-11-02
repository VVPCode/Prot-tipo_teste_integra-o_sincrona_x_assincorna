"""Topologia AMQP 0-9-1 do experimento (secao 3.2.2).

Ponto de troca direto -> fila duravel, com encaminhamento de mensagens
rejeitadas a um ponto de troca de mensagens mortas (RabbitMQ, 2026b).
Declarada de forma identica pelo produtor e pelo consumidor.
"""
from __future__ import annotations

import asyncio

import aio_pika

from comum import config


async def declarar(canal: aio_pika.abc.AbstractChannel) -> tuple[aio_pika.abc.AbstractExchange, aio_pika.abc.AbstractQueue]:
    troca_mortas = await canal.declare_exchange(
        config.EXCHANGE_MORTAS, aio_pika.ExchangeType.FANOUT, durable=True
    )
    fila_mortas = await canal.declare_queue(config.FILA_MORTAS, durable=True)
    await fila_mortas.bind(troca_mortas)

    troca = await canal.declare_exchange(
        config.EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    fila = await canal.declare_queue(
        config.FILA,
        durable=True,
        arguments={"x-dead-letter-exchange": config.EXCHANGE_MORTAS},
    )
    await fila.bind(troca, routing_key=config.ROUTING_KEY)
    return troca, fila


async def conectar(tentativas: int = 30, espera_s: float = 1.0):
    """Abre a conexao com o intermediador, aguardando-o se preciso.

    O aio_pika.connect_robust reconecta apenas depois de uma conexao ja
    estabelecida: na abertura, uma demora do RabbitMQ encerraria o servico. As
    tentativas evitam repetir a subida do ambiente numa campanha longa.
    """
    ultimo_erro: Exception | None = None
    for _ in range(max(1, tentativas)):
        try:
            return await aio_pika.connect_robust(config.AMQP_URL)
        except Exception as erro:  # noqa: BLE001 - qualquer falha e nova tentativa
            ultimo_erro = erro
            await asyncio.sleep(espera_s)
    raise RuntimeError("nao foi possivel conectar ao intermediador") from ultimo_erro
