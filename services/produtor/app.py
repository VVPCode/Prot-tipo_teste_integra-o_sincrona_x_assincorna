"""Servico produtor: expoe a operacao de negocio ao gerador de carga.

Um unico recurso, POST /pedidos. O estilo de integracao e definido por parametro
de configuracao do proprio servico (Quadro 5), e nao pelo endereco que o gerador
escolhe: a variavel MODO decide entre invocar o consumidor por HTTP e publicar a
mensagem no RabbitMQ.

T0 e registrado aqui, ao receber a requisicao (Quadro 5). O instante tomado pelo
gerador vem no cabecalho x-emitido-em-ms e e propagado como conferencia: quando
o produtor satura, a espera na fila de conexoes fica fora de T1 - T0, e a
diferenca entre as duas latencias mede esse efeito.

O produtor tambem registra o tamanho do corpo recebido, que e a variavel
independente "tamanho da mensagem" conferida em cada execucao. Ele nao valida o
pedido: a validacao cabe ao consumidor, antes da gravacao.
"""
from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from importlib.metadata import version

import aio_pika
import httpx
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse

from comum import config, topologia
from comum.registro import RegistroSerie

estado: dict = {}


@asynccontextmanager
async def ciclo_de_vida(_: FastAPI):
    estado["serie"] = RegistroSerie()
    await estado["serie"].abrir()
    estado["http"] = httpx.AsyncClient(
        base_url=config.CONSUMIDOR_URL,
        timeout=httpx.Timeout(config.TEMPO_LIMITE_MS / 1000.0),
        limits=httpx.Limits(max_connections=2000, max_keepalive_connections=2000),
    )
    conexao = await topologia.conectar()
    canal = await conexao.channel(publisher_confirms=True)
    troca, _fila = await topologia.declarar(canal)
    estado.update(conexao=conexao, canal=canal, troca=troca)
    try:
        yield
    finally:
        await estado["http"].aclose()
        await conexao.close()
        await estado["serie"].fechar()


app = FastAPI(title="Produtor", lifespan=ciclo_de_vida)


@app.get("/saude")
async def saude() -> dict:
    return {"situacao": "ok"}


@app.get("/info")
async def info() -> dict:
    return {
        "servico": "produtor",
        "fastapi": version("fastapi"),
        "httpx": version("httpx"),
        "aio_pika": version("aio-pika"),
        "pydantic": version("pydantic"),
        "parametros": config.parametros(),
    }


@app.get("/metricas")
async def metricas() -> dict:
    return {"contagens": dict(estado["serie"].contagens)}


def _instante_gerador(cabecalho: str) -> float:
    try:
        valor = float(cabecalho)
    except (TypeError, ValueError):
        return 0.0
    return valor if valor > 0 else 0.0


async def _via_sincrona(
    corpo: bytes, correlacao_id: str, t0_ms: float, t0_gerador_ms: float,
) -> Response:
    """Requisicao e resposta bloqueante (Fielding, 2000; Richardson, 2018)."""
    cabecalhos = {
        "content-type": "application/json",
        "x-correlacao-id": correlacao_id,
        "x-t0-ms": f"{t0_ms:.3f}",
        "x-t0-gerador-ms": f"{t0_gerador_ms:.3f}",
        "x-tamanho-corpo": str(len(corpo)),
    }
    try:
        resposta = await estado["http"].post("/processar", content=corpo, headers=cabecalhos)
    except httpx.TimeoutException:
        # Tempo esgotado e contabilizado como erro, e nao como latencia elevada.
        estado["serie"].anotar("erro_tempo_limite", len(corpo))
        return JSONResponse(
            {"correlacao_id": correlacao_id, "erro": "tempo_limite"}, status_code=504
        )
    except httpx.HTTPError as exc:
        estado["serie"].anotar("erro_http", len(corpo))
        return JSONResponse(
            {"correlacao_id": correlacao_id, "erro": type(exc).__name__}, status_code=502
        )
    estado["serie"].anotar(
        "aceita" if resposta.status_code == 200 else "erro_http", len(corpo)
    )
    return JSONResponse(
        {"correlacao_id": correlacao_id},
        status_code=200 if resposta.status_code == 200 else resposta.status_code,
    )


async def _via_assincrona(
    corpo: bytes, correlacao_id: str, t0_ms: float, t0_gerador_ms: float,
) -> Response:
    """Publicacao em ponto de troca com confirmacao do intermediador."""
    mensagem = aio_pika.Message(
        body=corpo,
        content_type="application/json",
        correlation_id=correlacao_id,
        message_id=correlacao_id,
        headers={
            "x-t0-ms": f"{t0_ms:.3f}",
            "x-t0-gerador-ms": f"{t0_gerador_ms:.3f}",
            "x-tamanho-corpo": str(len(corpo)),
        },
        delivery_mode=(
            aio_pika.DeliveryMode.PERSISTENT
            if config.MENSAGEM_PERSISTENTE
            else aio_pika.DeliveryMode.NOT_PERSISTENT
        ),
    )
    try:
        await estado["troca"].publish(
            mensagem, routing_key=config.ROUTING_KEY,
            timeout=config.TEMPO_LIMITE_MS / 1000.0,
        )
    except Exception as exc:  # noqa: BLE001 - falha de publicacao e erro da operacao
        estado["serie"].anotar("erro_publicacao", len(corpo))
        return JSONResponse(
            {"correlacao_id": correlacao_id, "erro": type(exc).__name__}, status_code=502
        )
    estado["serie"].anotar("aceita", len(corpo))
    # Responde imediatamente: confirma a aceitacao, nao o processamento.
    return JSONResponse({"correlacao_id": correlacao_id}, status_code=202)


@app.post("/pedidos")
async def pedidos(
    requisicao: Request, x_emitido_em_ms: str = Header(default="")
) -> Response:
    # T0: instante de recebimento da requisicao pelo produtor (Quadro 5).
    t0_ms = time.time() * 1000.0
    t0_gerador_ms = _instante_gerador(x_emitido_em_ms)
    corpo = await requisicao.body()
    correlacao_id = str(uuid.uuid4())

    # O produtor nao valida o pedido: quem valida e o consumidor, antes da
    # gravacao (Quadro 3). Validar aqui gastaria CPU no componente que limita a
    # via sincrona, tanto mais com a mensagem de 5.000 bytes, e impediria que um
    # pedido invalido chegasse ao consumidor, que e onde a falha de validacao
    # deve ser contabilizada.
    if config.MODO == "assincrono":
        return await _via_assincrona(corpo, correlacao_id, t0_ms, t0_gerador_ms)
    return await _via_sincrona(corpo, correlacao_id, t0_ms, t0_gerador_ms)
