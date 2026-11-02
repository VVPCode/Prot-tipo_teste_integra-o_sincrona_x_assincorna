"""Servico consumidor: executa a regra de negocio e grava o pedido.

Atende as duas vias com o mesmo codigo de negocio (secao 3.2.1):
  POST /processar  -> via sincrona, com capacidade simultanea imposta por semaforo
  fila AMQP        -> via assincrona, com prefetch igual e confirmacao manual

O marco de conclusao e, nas duas vias, a confirmacao da gravacao do pedido no
PostgreSQL (Quadro 5). T1 e a confirmacao manual de consumo sao emitidos somente
depois do COMMIT, de modo que uma falha do consumidor devolva a mensagem a fila
em vez de descarta-la (secao 3.2.2).
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from importlib.metadata import version

import aio_pika
from fastapi import FastAPI, Header, Response
from fastapi.responses import JSONResponse

from comum import config, negocio, topologia
from comum.banco import Banco
from comum.modelos import Pedido
from comum.negocio import FalhaGravacao, FalhaValidacao
from comum.registro import RegistroOperacoes

estado: dict = {}


def _numero(texto: str) -> float:
    try:
        valor = float(texto)
    except (TypeError, ValueError):
        return 0.0
    return valor if valor > 0 else 0.0


async def _consumir(mensagem: aio_pika.abc.AbstractIncomingMessage) -> None:
    registro: RegistroOperacoes = estado["registro"]
    cabecalhos = mensagem.headers or {}
    correlacao_id = mensagem.correlation_id or mensagem.message_id or ""
    t0_ms = _numero(cabecalhos.get("x-t0-ms", 0))
    t0_gerador_ms = _numero(cabecalhos.get("x-t0-gerador-ms", 0))
    tamanho_corpo = int(cabecalhos.get("x-tamanho-corpo", 0) or 0)

    def anotar(situacao: str) -> None:
        registro.anotar(
            correlacao_id, "assincrono", t0_ms, situacao,
            t0_gerador_ms=t0_gerador_ms, tamanho_corpo=tamanho_corpo,
        )

    try:
        pedido = Pedido.model_validate_json(mensagem.body)
    except Exception:
        # Mensagem invalida: rejeitada sem reenfileiramento -> mensagens mortas.
        anotar("invalida")
        await mensagem.reject(requeue=False)
        return

    try:
        inseriu = await negocio.processar(
            pedido, estado["banco"], correlacao_id, "assincrono",
            tamanho_corpo, recebido_em_ms=t0_ms,
        )
    except FalhaValidacao:
        anotar("invalida")
        await mensagem.reject(requeue=False)
        return
    except FalhaGravacao:
        anotar("falha_gravacao")
        await mensagem.reject(requeue=False)
        return

    # T1: depois do COMMIT. A idempotencia vem da chave primaria, de modo que a
    # reentrega da mesma mensagem e registrada como duplicada.
    anotar("sucesso" if inseriu else "duplicada")
    # Confirmacao manual, emitida somente apos a gravacao (RabbitMQ, 2026a).
    await mensagem.ack()


@asynccontextmanager
async def ciclo_de_vida(_: FastAPI):
    estado["semaforo"] = asyncio.Semaphore(config.CONCORRENCIA)
    estado["registro"] = RegistroOperacoes()
    await estado["registro"].abrir()

    banco = Banco()
    await banco.abrir()
    estado["banco"] = banco

    conexao = await topologia.conectar()
    canal = await conexao.channel()
    # Limita as entregas pendentes de confirmacao ao mesmo numero de operacoes
    # simultaneas permitido na via sincrona (AMQP Working Group, 2008).
    await canal.set_qos(prefetch_count=config.CONCORRENCIA)
    _troca, fila = await topologia.declarar(canal)
    etiqueta = await fila.consume(_consumir)
    estado.update(conexao=conexao, canal=canal, fila=fila, etiqueta=etiqueta)
    try:
        yield
    finally:
        await conexao.close()
        await banco.fechar()
        await estado["registro"].fechar()


app = FastAPI(title="Consumidor", lifespan=ciclo_de_vida)


@app.get("/saude")
async def saude() -> dict:
    return {"situacao": "ok"}


@app.get("/info")
async def info() -> dict:
    return {
        "servico": "consumidor",
        "fastapi": version("fastapi"),
        "aio_pika": version("aio-pika"),
        "asyncpg": version("asyncpg"),
        "pydantic": version("pydantic"),
        "parametros": config.parametros(),
        "banco": await estado["banco"].durabilidade(),
    }


@app.get("/metricas")
async def metricas() -> dict:
    return {"contagens": dict(estado["registro"].contagens)}


@app.get("/pedidos/contagem")
async def contagem_pedidos(execucao_id: str = "") -> dict:
    """Linhas gravadas na execucao, para conferencia da vazao (Quadro 5)."""
    alvo = execucao_id or config.EXECUCAO_ID
    return {"execucao_id": alvo, "linhas": await estado["banco"].contar_pedidos(alvo)}


@app.post("/reiniciar-registro")
async def reiniciar_registro(sufixo: str = "aquecimento") -> dict:
    """Descarta os dados do cenario de aquecimento (itens a e b, secao 3.4).

    Renomeia o registro de operacoes e esvazia as tabelas: os pedidos gravados
    durante o aquecimento levam o mesmo identificador de execucao da medicao e,
    se permanecessem, a contagem de linhas do banco deixaria de conferir com os
    registros T1 de sucesso.
    """
    antigo: RegistroOperacoes = estado["registro"]
    await antigo.fechar()
    antigo.renomear(sufixo)
    estado["registro"] = RegistroOperacoes()
    await estado["registro"].abrir()
    await estado["banco"].esvaziar()
    return {
        "situacao": "reiniciado",
        "descartado_em": f"consumidor_{sufixo}.csv",
        "tabelas_esvaziadas": True,
    }


@app.post("/processar")
async def processar(
    pedido: Pedido,
    x_correlacao_id: str = Header(default=""),
    x_t0_ms: str = Header(default="0"),
    x_t0_gerador_ms: str = Header(default="0"),
    x_tamanho_corpo: str = Header(default="0"),
) -> Response:
    registro: RegistroOperacoes = estado["registro"]
    t0_ms = _numero(x_t0_ms)
    t0_gerador_ms = _numero(x_t0_gerador_ms)
    try:
        tamanho_corpo = int(x_tamanho_corpo or 0)
    except ValueError:
        tamanho_corpo = 0

    def anotar(situacao: str) -> None:
        registro.anotar(
            x_correlacao_id, "sincrono", t0_ms, situacao,
            t0_gerador_ms=t0_gerador_ms, tamanho_corpo=tamanho_corpo,
        )

    # O excedente de demanda aguarda aqui; esgotado o tempo limite do
    # solicitante, converte-se em erro (secao 2.2).
    async with estado["semaforo"]:
        # Recusa quando o prazo do solicitante vence antes de a operacao poder
        # terminar: sem isso o consumidor seguiria executando trabalho que o
        # solicitante abandonou, e a espera acumulada deixaria de ser limitada
        # pelo tempo limite. O prazo so existe na via sincrona; na assincrona o
        # solicitante aguarda apenas a publicacao (secao 2.3).
        limite_ms = config.TEMPO_LIMITE_MS
        if t0_ms and limite_ms:
            decorrido_ms = time.time() * 1000.0 - t0_ms
            if decorrido_ms + config.TEMPO_PROCESSAMENTO_MS > limite_ms:
                anotar("expirada")
                return JSONResponse({"erro": "prazo_vencido"}, status_code=503)
        try:
            inseriu = await negocio.processar(
                pedido, estado["banco"], x_correlacao_id, "sincrono",
                tamanho_corpo, recebido_em_ms=t0_ms,
            )
        except FalhaValidacao as erro:
            anotar("invalida")
            return JSONResponse({"erro": str(erro)}, status_code=422)
        except FalhaGravacao as erro:
            anotar("falha_gravacao")
            return JSONResponse({"erro": str(erro)}, status_code=500)

    anotar("sucesso" if inseriu else "duplicada")
    return JSONResponse({"correlacao_id": x_correlacao_id}, status_code=200)
