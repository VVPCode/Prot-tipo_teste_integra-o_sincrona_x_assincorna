"""Acesso ao PostgreSQL pelo servico consumidor (secao 3.2.2).

Conjunto de conexoes de tamanho fixo, igual nas duas vias e igual ao limite de
processamento simultaneo: um conjunto menor seria um gargalo escondido, e um
maior nao seria usado, ja que o semaforo e o prefetch limitam a concorrencia ao
mesmo numero.

A gravacao do pedido e dos tres itens ocorre em uma unica transacao, e a
conclusao da operacao corresponde a confirmacao dessa gravacao. A idempotencia
vem da chave primaria: uma reentrega da mesma mensagem nao produz segunda linha.
"""
from __future__ import annotations

import asyncio

import asyncpg

from comum import config
from comum.modelos import Pedido

INSERIR_PEDIDO = """
INSERT INTO pedidos (
    pedido_id, cliente_id, data_hora_pedido, canal_venda, entrega_expressa,
    valor_total, observacao, correlacao_id, modo, execucao_id, tamanho_corpo
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (pedido_id) DO NOTHING
"""

INSERIR_ITEM = """
INSERT INTO itens (pedido_id, sequencia, sku, descricao, quantidade, preco_unitario)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (pedido_id, sequencia) DO NOTHING
"""


class Banco:
    """Conjunto de conexoes e gravacao transacional do pedido."""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def abrir(self, tentativas: int = 30, espera_s: float = 1.0) -> None:
        """Abre o conjunto de conexoes, aguardando o banco se preciso.

        Tamanho fixo, com minimo igual ao maximo, para que o conjunto nao cresca
        durante a medicao e a capacidade disponivel seja a mesma em toda a
        execucao. As tentativas evitam que uma demora do banco na subida do
        ambiente encerre o servico no meio de uma campanha longa.
        """
        ultimo_erro: Exception | None = None
        for _ in range(max(1, tentativas)):
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=config.POSTGRES_DSN,
                    min_size=config.POOL_BANCO,
                    max_size=config.POOL_BANCO,
                    command_timeout=config.TEMPO_LIMITE_MS / 1000.0 * 4,
                )
                return
            except Exception as erro:  # noqa: BLE001 - qualquer falha e nova tentativa
                ultimo_erro = erro
                await asyncio.sleep(espera_s)
        raise RuntimeError(
            "nao foi possivel abrir o conjunto de conexoes com o banco"
        ) from ultimo_erro

    async def fechar(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def gravar(
        self, pedido: Pedido, correlacao_id: str, modo: str, tamanho_corpo: int
    ) -> bool:
        """Grava pedido e itens em uma transacao.

        Devolve True quando a linha foi inserida e False quando o pedido ja
        existia, isto e, quando se trata de reentrega da mesma mensagem.
        """
        assert self._pool is not None, "o conjunto de conexoes nao foi aberto"
        async with self._pool.acquire() as conexao:
            async with conexao.transaction():
                resultado = await conexao.execute(
                    INSERIR_PEDIDO,
                    pedido.pedido_id,
                    pedido.cliente_id,
                    pedido.data_hora_pedido,
                    pedido.canal_venda,
                    pedido.entrega_expressa,
                    pedido.valor_total,
                    pedido.observacao,
                    correlacao_id,
                    modo,
                    config.EXECUCAO_ID,
                    tamanho_corpo,
                )
                # asyncpg devolve "INSERT 0 1" quando inseriu e "INSERT 0 0"
                # quando a chave primaria ja existia.
                inseriu = resultado.rsplit(" ", 1)[-1] != "0"
                if inseriu:
                    for sequencia, item in enumerate(pedido.itens, start=1):
                        await conexao.execute(
                            INSERIR_ITEM,
                            pedido.pedido_id,
                            sequencia,
                            item.sku,
                            item.descricao,
                            item.quantidade,
                            item.preco_unitario,
                        )
        # Ao sair do bloco a transacao foi confirmada: e este o instante que o
        # chamador registra como T1 (Quadro 5).
        return inseriu

    async def durabilidade(self) -> dict:
        """Parametros de durabilidade efetivos, para os metadados da execucao.

        A secao 3.2.2 declara que o banco opera com a durabilidade padrao; o
        valor efetivo e registrado em cada execucao em lugar de presumido.
        """
        if self._pool is None:
            return {}
        async with self._pool.acquire() as conexao:
            return {
                "fsync": await conexao.fetchval("show fsync"),
                "synchronous_commit": await conexao.fetchval("show synchronous_commit"),
                "versao": await conexao.fetchval("show server_version"),
            }

    async def esvaziar(self) -> None:
        """Esvazia as tabelas, para que a medicao parta de estado conhecido.

        O consumidor grava cada pedido com o identificador da execucao, que e o
        mesmo no aquecimento e na medicao. Sem este esvaziamento, os pedidos do
        cenario C0 permaneceriam na tabela e a contagem de linhas superaria os
        registros T1 de sucesso em toda execucao (item a do protocolo).
        """
        assert self._pool is not None, "o conjunto de conexoes nao foi aberto"
        async with self._pool.acquire() as conexao:
            await conexao.execute("TRUNCATE pedidos, itens")

    async def contar_pedidos(self, execucao_id: str | None = None) -> int:
        """Numero de linhas gravadas, para conferencia da vazao (Quadro 5)."""
        assert self._pool is not None, "o conjunto de conexoes nao foi aberto"
        alvo = execucao_id or config.EXECUCAO_ID
        async with self._pool.acquire() as conexao:
            return await conexao.fetchval(
                "SELECT count(*) FROM pedidos WHERE execucao_id = $1", alvo
            )
