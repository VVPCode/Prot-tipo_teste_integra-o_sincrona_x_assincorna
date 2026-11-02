-- Esquema do banco exclusivo do servico consumidor (secao 3.2 e Quadro 4).
-- Carregado na subida do conteiner do PostgreSQL. O ambiente e reiniciado com
-- "docker compose down -v" antes de cada cenario, de modo que as tabelas
-- partam vazias (item a do protocolo, secao 3.4).

-- Pedido: os campos do Quadro 3, mais o que identifica a execucao.
-- pedido_id e a chave primaria, o que sustenta a gravacao idempotente: uma
-- reentrega da mesma mensagem nao produz segunda linha (secao 3.2.1).
CREATE TABLE IF NOT EXISTS pedidos (
    pedido_id         uuid PRIMARY KEY,
    cliente_id        integer        NOT NULL,
    data_hora_pedido  timestamptz    NOT NULL,
    canal_venda       text           NOT NULL,
    entrega_expressa  boolean        NOT NULL,
    valor_total       numeric(12, 2) NOT NULL,
    observacao        text,
    -- Rastro da execucao, para conferir a contagem de linhas por medicao.
    correlacao_id     text           NOT NULL,
    modo              text           NOT NULL,
    execucao_id       text           NOT NULL,
    tamanho_corpo     integer        NOT NULL,
    gravado_em        timestamptz    NOT NULL DEFAULT clock_timestamp()
);

-- Tres itens por pedido em todas as mensagens do experimento, de modo que o
-- numero de linhas gravadas por operacao seja o mesmo nos dois tamanhos de
-- mensagem (Quadro 5, "Numero de itens por pedido").
CREATE TABLE IF NOT EXISTS itens (
    pedido_id       uuid           NOT NULL REFERENCES pedidos (pedido_id) ON DELETE CASCADE,
    sequencia       smallint       NOT NULL,
    sku             text           NOT NULL,
    descricao       text           NOT NULL,
    quantidade      integer        NOT NULL,
    preco_unitario  numeric(12, 2) NOT NULL,
    PRIMARY KEY (pedido_id, sequencia)
);

-- A consolidacao conta as linhas por execucao para conferir a vazao (Quadro 5).
CREATE INDEX IF NOT EXISTS idx_pedidos_execucao ON pedidos (execucao_id);
