"""Parametros controlados do experimento (secao 3.2 e Quadro 5 do artigo).

Todos os valores sao lidos do ambiente para que a mesma imagem de conteiner
seja usada nas duas vias de comunicacao, variando apenas a configuracao.
"""
from __future__ import annotations

import os


def _int(nome: str, padrao: int) -> int:
    return int(os.getenv(nome, str(padrao)))


def _float(nome: str, padrao: float) -> float:
    return float(os.getenv(nome, str(padrao)))


def _bool(nome: str, padrao: bool) -> bool:
    return os.getenv(nome, "1" if padrao else "0").strip().lower() in {"1", "true", "sim", "yes"}


# --- Variaveis controladas -------------------------------------------------
# Regra de negocio: validacao, tempo artificial fixo e gravacao (secao 3.2.1).
TEMPO_PROCESSAMENTO_MS = _int("TEMPO_PROCESSAMENTO_MS", 50)
# Parcela do tempo artificial gasta em laco de CPU (0 = apenas espera de E/S).
TEMPO_CPU_MS = _int("TEMPO_CPU_MS", 0)
# Capacidade de processamento simultaneo do consumidor. Na via sincrona e
# imposta por semaforo; na via assincrona, pelo prefetch do AMQP (secao 3.2.2).
CONCORRENCIA = _int("CONCORRENCIA", 8)
# Conjunto de conexoes com o banco, de tamanho fixo e igual a concorrencia.
POOL_BANCO = _int("POOL_BANCO", CONCORRENCIA)
# Tempo limite identico nas duas chamadas HTTP e no gerador (secao 3.2.2).
TEMPO_LIMITE_MS = _int("TEMPO_LIMITE_MS", 1000)

# --- Variavel independente operada pelo produtor ---------------------------
# Estilo de integracao: definido por parametro de configuracao do produtor
# (Quadro 5), e nao pelo endereco que o gerador de carga escolhe.
MODO = os.getenv("MODO", "sincrono").strip().lower()

# --- Identificacao da execucao (item f do protocolo, secao 3.4) ------------
EXECUCAO_ID = os.getenv("EXECUCAO_ID", "avulsa")
CENARIO = os.getenv("CENARIO", "NA")
REPETICAO = _int("REPETICAO", 0)
TAMANHO_MENSAGEM = _int("TAMANHO_MENSAGEM", 500)

# --- Enderecos -------------------------------------------------------------
CONSUMIDOR_URL = os.getenv("CONSUMIDOR_URL", "http://consumidor:8001")
AMQP_URL = os.getenv("AMQP_URL", "amqp://exp:exp@rabbitmq:5672/%2F")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://exp:exp@postgres:5432/exp"
)

# --- Topologia AMQP --------------------------------------------------------
EXCHANGE = os.getenv("AMQP_EXCHANGE", "pedidos")
ROUTING_KEY = os.getenv("AMQP_ROUTING_KEY", "pedidos.novo")
FILA = os.getenv("AMQP_FILA", "pedidos.novos")
EXCHANGE_MORTAS = os.getenv("AMQP_EXCHANGE_MORTAS", "pedidos.mortas")
FILA_MORTAS = os.getenv("AMQP_FILA_MORTAS", "pedidos.mortas")
MENSAGEM_PERSISTENTE = _bool("MENSAGEM_PERSISTENTE", True)

# --- Registro de resultados ------------------------------------------------
DIR_RESULTADOS = os.getenv("DIR_RESULTADOS", "/resultados")
INTERVALO_FLUSH_S = _float("INTERVALO_FLUSH_S", 0.2)


def parametros() -> dict:
    """Devolve os parametros controlados, para registro por execucao."""
    return {
        "tempo_processamento_ms": TEMPO_PROCESSAMENTO_MS,
        "tempo_cpu_ms": TEMPO_CPU_MS,
        "concorrencia": CONCORRENCIA,
        "pool_banco": POOL_BANCO,
        "tempo_limite_ms": TEMPO_LIMITE_MS,
        "mensagem_persistente": MENSAGEM_PERSISTENTE,
        "modo": MODO,
        "execucao_id": EXECUCAO_ID,
        "cenario": CENARIO,
        "repeticao": REPETICAO,
        "tamanho_mensagem": TAMANHO_MENSAGEM,
    }
