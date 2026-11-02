"""Contrato da operacao de negocio: registro de pedido (Quadro 3 do artigo).

Doze campos de tipos distintos - identificador textual, inteiro, data e hora com
fuso, texto enumerado, valor logico, decimal, texto livre e uma lista de tres
itens -, de modo a exercitar a serializacao, a validacao e a gravacao de dados
heterogeneos. As regras de validacao sao as do proprio quadro.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

# Quadro 3: canal_venda aceita apenas estes valores.
CANAIS = ("site", "aplicativo", "loja")

# Quadro 3: tolerancia na conferencia do valor total.
TOLERANCIA_TOTAL = Decimal("0.01")


class Item(BaseModel):
    """Produto do pedido. Tres itens em todas as mensagens do experimento."""

    sku: str = Field(min_length=1, max_length=32)
    descricao: str = Field(min_length=1, max_length=80)
    quantidade: int = Field(ge=1, le=999)
    preco_unitario: Decimal = Field(gt=0, decimal_places=2)


class Pedido(BaseModel):
    pedido_id: UUID
    cliente_id: int = Field(gt=0)
    data_hora_pedido: datetime
    canal_venda: str
    entrega_expressa: bool
    valor_total: Decimal = Field(gt=0, decimal_places=2)
    # Campo empregado para ajustar o tamanho da mensagem aos dois niveis do
    # experimento, de 500 e de 5.000 bytes.
    observacao: str = Field(default="", max_length=8000)
    itens: list[Item] = Field(min_length=1)

    @field_validator("canal_venda")
    @classmethod
    def _canal_conhecido(cls, v: str) -> str:
        if v not in CANAIS:
            raise ValueError("canal_venda deve ser um de " + ", ".join(CANAIS))
        return v

    @field_validator("data_hora_pedido")
    @classmethod
    def _com_fuso(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("data_hora_pedido exige fuso horario")
        return v

    @model_validator(mode="after")
    def _total_confere(self) -> "Pedido":
        """Valor total igual a soma de quantidade x preco, com tolerancia."""
        soma = sum(
            (Decimal(i.quantidade) * i.preco_unitario for i in self.itens),
            Decimal("0"),
        )
        if abs(self.valor_total - soma) > TOLERANCIA_TOTAL:
            raise ValueError(
                f"valor_total {self.valor_total} difere da soma dos itens {soma}"
            )
        return self
