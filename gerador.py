#!/usr/bin/env python3
"""
Gerador contínuo de eventos JSON para o pipeline de Big Data.

O programa emite um evento JSON por linha (JSONL/NDJSON). Por padrão, os
eventos são escritos em stdout, o que permite usar diretamente uma fonte
``exec`` do Apache Flume:

    python3 python/gerador.py

Para usar uma fonte ``taildir`` do Flume, grave os eventos em um arquivo:

    python3 python/gerador.py --output data/events.jsonl

Contrato de dados
-----------------
Todos os eventos possuem os campos abaixo. Campos que não se aplicam ao tipo
de evento recebem ``null``:

    event_id        identificador único do evento
    schema_version  versão do contrato, atualmente "1.0"
    event_type      "click", "cart" ou "delivery_status"
    event_time      instante do evento no formato ISO 8601 UTC
    ingested_at     instante em que o gerador emitiu o evento, em UTC
    source          origem lógica do evento, atualmente "web"
    user_id         cliente
    session_id      sessão do cliente
    product_id      produto relacionado
    category        categoria do produto
    quantity        quantidade relacionada ao evento
    unit_price      preço unitário
    total_value     valor associado ao evento
    cart_action     "add", "remove" ou "checkout" para eventos de carrinho
    order_id        pedido relacionado a checkout/status de entrega
    delivery_status status de entrega para eventos delivery_status

O campo ``event_time`` pode ser anterior a ``ingested_at``. Isso é
intencional: uma parte dos eventos é atrasada para que o job Flink possa
demonstrar event time, watermarks e tratamento de eventos fora de ordem.

Dependências: somente a biblioteca padrão do Python 3.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Any


SCHEMA_VERSION = "1.0"
SOURCE = "web"

EVENT_TYPES = ("click", "cart", "delivery_status")
EVENT_TYPE_WEIGHTS = (0.55, 0.30, 0.15)

CART_ACTIONS = ("add", "remove", "checkout")
CART_ACTION_WEIGHTS = (0.65, 0.15, 0.20)

DELIVERY_STATUSES = (
    "created",
    "shipped",
    "in_transit",
    "out_for_delivery",
    "delivered",
)

PRODUCTS = (
    {"product_id": "prod-001", "category": "eletronicos", "unit_price": 1299.90},
    {"product_id": "prod-002", "category": "informatica", "unit_price": 349.90},
    {"product_id": "prod-003", "category": "casa", "unit_price": 189.90},
    {"product_id": "prod-004", "category": "esportes", "unit_price": 249.90},
    {"product_id": "prod-005", "category": "moda", "unit_price": 159.90},
    {"product_id": "prod-006", "category": "beleza", "unit_price": 89.90},
    {"product_id": "prod-007", "category": "livros", "unit_price": 74.90},
    {"product_id": "prod-008", "category": "brinquedos", "unit_price": 119.90},
)

USERS = tuple(f"user-{number:04d}" for number in range(1, 101))


def utc_now() -> datetime:
    """Retorna o horário atual com timezone UTC."""

    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    """Serializa uma data UTC de forma simples para Flink/Spark."""

    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


class EventGenerator:
    """Mantém o estado mínimo necessário para gerar eventos relacionados."""

    def __init__(
        self,
        seed: int,
        late_rate: float,
        max_lateness_seconds: float,
    ) -> None:
        self.random = random.Random(seed)
        self.late_rate = late_rate
        self.max_lateness_seconds = max_lateness_seconds
        self.sequence = 0
        self.orders: dict[str, dict[str, Any]] = {}

    def next_event(self) -> dict[str, Any]:
        """Cria o próximo evento do fluxo."""

        self.sequence += 1
        ingested_at = utc_now()
        event_time = self._event_time(ingested_at)
        event_type = self.random.choices(
            EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1
        )[0]

        if event_type == "click":
            event = self._click_event(event_time, ingested_at)
        elif event_type == "cart":
            event = self._cart_event(event_time, ingested_at)
        else:
            event = self._delivery_event(event_time, ingested_at)

        event["event_id"] = f"evt-{self.sequence:08d}"
        event["schema_version"] = SCHEMA_VERSION
        return event

    def _event_time(self, ingested_at: datetime) -> datetime:
        """Gera event time e simula atraso/out-of-order de forma controlada."""

        if self.random.random() < self.late_rate:
            delay = self.random.uniform(0.5, self.max_lateness_seconds)
            return ingested_at - timedelta(seconds=delay)
        return ingested_at

    def _base_event(
        self,
        event_type: str,
        event_time: datetime,
        ingested_at: datetime,
        user_id: str,
        session_id: str,
        product: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "event_id": None,
            "schema_version": SCHEMA_VERSION,
            "event_type": event_type,
            "event_time": isoformat_utc(event_time),
            "ingested_at": isoformat_utc(ingested_at),
            "source": SOURCE,
            "user_id": user_id,
            "session_id": session_id,
            "product_id": product["product_id"],
            "category": product["category"],
            "quantity": None,
            "unit_price": round(float(product["unit_price"]), 2),
            "total_value": None,
            "cart_action": None,
            "order_id": None,
            "delivery_status": None,
        }

    def _click_event(
        self, event_time: datetime, ingested_at: datetime
    ) -> dict[str, Any]:
        user_id = self.random.choice(USERS)
        session_id = f"sess-{user_id}-{self.random.randint(1, 3):02d}"
        product = self.random.choice(PRODUCTS)
        event = self._base_event(
            "click", event_time, ingested_at, user_id, session_id, product
        )
        event["quantity"] = 1
        event["total_value"] = 0.0
        return event

    def _cart_event(
        self, event_time: datetime, ingested_at: datetime
    ) -> dict[str, Any]:
        user_id = self.random.choice(USERS)
        session_id = f"sess-{user_id}-{self.random.randint(1, 3):02d}"
        product = self.random.choice(PRODUCTS)
        action = self.random.choices(
            CART_ACTIONS, weights=CART_ACTION_WEIGHTS, k=1
        )[0]
        quantity = self.random.randint(1, 3)
        event = self._base_event(
            "cart", event_time, ingested_at, user_id, session_id, product
        )
        event["quantity"] = quantity
        event["cart_action"] = action
        event["total_value"] = round(product["unit_price"] * quantity, 2)

        if action == "checkout":
            order_id = f"order-{self.sequence:08d}"
            event["order_id"] = order_id
            self.orders[order_id] = {
                "user_id": user_id,
                "session_id": session_id,
                "product": product,
                "quantity": quantity,
                "status_index": 0,
            }

        return event

    def _delivery_event(
        self, event_time: datetime, ingested_at: datetime
    ) -> dict[str, Any]:
        if not self.orders:
            self._create_fallback_order()

        order_id = self.random.choice(tuple(self.orders))
        order = self.orders[order_id]
        status_index = int(order["status_index"])

        # A pedido existente avança gradualmente até "delivered". Depois de
        # entregue, continua aparecendo no fluxo para manter o gerador ativo.
        if status_index < len(DELIVERY_STATUSES) - 1 and self.random.random() < 0.75:
            status_index += 1
            order["status_index"] = status_index

        product = order["product"]
        event = self._base_event(
            "delivery_status",
            event_time,
            ingested_at,
            order["user_id"],
            order["session_id"],
            product,
        )
        event["quantity"] = order["quantity"]
        event["total_value"] = round(
            product["unit_price"] * order["quantity"], 2
        )
        event["order_id"] = order_id
        event["delivery_status"] = DELIVERY_STATUSES[status_index]
        return event

    def _create_fallback_order(self) -> None:
        """Garante status de entrega mesmo antes de sair um checkout."""

        user_id = self.random.choice(USERS)
        session_id = f"sess-{user_id}-{self.random.randint(1, 3):02d}"
        product = self.random.choice(PRODUCTS)
        order_id = f"order-bootstrap-{self.sequence:08d}"
        self.orders[order_id] = {
            "user_id": user_id,
            "session_id": session_id,
            "product": product,
            "quantity": self.random.randint(1, 3),
            "status_index": 0,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera eventos JSONL contínuos para o pipeline de Big Data."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        metavar="SEGUNDOS",
        help="intervalo entre eventos (padrão: 0.5; use 0 para teste rápido)",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=0,
        metavar="N",
        help="quantidade de eventos; 0 mantém o gerador ativo indefinidamente",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="arquivo JSONL de saída; sem esta opção, usa stdout",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="semente para resultados reproduzíveis (padrão: 42)",
    )
    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.20,
        metavar="PROPORCAO",
        help="proporção de eventos atrasados (padrão: 0.20)",
    )
    parser.add_argument(
        "--max-lateness",
        type=float,
        default=10.0,
        metavar="SEGUNDOS",
        help="atraso máximo do event_time (padrão: 10)",
    )
    args = parser.parse_args()

    if args.interval < 0:
        parser.error("--interval não pode ser negativo")
    if args.events < 0:
        parser.error("--events não pode ser negativo")
    if not 0 <= args.late_rate <= 1:
        parser.error("--late-rate deve estar entre 0 e 1")
    if args.max_lateness <= 0:
        parser.error("--max-lateness deve ser maior que zero")
    return args


def open_output(path: Path | None) -> tuple[IO[str], bool]:
    if path is None:
        return sys.stdout, False
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("a", encoding="utf-8", buffering=1), True


def run() -> None:
    args = parse_args()
    generator = EventGenerator(
        seed=args.seed,
        late_rate=args.late_rate,
        max_lateness_seconds=args.max_lateness,
    )
    output, should_close = open_output(args.output)

    try:
        emitted = 0
        while args.events == 0 or emitted < args.events:
            event = generator.next_event()
            output.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
            output.write("\n")
            output.flush()
            emitted += 1

            if args.interval and (args.events == 0 or emitted < args.events):
                time.sleep(args.interval)
    except BrokenPipeError:
        # Comportamento esperado quando o consumidor do pipe é encerrado.
        pass
    except KeyboardInterrupt:
        pass
    finally:
        if should_close:
            output.close()


if __name__ == "__main__":
    run()