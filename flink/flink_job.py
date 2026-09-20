"""Pipeline Flink -> HBase com métricas e alertas por janela de tempo."""

import json
import logging
import os
import time
from datetime import datetime

import happybase
from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("ecommerce-flink")

INPUT_DIR = os.getenv("FLINK_INPUT_DIR", "/var/lib/flink-input")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
HBASE_TABLE = os.getenv("HBASE_TABLE", "ecommerce_metrics")
ALERT_THRESHOLD = int(os.getenv("ALERT_CLICK_THRESHOLD", "5"))


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            dt = datetime.fromisoformat(value["event_time"].replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except (KeyError, TypeError, ValueError):
            return record_timestamp


def connect_hbase():
    last_error = None
    for attempt in range(30):
        try:
            connection = happybase.Connection(
                host=HBASE_HOST,
                port=HBASE_PORT,
                timeout=5000,
                autoconnect=False,
            )
            connection.open()
            LOG.info("Conectado ao HBase em %s:%s", HBASE_HOST, HBASE_PORT)
            return connection, connection.table(HBASE_TABLE)
        except Exception as exc:
            last_error = exc
            LOG.warning(
                "HBase ainda não disponível (tentativa %s/30): %s",
                attempt + 1,
                exc,
            )
            time.sleep(2)
    raise RuntimeError(f"Não foi possível conectar ao HBase: {last_error}")


class UpdateMetrics(KeyedProcessFunction):
    """Mantém métricas acumuladas por produto e grava no HBase."""

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "product_metrics",
            Types.PICKLED_BYTE_ARRAY(),
        )
        self.state = runtime_context.get_state(descriptor)
        self.connection, self.table = connect_hbase()

    def process_element(self, event, ctx):
        product_id = event.get("product_id") or "unknown"
        metrics = self.state.value() or {
            "clicks": 0,
            "cart_adds": 0,
            "checkouts": 0,
            "orders_value": 0.0,
            "delivered": 0,
            "category": event.get("category") or "unknown",
            "last_event_time": "",
            "last_event_id": "",
        }

        event_type = event.get("event_type")
        action = event.get("cart_action")
        status = event.get("delivery_status")

        if event_type == "click":
            metrics["clicks"] += 1
        elif event_type == "cart" and action == "add":
            metrics["cart_adds"] += 1
        elif event_type == "cart" and action == "checkout":
            metrics["checkouts"] += 1
            metrics["orders_value"] += float(event.get("total_value") or 0.0)
        elif event_type == "delivery_status" and status == "delivered":
            metrics["delivered"] += 1

        metrics["category"] = event.get("category") or metrics["category"]
        metrics["last_event_time"] = event.get("event_time") or metrics["last_event_time"]
        metrics["last_event_id"] = event.get("event_id") or metrics["last_event_id"]
        self.state.update(metrics)

        row = {
            b"metrics:clicks": str(metrics["clicks"]).encode(),
            b"metrics:cart_adds": str(metrics["cart_adds"]).encode(),
            b"metrics:checkouts": str(metrics["checkouts"]).encode(),
            b"metrics:orders_value": f'{metrics["orders_value"]:.2f}'.encode(),
            b"metrics:delivered": str(metrics["delivered"]).encode(),
            b"meta:category": metrics["category"].encode(),
            b"meta:last_event_time": metrics["last_event_time"].encode(),
            b"meta:last_event_id": metrics["last_event_id"].encode(),
        }
        self.table.put(product_id.encode(), row)

    def close(self):
        if self.connection is not None:
            self.connection.close()


class ClickAlertWindow(ProcessWindowFunction):
    """Gera alerta quando um produto ultrapassa o limite de cliques na janela."""

    def open(self, runtime_context: RuntimeContext):
        self.connection, self.table = connect_hbase()

    def process(self, key, context, elements):
        events = list(elements)
        click_events = [event for event in events if event.get("event_type") == "click"]
        clicks = len(click_events)

        if clicks < ALERT_THRESHOLD:
            return

        window_start = context.window().start
        window_end = context.window().end
        alert_id = f"{key}-{window_start}"
        row_key = f"alert-{alert_id}".encode()

        row = {
            b"alerts:type": b"HIGH_CLICK_VOLUME",
            b"alerts:message": (
                f"Produto {key} teve {clicks} cliques na janela."
            ).encode(),
            b"alerts:product_id": str(key).encode(),
            b"alerts:clicks": str(clicks).encode(),
            b"alerts:window_start": str(window_start).encode(),
            b"alerts:window_end": str(window_end).encode(),
        }
        self.table.put(row_key, row)

        yield {
            "alert_id": alert_id,
            "product_id": key,
            "clicks": clicks,
            "window_start": window_start,
            "window_end": window_end,
        }

    def close(self):
        if self.connection is not None:
            self.connection.close()


def parse_event(line: str):
    try:
        event = json.loads(line)
        if not isinstance(event, dict):
            return None
        if not event.get("product_id"):
            return None
        return event
    except json.JSONDecodeError:
        LOG.warning("Linha JSON inválida ignorada: %s", line[:200])
        return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), INPUT_DIR
        )
        .monitor_continuously(Duration.of_seconds(2))
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    stream = env.from_source(source, watermark_strategy, "flume-events")

    events = stream.map(
        parse_event,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    ).filter(lambda event: event is not None)

    # Caminho 1: métricas acumuladas por produto.
    (
        events.key_by(lambda event: event["product_id"])
        .process(UpdateMetrics(), output_type=Types.PICKLED_BYTE_ARRAY())
        .print("METRICA")
    )

    # Caminho 2: janela deslizante para detectar excesso de cliques.
    (
        events.filter(lambda event: event.get("event_type") == "click")
        .key_by(lambda event: event["product_id"])
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .process(
            ClickAlertWindow(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .print("ALERTA")
    )

    env.execute("ecommerce-flink-metrics-and-alerts")


if __name__ == "__main__":
    main()
