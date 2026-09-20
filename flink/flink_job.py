
"""Pipeline Flink -> HBase para os eventos de e-commerce."""

import json
import logging
import os
import time
from datetime import datetime, timezone

import happybase

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeContext,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    SourceFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


# ============================================================
# CONFIGURAÇÕES
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

LOG = logging.getLogger("ecommerce-flink")

INPUT_DIR = os.getenv(
    "FLINK_INPUT_DIR",
    "/var/lib/flink-input",
)

HBASE_HOST = os.getenv(
    "HBASE_HOST",
    "hbase",
)

HBASE_PORT = int(
    os.getenv("HBASE_THRIFT_PORT", "9090")
)

HBASE_TABLE = os.getenv(
    "HBASE_TABLE",
    "ecommerce_metrics",
)

POLL_INTERVAL_SECONDS = 2

# Arquivos com estas extensões não serão processados.
IGNORED_SUFFIXES = (
    ".tmp",
    ".part",
    ".partial",
    ".inprogress",
)


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def is_finalized_file(filename: str) -> bool:
    """
    Verifica se o arquivo pode ser processado.

    O Flume pode manter arquivos temporários enquanto escreve.
    Esses arquivos são ignorados para evitar que o Flink tente
    ler um arquivo que ainda está sendo alterado.
    """

    if not filename:
        return False

    if filename.startswith("."):
        return False

    if filename.endswith(IGNORED_SUFFIXES):
        return False

    if not filename.endswith(".json"):
        return False

    return True


def parse_event(line: str):
    """
    Converte uma linha JSON em um dicionário.

    Linhas inválidas ou eventos sem product_id são ignorados.
    """

    try:
        event = json.loads(line)

        if not isinstance(event, dict):
            return None

        if not event.get("product_id"):
            return None

        return event

    except json.JSONDecodeError:
        LOG.warning(
            "Linha JSON inválida ignorada: %s",
            line[:200],
        )
        return None


def parse_event_timestamp(event) -> int:
    """
    Converte event_time para timestamp em milissegundos.
    """

    try:
        event_time = event.get("event_time")

        if not event_time:
            return int(time.time() * 1000)

        normalized = event_time.replace("Z", "+00:00")

        dt = datetime.fromisoformat(normalized)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return int(dt.timestamp() * 1000)

    except (TypeError, ValueError, AttributeError):
        return int(time.time() * 1000)


# ============================================================
# SOURCE PERSONALIZADO
# ============================================================

class CompletedJsonFileSource(SourceFunction):
    """
    Fonte personalizada para ler arquivos JSON finalizados.

    O Flume escreve arquivos continuamente no diretório.
    Esta fonte:

    1. Ignora arquivos temporários.
    2. Lê somente arquivos .json.
    3. Não relê o mesmo arquivo.
    4. Aguarda o arquivo ficar estável antes de lê-lo.
    5. Verifica novos arquivos periodicamente.
    """

    def __init__(
        self,
        input_dir: str,
        poll_interval: int = 2,
    ):
        self.input_dir = input_dir
        self.poll_interval = poll_interval
        self.running = True
        self.processed_files = set()

    def cancel(self):
        """
        Interrompe a fonte.
        """

        self.running = False

    def _list_candidate_files(self):
        """
        Retorna os arquivos JSON que podem ser processados.
        """

        if not os.path.exists(self.input_dir):
            LOG.warning(
                "Diretório de entrada não existe: %s",
                self.input_dir,
            )

            return []

        try:
            filenames = os.listdir(self.input_dir)

        except OSError as exc:
            LOG.error(
                "Erro ao listar diretório %s: %s",
                self.input_dir,
                exc,
            )

            return []

        candidates = []

        for filename in filenames:
            if not is_finalized_file(filename):
                continue

            full_path = os.path.join(
                self.input_dir,
                filename,
            )

            if not os.path.isfile(full_path):
                continue

            if full_path in self.processed_files:
                continue

            candidates.append(full_path)

        candidates.sort()

        return candidates

    def _is_file_stable(self, file_path: str) -> bool:
        """
        Confirma se o tamanho do arquivo permaneceu igual.

        Isso reduz o risco de ler um arquivo enquanto o Flume
        ainda está gravando nele.
        """

        try:
            first_size = os.path.getsize(file_path)

            time.sleep(0.5)

            second_size = os.path.getsize(file_path)

            return (
                first_size > 0
                and first_size == second_size
            )

        except OSError:
            return False

    def _read_file(self, file_path: str, ctx):
        """
        Lê um arquivo JSON linha por linha.
        """

        if not self._is_file_stable(file_path):
            LOG.info(
                "Arquivo ainda está sendo escrito: %s",
                file_path,
            )

            return

        LOG.info(
            "Processando arquivo finalizado: %s",
            file_path,
        )

        try:
            with open(
                file_path,
                "r",
                encoding="utf-8",
            ) as file:

                for line_number, line in enumerate(
                    file,
                    start=1,
                ):

                    if not self.running:
                        return

                    line = line.strip()

                    if not line:
                        continue

                    event = parse_event(line)

                    if event is None:
                        continue

                    ctx.collect(event)

            self.processed_files.add(file_path)

            LOG.info(
                "Arquivo processado com sucesso: %s",
                file_path,
            )

        except OSError as exc:
            LOG.error(
                "Erro ao ler arquivo %s: %s",
                file_path,
                exc,
            )

    def run(self, ctx):
        """
        Executa o loop contínuo de leitura.
        """

        LOG.info(
            "Fonte iniciada. Diretório: %s",
            self.input_dir,
        )

        while self.running:
            candidate_files = self._list_candidate_files()

            for file_path in candidate_files:
                if not self.running:
                    return

                self._read_file(file_path, ctx)

            time.sleep(self.poll_interval)


# ============================================================
# TIMESTAMP E WATERMARK
# ============================================================

class EventTimestampAssigner(TimestampAssigner):
    """
    Extrai o timestamp do campo event_time.
    """

    def extract_timestamp(
        self,
        value,
        record_timestamp,
    ):
        return parse_event_timestamp(value)


# ============================================================
# MÉTRICAS ACUMULADAS
# ============================================================

class UpdateMetrics(KeyedProcessFunction):
    """
    Atualiza métricas acumuladas por produto no HBase.
    """

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "product_metrics",
            Types.PICKLED_BYTE_ARRAY(),
        )

        self.state = runtime_context.get_state(
            descriptor
        )

        self.connection = None
        self.table = None
        last_error = None

        for attempt in range(30):
            try:
                self.connection = happybase.Connection(
                    host=HBASE_HOST,
                    port=HBASE_PORT,
                    timeout=5000,
                    autoconnect=False,
                )

                self.connection.open()

                self.table = self.connection.table(
                    HBASE_TABLE
                )

                LOG.info(
                    "Conectado ao HBase em %s:%s",
                    HBASE_HOST,
                    HBASE_PORT,
                )

                break

            except Exception as exc:
                last_error = exc

                LOG.warning(
                    "HBase ainda não disponível "
                    "(tentativa %s/30): %s",
                    attempt + 1,
                    exc,
                )

                time.sleep(2)

        else:
            raise RuntimeError(
                "Não foi possível conectar ao HBase: "
                f"{last_error}"
            )

    def process_element(self, event, ctx):
        product_id = (
            event.get("product_id")
            or "unknown"
        )

        metrics = self.state.value() or {
            "clicks": 0,
            "cart_adds": 0,
            "checkouts": 0,
            "orders_value": 0.0,
            "delivered": 0,
            "category": event.get("category")
            or "unknown",
            "last_event_time": "",
            "last_event_id": "",
        }

        event_type = event.get("event_type")
        cart_action = event.get("cart_action")
        delivery_status = event.get(
            "delivery_status"
        )

        if event_type == "click":
            metrics["clicks"] += 1

        elif (
            event_type == "cart"
            and cart_action == "add"
        ):
            metrics["cart_adds"] += 1

        elif (
            event_type == "cart"
            and cart_action == "checkout"
        ):
            metrics["checkouts"] += 1

            try:
                total_value = float(
                    event.get("total_value") or 0.0
                )

            except (TypeError, ValueError):
                total_value = 0.0

            metrics["orders_value"] += total_value

        elif (
            event_type == "delivery_status"
            and delivery_status == "delivered"
        ):
            metrics["delivered"] += 1

        metrics["category"] = (
            event.get("category")
            or metrics["category"]
        )

        metrics["last_event_time"] = (
            event.get("event_time")
            or metrics["last_event_time"]
        )

        metrics["last_event_id"] = (
            event.get("event_id")
            or metrics["last_event_id"]
        )

        self.state.update(metrics)

        row = {
            b"metrics:clicks": str(
                metrics["clicks"]
            ).encode(),

            b"metrics:cart_adds": str(
                metrics["cart_adds"]
            ).encode(),

            b"metrics:checkouts": str(
                metrics["checkouts"]
            ).encode(),

            b"metrics:orders_value": (
                f'{metrics["orders_value"]:.2f}'
            ).encode(),

            b"metrics:delivered": str(
                metrics["delivered"]
            ).encode(),

            b"meta:category": (
                metrics["category"]
            ).encode(),

            b"meta:last_event_time": (
                metrics["last_event_time"]
            ).encode(),

            b"meta:last_event_id": (
                metrics["last_event_id"]
            ).encode(),
        }

        self.table.put(
            product_id.encode(),
            row,
        )

        LOG.debug(
            "Métrica atualizada para product_id=%s",
            product_id,
        )

    def close(self):
        if self.connection is not None:
            self.connection.close()


# ============================================================
# ALERTAS EM JANELAS DESLIZANTES
# ============================================================

class WindowClickAlerts(ProcessWindowFunction):
    """
    Gera alertas para produtos com pelo menos 10 cliques
    em uma janela de 5 minutos, deslocada a cada 1 minuto.
    """

    def open(self, runtime_context: RuntimeContext):
        self.connection = None
        self.table = None
        last_error = None

        for attempt in range(30):
            try:
                self.connection = happybase.Connection(
                    host=HBASE_HOST,
                    port=HBASE_PORT,
                    timeout=5000,
                    autoconnect=False,
                )

                self.connection.open()

                self.table = self.connection.table(
                    HBASE_TABLE
                )

                LOG.info(
                    "WindowClickAlerts conectado ao HBase "
                    "em %s:%s",
                    HBASE_HOST,
                    HBASE_PORT,
                )

                break

            except Exception as exc:
                last_error = exc

                LOG.warning(
                    "HBase ainda não disponível para alertas "
                    "(tentativa %s/30): %s",
                    attempt + 1,
                    exc,
                )

                time.sleep(2)

        else:
            raise RuntimeError(
                "Não foi possível conectar ao HBase: "
                f"{last_error}"
            )

    def process(
        self,
        key,
        context: TimeWindow,
        elements,
    ):
        click_count = 0
        first_event_time = None
        last_event_time = None

        for event in elements:
            if event.get("event_type") != "click":
                continue

            click_count += 1

            event_time = event.get(
                "event_time"
            )

            if event_time:
                if first_event_time is None:
                    first_event_time = event_time

                last_event_time = event_time

        window_start = context.start
        window_end = context.end

        alert_threshold = 10

        if click_count < alert_threshold:
            return

        alert_type = "HIGH_CLICK_VOLUME"

        row_key = (
            f"alert|{key}|"
            f"{window_start}|{window_end}"
        )

        row = {
            b"alert:product_id": str(
                key
            ).encode(),

            b"alert:type": alert_type.encode(),

            b"alert:click_count": str(
                click_count
            ).encode(),

            b"alert:window_start": str(
                window_start
            ).encode(),

            b"alert:window_end": str(
                window_end
            ).encode(),

            b"alert:first_event_time": str(
                first_event_time or ""
            ).encode(),

            b"alert:last_event_time": str(
                last_event_time or ""
            ).encode(),
        }

        self.table.put(
            row_key.encode(),
            row,
        )

        LOG.info(
            "ALERTA GERADO | product_id=%s | "
            "clicks=%s | window_start=%s | "
            "window_end=%s",
            key,
            click_count,
            window_start,
            window_end,
        )

        yield {
            "alert_type": alert_type,
            "product_id": key,
            "click_count": click_count,
            "window_start": window_start,
            "window_end": window_end,
        }

    def close(self):
        if self.connection is not None:
            self.connection.close()


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def main():
    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    env.set_parallelism(1)

    source = env.add_source(
        CompletedJsonFileSource(
            input_dir=INPUT_DIR,
            poll_interval=POLL_INTERVAL_SECONDS,
        ),
        type_info=Types.PICKLED_BYTE_ARRAY(),
    )

    stream = source.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(15)
        )
        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )

    events = (
        stream
        .filter(
            lambda event: event is not None
        )
    )

    # --------------------------------------------------------
    # FLUXO 1: MÉTRICAS ACUMULADAS POR PRODUTO
    # --------------------------------------------------------

    (
        events
        .key_by(
            lambda event: event["product_id"]
        )
        .process(
            UpdateMetrics(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .print()
    )

    # --------------------------------------------------------
    # FLUXO 2: ALERTAS EM JANELAS DESLIZANTES
    # --------------------------------------------------------

    (
        events
        .filter(
            lambda event: (
                event.get("event_type")
                == "click"
            )
        )
        .key_by(
            lambda event: event["product_id"]
        )
        .window(
            SlidingEventTimeWindows.of(
                Time.minutes(5),
                Time.minutes(1),
            )
        )
        .process(
            WindowClickAlerts(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .print()
    )

    env.execute(
        "ecommerce-flink-to-hbase"
    )


if __name__ == "__main__":
    main()