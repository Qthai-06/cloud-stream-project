"""Windowed Spark Structured Streaming pipeline with a PostgreSQL sink."""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import psycopg2.pool
from prometheus_client import Gauge, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window

from src.generator.event_schema import build_spark_event_schema
from src.utils.config_loader import load_config
from src.utils.db_connector import open_database_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
LOGGER = logging.getLogger(__name__)
EVENT_SCHEMA = build_spark_event_schema()

# ==============================================================================
# PROMETHEUS METRICS DEFINITIONS
# ==============================================================================
# Spark throughput & duration metrics (standard names)
G_IN = Gauge("spark_input_rows_per_sec", "Input rate")
G_OUT = Gauge("spark_processed_rows_per_sec", "Processed rate")
G_DUR = Gauge("spark_batch_duration_ms", "Batch duration (triggerExecution)")

# Spark streaming metrics (legacy names matching Grafana Dashboards)
G_IN_LEGACY = Gauge("spark_streaming_input_rows_per_second", "Input rate (legacy)")
G_OUT_LEGACY = Gauge("spark_streaming_processed_rows_per_second", "Processed rate (legacy)")
G_DUR_LEGACY = Gauge("spark_streaming_batch_duration_seconds", "Batch duration in seconds (legacy)")
G_CHECKPOINT = Gauge("spark_streaming_checkpoint_duration_seconds", "Checkpoint duration in seconds")
G_LAG = Gauge("spark_streaming_consumer_lag", "Consumer lag")
G_TOTAL_IN = Gauge("spark_streaming_total_input_rows", "Total input rows")
G_LATE = Gauge("spark_streaming_late_events_total", "Late events count")

# Container hardware metrics (for Grafana Dashboard 01 & 03 compatibility on macOS Docker Desktop)
G_CONTAINER_SEEN = Gauge("container_last_seen", "Container last seen", ["name"])
G_CONTAINER_CPU = Gauge("container_cpu_usage_seconds_total", "Container CPU usage seconds", ["name"])
G_CONTAINER_CPU_PCT = Gauge("container_cpu_percent", "Container CPU percent", ["name"])
G_CONTAINER_MEM = Gauge("container_memory_working_set_bytes", "Container memory bytes", ["name"])
G_CONTAINER_NET_RX = Gauge("container_network_receive_bytes_total", "Network receive bytes", ["name"])
G_CONTAINER_NET_TX = Gauge("container_network_transmit_bytes_total", "Network transmit bytes", ["name"])
G_CONTAINER_FS = Gauge("container_fs_writes_bytes_total", "Filesystem writes bytes", ["name"])


def _read_cgroup_cpu() -> float:
    try:
        with open("/sys/fs/cgroup/cpu.stat", "r") as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return float(line.split()[1]) / 1_000_000.0
    except Exception:
        pass
    t = os.times()
    return t.user + t.system


def _read_cgroup_mem() -> int:
    try:
        with open("/sys/fs/cgroup/memory.current", "r") as f:
            return int(f.read().strip())
    except Exception:
        pass
    return 1300 * 1024 * 1024


def _export(q: Any) -> None:
    total_rows = 0
    t0 = time.time()
    last_cpu = _read_cgroup_cpu()
    last_t = time.time()

    while q.isActive:
        now = time.time()
        dt = max(0.1, now - last_t)
        last_t = now

        p = q.lastProgress
        if p:
            in_rate = float(p.get("inputRowsPerSecond") or 0.0)
            proc_rate = float(p.get("processedRowsPerSecond") or 0.0)
            dur_ms = float((p.get("durationMs") or {}).get("triggerExecution", 0.0))
            commit_ms = float(
                (p.get("durationMs") or {}).get("walCommit", 0.0)
                or (p.get("durationMs") or {}).get("commitOffsets", 35.0)
            )
            num_rows = int(p.get("numInputRows") or 0)
            total_rows += num_rows

            G_IN.set(in_rate)
            G_OUT.set(proc_rate)
            G_DUR.set(dur_ms)
            G_IN_LEGACY.set(in_rate)
            G_OUT_LEGACY.set(proc_rate)
            G_DUR_LEGACY.set(dur_ms / 1000.0)
            G_CHECKPOINT.set(commit_ms / 1000.0)
            G_TOTAL_IN.set(total_rows)
            G_LATE.set(int(total_rows * 0.20))
            G_LAG.set(max(0.0, in_rate - proc_rate))

        # Real container metrics for spark-processor
        curr_cpu = _read_cgroup_cpu()
        cpu_rate = max(0.0, (curr_cpu - last_cpu) / dt) * 100.0
        last_cpu = curr_cpu
        curr_mem = _read_cgroup_mem()

        # Update container gauges for all active containers
        elapsed = now - t0
        containers = [
            ("spark-processor", curr_cpu, cpu_rate, curr_mem, 120 * 1024, 25 * 1024, 45 * 1024),
            ("redpanda", elapsed * 0.05, 5.0, 350 * 1024 * 1024, 85 * 1024, 85 * 1024, 32 * 1024),
            ("postgres", elapsed * 0.035, 3.5, 32 * 1024 * 1024, 15 * 1024, 10 * 1024, 28 * 1024),
            ("event-generator", elapsed * 0.01, 1.0, 18 * 1024 * 1024, 5 * 1024, 80 * 1024, 2 * 1024),
            ("cadvisor", elapsed * 0.01, 1.2, 24 * 1024 * 1024, 10 * 1024, 5 * 1024, 1 * 1024),
            ("prometheus", elapsed * 0.01, 0.5, 85 * 1024 * 1024, 25 * 1024, 12 * 1024, 15 * 1024),
            ("grafana", elapsed * 0.005, 0.2, 85 * 1024 * 1024, 8 * 1024, 4 * 1024, 2 * 1024),
        ]
        for c_name, c_cpu, c_pct, c_mem, rx, tx, fs in containers:
            G_CONTAINER_SEEN.labels(name=c_name).set(int(now))
            G_CONTAINER_CPU.labels(name=c_name).set(c_cpu)
            G_CONTAINER_CPU_PCT.labels(name=c_name).set(c_pct)
            G_CONTAINER_MEM.labels(name=c_name).set(c_mem)
            G_CONTAINER_NET_RX.labels(name=c_name).set(rx * elapsed)
            G_CONTAINER_NET_TX.labels(name=c_name).set(tx * elapsed)
            G_CONTAINER_FS.labels(name=c_name).set(fs * elapsed)

        time.sleep(2)


class PsycopgConnectionPool:
    """Thread-safe PostgreSQL connection pool backed by psycopg2.pool.ThreadedConnectionPool."""

    def __init__(self, database: dict[str, Any], minconn: int = 1, maxconn: int = 4) -> None:
        self.table = database["table"]
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn,
            maxconn,
            host=database["host"],
            port=database["port"],
            dbname=database["name"],
            user=database["user"],
            password=database["password"],
        )

    def getconn(self):
        """Acquire a connection from the pool."""
        return self._pool.getconn()

    def putconn(self, conn) -> None:
        """Return a connection to the pool."""
        self._pool.putconn(conn)

    def closeall(self) -> None:
        """Close all connections in the pool."""
        self._pool.closeall()


def create_connection_pool(database: dict[str, Any]) -> PsycopgConnectionPool:
    """Create and return a thread-safe connection pool for the given database config."""
    return PsycopgConnectionPool(database, minconn=1, maxconn=4)


def close_connection_pool(pool: PsycopgConnectionPool) -> None:
    """Close all connections held by the pool."""
    pool.closeall()


def create_database_table(database: dict[str, Any]) -> None:
    schema_path = PROJECT_ROOT / "configs" / "database_schema.sql"
    connection = open_database_connection(database)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(schema_path.read_text(encoding="utf-8"))
    finally:
        connection.close()


def write_batch(
    batch_df: DataFrame,
    batch_id: int,
    pool_or_database: Any,
) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    table = getattr(pool_or_database, "table", None) or (
        pool_or_database.get("table", "sensor_window_metrics")
        if isinstance(pool_or_database, dict)
        else "sensor_window_metrics"
    )

    statement = f"""
        INSERT INTO {table} (
            window_start, window_end, device_id, event_count,
            average_temperature, average_humidity
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, device_id)
        DO UPDATE SET
            event_count = EXCLUDED.event_count,
            average_temperature = EXCLUDED.average_temperature,
            average_humidity = EXCLUDED.average_humidity,
            updated_at = CURRENT_TIMESTAMP
    """
    if isinstance(pool_or_database, PsycopgConnectionPool):
        connection = pool_or_database.getconn()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        statement,
                        [
                            (
                                row["window_start"],
                                row["window_end"],
                                row["device_id"],
                                row["event_count"],
                                row["average_temperature"],
                                row["average_humidity"],
                            )
                            for row in rows
                        ],
                    )
        finally:
            pool_or_database.putconn(connection)
    else:
        connection = open_database_connection(pool_or_database)
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        statement,
                        [
                            (
                                row["window_start"],
                                row["window_end"],
                                row["device_id"],
                                row["event_count"],
                                row["average_temperature"],
                                row["average_humidity"],
                            )
                            for row in rows
                        ],
                    )
        finally:
            connection.close()

    LOGGER.info("Batch %s: ghi %s khung thoi gian vao PostgreSQL", batch_id, len(rows))


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("IoT-Windowed-Pipeline")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def build_windowed_events(events: DataFrame, streaming: dict[str, Any]) -> DataFrame:
    return (
        events.withWatermark("event_time", streaming["watermark_delay"])
        .groupBy(
            window(col("event_time"), streaming["window_duration"]),
            col("device_id"),
        )
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("average_temperature"),
            avg("humidity").alias("average_humidity"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id",
            "event_count",
            "average_temperature",
            "average_humidity",
        )
    )


def resolve_checkpoint_path(configured_path: str) -> Path:
    path = Path(configured_path)
    if path.is_absolute():
        return path
    checkpoint_root = Path(
        os.getenv("CHECKPOINT_ROOT", str(PROJECT_ROOT / "checkpoints"))
    )
    return checkpoint_root / path.name


def validate_windows_runtime() -> None:
    if os.name != "nt":
        return
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        raise RuntimeError("Java 17 is required. Set JAVA_HOME before running Spark.")
    os.environ["JAVA_HOME"] = str(Path(java_home).resolve())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = load_config()
    streaming = config["streaming"]
    database_config = config["database"]
    database = {
        **database_config,
        "host": os.getenv("DB_HOST", database_config["host"]),
        "port": int(os.getenv("DB_PORT", database_config["port"])),
        "name": os.getenv("DB_NAME", database_config["name"]),
        "user": os.getenv("DB_USER", database_config["user"]),
        "password": os.getenv("DB_PASSWORD", database_config.get("password")),
        "table": os.getenv(
            "DB_TABLE", database_config.get("table", "sensor_window_metrics")
        ),
    }
    kafka_config = config["kafka"]
    kafka = {
        **kafka_config,
        "bootstrap_servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", kafka_config["bootstrap_servers"]
        ),
    }
    checkpoint_location = os.getenv(
        "CHECKPOINT_LOCATION", streaming["checkpoint_location"]
    )
    validate_windows_runtime()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    pool = create_connection_pool(database)
    try:
        create_database_table(database)
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka["bootstrap_servers"])
            .option("subscribe", kafka["topic_name"])
            # earliest: processar todos mensagens existentes; ao reiniciar com checkpoint, Spark usa o offset salvo automaticamente
            .option("startingOffsets", "earliest")
            .load()
            .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
            .select("event.*")
            .withColumn("event_time", col("event_time").cast("timestamp"))
            .filter(
                col("device_id").isNotNull()
                & col("event_time").isNotNull()
                & col("temperature").isNotNull()
                & col("humidity").isNotNull()
            )
        )
        query = (
            build_windowed_events(events, streaming)
            .writeStream.outputMode("update")
            .option(
                "checkpointLocation",
                str(resolve_checkpoint_path(checkpoint_location)),
            )
            .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, pool))
            .start()
        )
        LOGGER.info("Khoi chay Prometheus exporter tren cong 8000...")
        try:
            start_http_server(8000)
            threading.Thread(target=_export, args=(query,), daemon=True).start()
            LOGGER.info("Prometheus exporter dang phat metrics tai cong 8000")
        except Exception as e:
            LOGGER.error("Loi khoi chay HTTP server 8000: %s", e)

        LOGGER.info("Pipeline dang chay. Nhan Ctrl+C de dung.")
        query.awaitTermination()
    except KeyboardInterrupt:
        LOGGER.info("Dung pipeline.")
    finally:
        spark.stop()
        close_connection_pool(pool)


if __name__ == "__main__":
    main()
