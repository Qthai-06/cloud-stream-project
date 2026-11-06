"""Windowed Spark Structured Streaming pipeline with a PostgreSQL sink."""

import logging
import os
from pathlib import Path
from typing import Any

import psycopg2.pool
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window

from src.generator.event_schema import build_spark_event_schema
from src.utils.config_loader import load_config
from src.utils.db_connector import open_database_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
LOGGER = logging.getLogger(__name__)
EVENT_SCHEMA = build_spark_event_schema()


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


def write_batch(batch_df: DataFrame, batch_id: int, pool: PsycopgConnectionPool) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    statement = f"""
        INSERT INTO {pool.table} (
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
    connection = pool.getconn()
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
        pool.putconn(connection)
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
        LOGGER.info("Pipeline dang chay. Nhan Ctrl+C de dung.")
        query.awaitTermination()
    except KeyboardInterrupt:
        LOGGER.info("Dung pipeline.")
    finally:
        spark.stop()
        close_connection_pool(pool)


if __name__ == "__main__":
    main()
