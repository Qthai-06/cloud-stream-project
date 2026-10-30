"""Windowed Spark Structured Streaming pipeline with watermark and PostgreSQL sink."""

import logging
import os
from pathlib import Path
from typing import Any

import psycopg2
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
LOGGER = logging.getLogger(__name__)

EVENT_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),
        StructField("temperature", DoubleType(), nullable=False),
        StructField("humidity", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=True),
        StructField("is_late_simulated", BooleanType(), nullable=True),
    ]
)


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def open_database_connection(database: dict[str, Any]) -> Any:
    password = database.get("password")
    if password is None:
        raise KeyError("Missing database password in pipeline_config.yaml")

    return psycopg2.connect(
        host=database["host"],
        port=database["port"],
        dbname=database["name"],
        user=database["user"],
        password=password,
    )


def create_database_table(database: dict[str, Any]) -> None:
    schema_path = PROJECT_ROOT / "configs" / "database_schema.sql"
    schema = schema_path.read_text(encoding="utf-8")
    connection = open_database_connection(database)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(schema)
    finally:
        connection.close()


def write_batch(batch_df: DataFrame, batch_id: int, database: dict[str, Any]) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    statement = f"""
        INSERT INTO {database["table"]} (
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
    connection = open_database_connection(database)
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


def has_valid_winutils(target: Path) -> bool:
    if not target.is_file():
        return False
    try:
        with target.open("rb") as handle:
            return handle.read(2) == b"MZ"
    except OSError:
        return False


def validate_windows_runtime() -> None:
    if os.name != "nt":
        return

    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_home = str(Path(java_home).resolve())
    else:
        java_candidates = [
            Path("C:/Program Files/Java"),
            Path("C:/Program Files/Eclipse Adoptium"),
        ]
        for base in java_candidates:
            if not base.exists():
                continue
            for candidate in sorted(base.glob("jdk-17*"), key=lambda item: item.name, reverse=True):
                java_exe = candidate / "bin" / "java.exe"
                if java_exe.is_file():
                    java_home = str(candidate)
                    break
            if java_home:
                break

    if not java_home:
        raise RuntimeError(
            "Java 17 is required. Install OpenJDK 17 and set JAVA_HOME before running Spark."
        )

    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = str(Path(java_home) / "bin") + os.pathsep + os.environ.get("PATH", "")

    hadoop_home = os.environ.get("HADOOP_HOME") or "C:/hadoop"
    winutils = Path(hadoop_home) / "bin" / "winutils.exe"
    if not has_valid_winutils(winutils):
        raise RuntimeError(
            "Missing valid winutils.exe. Download a Windows Hadoop build for Hadoop 3.3.x "
            f"and place it at {winutils}. Then set HADOOP_HOME to {hadoop_home}."
        )

    os.environ["HADOOP_HOME"] = str(Path(hadoop_home))
    os.environ["PATH"] = str(Path(hadoop_home) / "bin") + os.pathsep + os.environ.get("PATH", "")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = load_config()
    database = config["database"]
    streaming = config["streaming"]
    validate_windows_runtime()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        create_database_table(database)
        kafka = config["kafka"]
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka["bootstrap_servers"])
            .option("subscribe", kafka["topic_name"])
            .option("startingOffsets", "latest")
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
        windowed = (
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
        query = (
            windowed.writeStream.outputMode("append")
            .option(
                "checkpointLocation",
                str(PROJECT_ROOT / streaming["checkpoint_location"]),
            )
            .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, database))
            .start()
        )
        LOGGER.info("Pipeline dang chay. Nhan Ctrl+C de dung.")
        query.awaitTermination()
    except KeyboardInterrupt:
        LOGGER.info("Dung pipeline.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
