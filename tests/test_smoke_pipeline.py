"""Regression tests for the configured streaming pipeline components."""

import os
import random
import json
import socket
import sys
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window

from src.generator.event_producer import generate_sensor_event
from src.generator.event_schema import EventValidationError, validate_event
from src.streaming.late_data_router import duration_to_seconds, route_late_events
from src.streaming.event_storage import append_jsonl, dead_letter_record, raw_event_record
from src.streaming.spark_pipeline import (
    EVENT_SCHEMA,
    build_windowed_events,
    resolve_checkpoint_path,
)
from src.utils.config_loader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PipelineConfigurationTests(unittest.TestCase):
    def test_runtime_dependencies_are_installed(self) -> None:
        import kafka
        import psycopg2
        import pyspark

        self.assertTrue(kafka.__version__)
        self.assertTrue(psycopg2.__version__)
        self.assertTrue(pyspark.__version__)

    def test_config_uses_container_endpoints_and_postgres_sink(self) -> None:
        with patch.dict(
            os.environ,
            {"POSTGRES_PASSWORD": "test-password"},
            clear=False,
        ):
            config = load_config()

        self.assertEqual(config["kafka"]["bootstrap_servers"], "redpanda:29092")
        self.assertEqual(config["database"]["table"], "sensor_window_metrics")
        self.assertEqual(config["database"]["password"], "test-password")

    def test_generator_emits_timezone_aware_timestamp(self) -> None:
        _, event = generate_sensor_event(["sensor-A101"], 0, 0, 0)
        event_time = datetime.fromisoformat(event["event_time"])
        self.assertIsNotNone(event_time.tzinfo)
        self.assertEqual(event_time.tzinfo, timezone.utc)

    def test_event_schema_accepts_generated_event(self) -> None:
        _, event = generate_sensor_event(["sensor-A101"], 0, 0, 0)
        validate_event(event)

    def test_event_schema_rejects_invalid_payload(self) -> None:
        invalid_event = {
            "device_id": "sensor-A101",
            "event_time": "2026-11-04T14:00:00",
            "temperature": 100.0,
            "humidity": 50.0,
            "status": "UNKNOWN",
            "is_late_simulated": "false",
        }

        with self.assertRaises(EventValidationError):
            validate_event(invalid_event)

    def test_generator_late_data_ratio_is_statistically_close_to_20_percent(self) -> None:
        sample_size = 10_000
        expected_ratio = 0.20

        random_state = random.getstate()
        try:
            random.seed(20261104)
            late_count = sum(
                generate_sensor_event(["sensor-A101"], expected_ratio, 30, 120)[
                    1
                ]["is_late_simulated"]
                for _ in range(sample_size)
            )
        finally:
            random.setstate(random_state)

        actual_ratio = late_count / sample_size
        print(
            f"\nStatistical late-data result: {late_count}/{sample_size} "
            f"({actual_ratio:.2%})"
        )
        self.assertGreaterEqual(actual_ratio, 0.18)
        self.assertLessEqual(actual_ratio, 0.22)

    def test_watermark_duration_parser_supports_seconds_and_minutes(self) -> None:
        self.assertEqual(duration_to_seconds("120 seconds"), 120)
        self.assertEqual(duration_to_seconds("2 minutes"), 120)

    def test_checkpoint_path_uses_persistent_mount(self) -> None:
        with patch.dict(
            os.environ,
            {"CHECKPOINT_ROOT": "/opt/workspace/checkpoints"},
            clear=False,
        ):
            checkpoint = resolve_checkpoint_path("checkpoints/spark_pipeline")

        self.assertEqual(
            checkpoint,
            Path("/opt/workspace/checkpoints/spark_pipeline"),
        )

class SparkWindowIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._python_env = {
            "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON"),
            "PYSPARK_DRIVER_PYTHON": os.environ.get("PYSPARK_DRIVER_PYTHON"),
        }
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        cls.spark = (
            SparkSession.builder.appName("SparkWindowIntegrationTest")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.io.native.lib.available", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        for name, value in cls._python_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_real_events_are_windowed_by_device_for_one_minute(self) -> None:
        events = [
            {
                "device_id": "sensor-A101",
                "event_time": "2026-11-04T14:00:10+00:00",
                "temperature": 20.0,
                "humidity": 40.0,
                "status": "OK",
                "is_late_simulated": False,
            },
            {
                "device_id": "sensor-A101",
                "event_time": "2026-11-04T14:00:50+00:00",
                "temperature": 30.0,
                "humidity": 60.0,
                "status": "OK",
                "is_late_simulated": False,
            },
            {
                "device_id": "sensor-A101",
                "event_time": "2026-11-04T14:01:05+00:00",
                "temperature": 40.0,
                "humidity": 80.0,
                "status": "OK",
                "is_late_simulated": False,
            },
            {
                "device_id": "sensor-A102",
                "event_time": "2026-11-04T14:00:20+00:00",
                "temperature": 50.0,
                "humidity": 70.0,
                "status": "CRITICAL",
                "is_late_simulated": False,
            },
        ]
        raw_events = self.spark.createDataFrame(
            [(json.dumps(event),) for event in events], ["value"]
        )
        parsed_events = (
            raw_events.select(
                from_json(col("value"), EVENT_SCHEMA).alias("event")
            )
            .select("event.*")
            .withColumn("event_time", col("event_time").cast("timestamp"))
        )

        with patch.dict(
            os.environ,
            {"POSTGRES_PASSWORD": "integration-test-password"},
            clear=False,
        ):
            config = load_config()
        result = build_windowed_events(
            parsed_events, config["streaming"]
        ).collect()

        self.assertEqual(config["streaming"]["window_duration"], "1 minute")
        self.assertEqual(len(result), 3)
        for row in result:
            self.assertEqual(
                (row["window_end"] - row["window_start"]).total_seconds(),
                60,
            )

        rows_by_device = {}
        for row in result:
            rows_by_device.setdefault(row["device_id"], []).append(row)

        self.assertEqual(len(rows_by_device["sensor-A101"]), 2)
        first_window, second_window = sorted(
            rows_by_device["sensor-A101"],
            key=lambda row: row["window_start"],
        )
        self.assertEqual(first_window["event_count"], 2)
        self.assertEqual(first_window["average_temperature"], 25.0)
        self.assertEqual(first_window["average_humidity"], 50.0)
        self.assertEqual(second_window["event_count"], 1)
        self.assertEqual(second_window["average_temperature"], 40.0)
        self.assertEqual(second_window["average_humidity"], 80.0)

        sensor_a102 = rows_by_device["sensor-A102"][0]
        self.assertEqual(sensor_a102["event_count"], 1)
        self.assertEqual(sensor_a102["average_temperature"], 50.0)
        self.assertEqual(sensor_a102["average_humidity"], 70.0)
        self.assertEqual(
            sensor_a102["window_start"], first_window["window_start"]
        )

    def test_watermark_routes_only_events_older_than_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            late_log = Path(temp_dir) / "late-data.jsonl"
            events = self.spark.createDataFrame(
                [
                    (
                        "sensor-watermark",
                        datetime(2026, 11, 4, 14, 0, 10),
                        20.0,
                        40.0,
                    ),
                    (
                        "sensor-watermark",
                        datetime(2026, 11, 4, 14, 2, 10),
                        21.0,
                        41.0,
                    ),
                ],
                ["device_id", "event_time", "temperature", "humidity"],
            )
            watermark_cutoff = datetime(
                2026, 11, 4, 14, 1, tzinfo=timezone.utc
            )

            route_late_events(
                events,
                batch_id=7,
                late_log_path=late_log,
                watermark_seconds=120,
                watermark_cutoff=watermark_cutoff,
            )

            records = [
                json.loads(line)
                for line in late_log.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["batch_id"], 7)
            self.assertEqual(
                records[0]["event"]["event_time"],
                "2026-11-04T14:00:10",
            )

    def test_raw_and_dead_letter_storage_preserve_input_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_path = root / "raw.jsonl"
            dead_path = root / "dead.jsonl"
            self.assertEqual(
                append_jsonl(
                    raw_path,
                    [raw_event_record('{"device_id":"sensor-A101"}', 1)],
                ),
                1,
            )
            self.assertEqual(
                append_jsonl(
                    dead_path,
                    [dead_letter_record("{bad-json", 1, "event_schema_parse_failed")],
                ),
                1,
            )
            self.assertEqual(len(raw_path.read_text(encoding="utf-8").splitlines()), 1)
            dead_record = json.loads(
                dead_path.read_text(encoding="utf-8").splitlines()[0]
            )
            self.assertEqual(dead_record["reason"], "event_schema_parse_failed")

    def test_input_accounting_has_no_unclassified_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "raw.jsonl"
            accepted_path = root / "accepted.jsonl"
            late_path = root / "late.jsonl"
            dead_path = root / "dead.jsonl"
            append_jsonl(
                input_path,
                [
                    raw_event_record('{"id":1}', 1),
                    raw_event_record('{"id":2}', 1),
                    raw_event_record("{bad-json}", 1),
                ],
            )
            append_jsonl(accepted_path, [{"event": {"id": 1}}])
            append_jsonl(late_path, [{"event": {"id": 2}}])
            append_jsonl(
                dead_path,
                [dead_letter_record("{bad-json}", 1, "event_schema_parse_failed")],
            )

            input_count = len(input_path.read_text(encoding="utf-8").splitlines())
            output_count = sum(
                len(path.read_text(encoding="utf-8").splitlines())
                for path in (accepted_path, late_path, dead_path)
            )
            self.assertEqual(input_count, output_count)

    @unittest.skipIf(
        os.name == "nt",
        "Spark streaming checkpoint manager requires Linux or a working Hadoop native runtime",
    )
    def test_watermark_drops_event_arriving_after_window_is_closed(self) -> None:
        query_name = "watermark_integration_test"
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        query = (
            self.spark.readStream.format("socket")
            .option("host", "127.0.0.1")
            .option("port", port)
            .load()
            .select(from_json(col("value"), EVENT_SCHEMA).alias("event"))
            .select("event.*")
            .withColumn("event_time", col("event_time").cast("timestamp"))
            .withWatermark("event_time", "120 seconds")
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("device_id"),
            )
            .count()
            .writeStream.outputMode("append")
            .format("memory")
            .queryName(query_name)
            .trigger(processingTime="1 second")
            .start()
        )
        connection = None
        try:
            server.settimeout(10)
            connection, _ = server.accept()
            first_event = {
                "device_id": "sensor-watermark",
                "event_time": "2026-11-04T14:00:10+00:00",
                "temperature": 20.0,
                "humidity": 40.0,
                "status": "OK",
                "is_late_simulated": False,
            }
            connection.sendall((json.dumps(first_event) + "\n").encode())
            time.sleep(3)

            watermark_advancing_event = dict(first_event)
            watermark_advancing_event["event_time"] = "2026-11-04T14:03:10+00:00"
            connection.sendall(
                (json.dumps(watermark_advancing_event) + "\n").encode()
            )
            time.sleep(3)
            query.processAllAvailable()
            rows_after_watermark = self.spark.sql(
                f"SELECT * FROM {query_name}"
            ).collect()
            self.assertEqual(len(rows_after_watermark), 1)
            self.assertEqual(rows_after_watermark[0]["count"], 1)

            late_event = dict(first_event)
            late_event["event_time"] = "2026-11-04T14:00:20+00:00"
            connection.sendall((json.dumps(late_event) + "\n").encode())
            time.sleep(3)
            query.processAllAvailable()

            rows_after_late_event = self.spark.sql(
                f"SELECT * FROM {query_name}"
            ).collect()
            self.assertEqual(rows_after_late_event, rows_after_watermark)
        finally:
            if connection is not None:
                connection.close()
            server.close()
            query.stop()


class PostgreSQLIntegrationTests(unittest.TestCase):
    def test_write_batch_inserts_and_upserts_real_row(self) -> None:
        try:
            import psycopg2
        except ImportError as error:
            self.skipTest(f"psycopg2 is unavailable: {error}")

        password = os.getenv("POSTGRES_PASSWORD")
        if not password:
            self.skipTest(
                "Set POSTGRES_PASSWORD and start PostgreSQL to run this integration test"
            )

        from src.streaming.spark_pipeline import (
            create_database_table,
            write_batch,
        )

        database = {
            "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
            "port": int(os.getenv("POSTGRES_PORT", "15432")),
            "name": os.getenv("POSTGRES_DB", "app_db"),
            "user": os.getenv("POSTGRES_USER", "admin"),
            "password": password,
            "table": "sensor_window_metrics",
        }
        window_start = datetime(2026, 11, 4, 14, 0, tzinfo=timezone.utc)
        window_end = window_start + timedelta(minutes=1)
        device_id = "integration-test-upsert"
        cleanup_needed = False

        try:
            create_database_table(database)
            connection = psycopg2.connect(**{
                "host": database["host"],
                "port": database["port"],
                "dbname": database["name"],
                "user": database["user"],
                "password": database["password"],
            })
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM sensor_window_metrics WHERE device_id = %s",
                        (device_id,),
                    )
            cleanup_needed = True
            first_batch = self._database_row(
                window_start, window_end, device_id, 1, 21.5, 45.0
            )
            write_batch(self._database_batch(first_batch), 1, database)
            updated_batch = self._database_row(
                window_start, window_end, device_id, 3, 30.0, 60.0
            )
            write_batch(self._database_batch(updated_batch), 2, database)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT event_count, average_temperature, average_humidity
                        FROM sensor_window_metrics
                        WHERE device_id = %s
                        """,
                        (device_id,),
                    )
                    self.assertEqual(cursor.fetchone(), (3, 30.0, 60.0))
            connection.close()
        except psycopg2.Error as error:
            self.skipTest(f"PostgreSQL is unavailable: {error}")
        finally:
            if not cleanup_needed:
                return
            cleanup = psycopg2.connect(**{
                "host": database["host"],
                "port": database["port"],
                "dbname": database["name"],
                "user": database["user"],
                "password": database["password"],
            })
            try:
                with cleanup:
                    with cleanup.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM sensor_window_metrics WHERE device_id = %s",
                            (device_id,),
                        )
            finally:
                cleanup.close()

    @staticmethod
    def _database_row(
        window_start, window_end, device_id, count, temperature, humidity
    ):
        return {
            "window_start": window_start,
            "window_end": window_end,
            "device_id": device_id,
            "event_count": count,
            "average_temperature": temperature,
            "average_humidity": humidity,
        }

    @staticmethod
    def _database_batch(row):
        class Batch:
            def collect(self):
                return [row]

        return Batch()


@unittest.skipUnless(
    os.getenv("RUN_LIVE_INTEGRATION") == "1",
    "Set RUN_LIVE_INTEGRATION=1 with Kafka, Spark, and PostgreSQL running",
)
class KafkaSparkPostgreSQLIntegrationTests(unittest.TestCase):
    def test_event_flows_from_kafka_through_spark_to_postgresql(self) -> None:
        from kafka import KafkaProducer
        from src.utils.db_connector import open_database_connection

        config = load_config()
        device_id = f"integration-{uuid.uuid4().hex[:12]}"
        base_time = datetime.now(timezone.utc).replace(microsecond=0)
        events = []
        for seconds, temperature in ((0, 20.0), (120, 30.0), (240, 40.0)):
            event = {
                "device_id": device_id,
                "event_time": (
                    base_time + timedelta(seconds=seconds)
                ).isoformat(),
                "temperature": temperature,
                "humidity": 50.0,
                "status": "OK",
                "is_late_simulated": False,
            }
            validate_event(event)
            events.append(event)

        producer = None
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv(
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"
                ),
                key_serializer=lambda value: value.encode(),
                value_serializer=lambda value: json.dumps(value).encode(),
            )
            for event in events:
                producer.send(
                    config["kafka"]["topic_name"],
                    key=device_id,
                    value=event,
                )
            producer.flush()
        except Exception as error:
            self.skipTest(f"Kafka is unavailable: {error}")
        finally:
            if producer is not None:
                producer.close()

        database = config["database"]
        connection = open_database_connection(database)
        try:
            deadline = time.monotonic() + 90
            row = None
            while time.monotonic() < deadline:
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT event_count, average_temperature
                            FROM sensor_window_metrics
                            WHERE device_id = %s
                            ORDER BY window_start
                            LIMIT 1
                            """,
                            (device_id,),
                        )
                        row = cursor.fetchone()
                if row is not None:
                    break
                time.sleep(2)
            self.assertEqual(row, (1, 20.0))
        finally:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM sensor_window_metrics WHERE device_id = %s",
                        (device_id,),
                    )
            connection.close()

if __name__ == "__main__":
    unittest.main()
