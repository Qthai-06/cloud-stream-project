"""Regression tests for the configured streaming pipeline components."""

import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.generator.event_producer import generator_sensor_event
from src.streaming.late_data_router import duration_to_seconds
from src.utils.config_loader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PipelineConfigurationTests(unittest.TestCase):
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
        _, event = generator_sensor_event(["sensor-A101"], 0, 0, 0)
        event_time = datetime.fromisoformat(event["event_time"])
        self.assertIsNotNone(event_time.tzinfo)
        self.assertEqual(event_time.tzinfo, timezone.utc)

    def test_watermark_duration_parser_supports_seconds_and_minutes(self) -> None:
        self.assertEqual(duration_to_seconds("120 seconds"), 120)
        self.assertEqual(duration_to_seconds("2 minutes"), 120)


if __name__ == "__main__":
    unittest.main()
