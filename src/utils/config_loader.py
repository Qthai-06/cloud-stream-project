"""Load pipeline configuration with environment-specific connection overrides."""

import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    kafka = config.setdefault("kafka", {})
    kafka["bootstrap_servers"] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        kafka.get("bootstrap_servers", "redpanda:29092"),
    )

    database = config.setdefault("database", {})
    database["host"] = os.getenv("POSTGRES_HOST", database.get("host", "127.0.0.1"))
    database["port"] = int(os.getenv("POSTGRES_PORT", database.get("port", 5432)))
    database["user"] = os.getenv("POSTGRES_USER", database.get("user", "admin"))
    database["name"] = os.getenv("POSTGRES_DB", database.get("name", "app_db"))
    configured_password = database.get("password", "")
    database["password"] = os.getenv(
        "POSTGRES_PASSWORD",
        "" if configured_password == "${POSTGRES_PASSWORD}" else configured_password,
    )
    database["table"] = database.get("table", "sensor_window_metrics")
    if not database["password"]:
        raise ValueError("POSTGRES_PASSWORD must be configured for the database sink")

    return config
