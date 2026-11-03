"""PostgreSQL connection helpers shared by streaming and integrity checks."""

from typing import Any

import psycopg2


def open_database_connection(database: dict[str, Any]) -> Any:
    required = ("host", "port", "name", "user", "password")
    missing = [key for key in required if database.get(key) in (None, "")]
    if missing:
        raise KeyError(f"Missing database configuration: {', '.join(missing)}")

    return psycopg2.connect(
        host=database["host"],
        port=database["port"],
        dbname=database["name"],
        user=database["user"],
        password=database["password"],
    )
