"""Independent validation rules for sensor event payloads."""

import math
from datetime import datetime
from typing import Any, Mapping


REQUIRED_EVENT_FIELDS = frozenset(
    {
        "device_id",
        "event_time",
        "temperature",
        "humidity",
        "status",
        "is_late_simulated",
    }
)
VALID_STATUSES = frozenset({"OK", "CRITICAL"})


class EventValidationError(ValueError):
    """Raised when an event does not conform to the sensor event schema."""


def validate_event(event: Mapping[str, Any]) -> None:
    """Validate a sensor event before it is serialized or published."""
    missing_fields = REQUIRED_EVENT_FIELDS.difference(event)
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise EventValidationError(f"Missing required event field(s): {missing}")

    unexpected_fields = set(event).difference(REQUIRED_EVENT_FIELDS)
    if unexpected_fields:
        unexpected = ", ".join(sorted(unexpected_fields))
        raise EventValidationError(f"Unexpected event field(s): {unexpected}")

    device_id = event["device_id"]
    if not isinstance(device_id, str) or not device_id.strip():
        raise EventValidationError("device_id must be a non-empty string")

    event_time = event["event_time"]
    if not isinstance(event_time, str):
        raise EventValidationError("event_time must be an ISO-8601 string")
    try:
        parsed_event_time = datetime.fromisoformat(event_time)
    except ValueError as error:
        raise EventValidationError("event_time must be a valid ISO-8601 string") from error
    if parsed_event_time.tzinfo is None:
        raise EventValidationError("event_time must include a timezone")

    for field, minimum, maximum in (
        ("temperature", 20.0, 85.0),
        ("humidity", 30.0, 90.0),
    ):
        value = event[field]
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not minimum <= value <= maximum
        ):
            raise EventValidationError(
                f"{field} must be a finite number between {minimum} and {maximum}"
            )

    if not isinstance(event["status"], str) or event["status"] not in VALID_STATUSES:
        raise EventValidationError("status must be either 'OK' or 'CRITICAL'")
    if not isinstance(event["is_late_simulated"], bool):
        raise EventValidationError("is_late_simulated must be a boolean")


def build_spark_event_schema() -> Any:
    """Build the Spark schema from the independent event field definition."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("device_id", StringType(), nullable=False),
            StructField("event_time", StringType(), nullable=False),
            StructField("temperature", DoubleType(), nullable=False),
            StructField("humidity", DoubleType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("is_late_simulated", BooleanType(), nullable=False),
        ]
    )
