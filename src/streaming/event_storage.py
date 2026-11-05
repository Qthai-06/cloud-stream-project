"""Durable JSONL storage for raw, invalid, and late streaming events."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def append_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    """Append records durably and return the number of records written."""
    records = list(records)
    if not records:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, default=json_default) + "\n")
        output.flush()
    return len(records)


def raw_event_record(raw_payload: str, batch_id: int) -> dict[str, Any]:
    return {
        "stored_at": datetime.now(timezone.utc),
        "batch_id": batch_id,
        "raw_payload": raw_payload,
    }


def dead_letter_record(
    raw_payload: str, batch_id: int, reason: str
) -> dict[str, Any]:
    return {
        "stored_at": datetime.now(timezone.utc),
        "batch_id": batch_id,
        "reason": reason,
        "raw_payload": raw_payload,
    }
