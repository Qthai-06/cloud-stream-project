"""Post-experiment batch evaluator: analyzes window results, late-data logs and integrity."""

import json
import logging
import os
from pathlib import Path
from typing import Any

from src.utils.config_loader import load_config
from src.utils.db_connector import open_database_connection

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def count_window_records(database: dict) -> int:
    """Return the total number of rows in ``sensor_window_metrics``.

    Args:
        database: Database connection parameters as returned by ``load_config()``.

    Returns:
        Total row count as an integer.
    """
    conn = open_database_connection(database)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sensor_window_metrics;")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


def check_duplicate_windows(database: dict) -> list[dict]:
    """Detect duplicate primary-key combinations in ``sensor_window_metrics``.

    A duplicate is defined as more than one row sharing the same
    (window_start, window_end, device_id) composite key.

    Args:
        database: Database connection parameters as returned by ``load_config()``.

    Returns:
        A list of dicts, each with keys ``window_start``, ``window_end``,
        ``device_id``, and ``count``.  An empty list means no duplicates.
    """
    query = """
        SELECT
            window_start,
            window_end,
            device_id,
            COUNT(*) AS cnt
        FROM sensor_window_metrics
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1;
    """
    conn = open_database_connection(database)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [
                {
                    "window_start": row[0],
                    "window_end": row[1],
                    "device_id": row[2],
                    "count": int(row[3]),
                }
                for row in rows
            ]
    finally:
        conn.close()


def compute_window_stats(database: dict) -> dict[str, Any]:
    """Compute aggregate statistics over ``sensor_window_metrics``.

    Args:
        database: Database connection parameters as returned by ``load_config()``.

    Returns:
        Dict with keys:
        - ``min_event_count`` (int)
        - ``max_event_count`` (int)
        - ``avg_event_count`` (float)
        - ``distinct_devices`` (int)
        - ``earliest_window`` (datetime | None)
        - ``latest_window`` (datetime | None)
    """
    query = """
        SELECT
            MIN(event_count),
            MAX(event_count),
            AVG(event_count),
            COUNT(DISTINCT device_id),
            MIN(window_start),
            MAX(window_end)
        FROM sensor_window_metrics;
    """
    conn = open_database_connection(database)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            if not row:
                return {
                    "min_event_count": None,
                    "max_event_count": None,
                    "avg_event_count": None,
                    "distinct_devices": 0,
                    "earliest_window": None,
                    "latest_window": None,
                }
            return {
                "min_event_count": int(row[0]) if row[0] is not None else None,
                "max_event_count": int(row[1]) if row[1] is not None else None,
                "avg_event_count": float(row[2]) if row[2] is not None else None,
                "distinct_devices": int(row[3]) if row[3] is not None else 0,
                "earliest_window": row[4],
                "latest_window": row[5],
            }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Log-file helpers
# ---------------------------------------------------------------------------


def count_jsonl_lines(path: Path) -> int:
    """Count non-empty lines in a JSONL file.

    Args:
        path: Filesystem path to the ``.jsonl`` file.

    Returns:
        Number of non-empty lines, or ``0`` if the file does not exist.
    """
    if not path.exists():
        logger.debug("JSONL file not found, returning 0: %s", path)
        return 0

    count = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def evaluate_late_data_accounting(log_dir: Path) -> dict[str, int]:
    """Assess event accounting across the four canonical log files.

    Reads line counts from:
    - ``raw_events.jsonl``      – every event that entered the pipeline
    - ``accepted_events.jsonl`` – events processed within the allowed window
    - ``late_data.jsonl``       – events that arrived after their window closed
    - ``dead_letter.jsonl``     – events that could not be processed at all

    ``unclassified = raw - (accepted + late + dead_letter)``
    A healthy pipeline should produce ``unclassified == 0``.

    Args:
        log_dir: Directory that contains the ``.jsonl`` log files.

    Returns:
        Dict with keys: ``raw``, ``accepted``, ``late``, ``dead_letter``,
        ``unclassified``.
    """
    raw = count_jsonl_lines(log_dir / "raw_events.jsonl")
    accepted = count_jsonl_lines(log_dir / "accepted_events.jsonl")
    late = count_jsonl_lines(log_dir / "late_data.jsonl")
    dead_letter = count_jsonl_lines(log_dir / "dead_letter.jsonl")
    unclassified = raw - (accepted + late + dead_letter)

    return {
        "raw": raw,
        "accepted": accepted,
        "late": late,
        "dead_letter": dead_letter,
        "unclassified": unclassified,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def run_evaluation(database: dict, log_dir: Path) -> None:
    """Execute all evaluation checks and print a human-readable report.

    Args:
        database: Database connection parameters.
        log_dir: Directory containing the pipeline's JSONL log files.
    """
    separator = "=" * 60

    # ------------------------------------------------------------------
    # Section 1 – Window record count
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("=== WINDOW RECORD COUNT")
    print(separator)
    total_records = count_window_records(database)
    print(f"  Total rows in sensor_window_metrics : {total_records:,}")

    # ------------------------------------------------------------------
    # Section 2 – Duplicate key check
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("=== DUPLICATE WINDOW CHECK")
    print(separator)
    duplicates = check_duplicate_windows(database)
    if duplicates:
        print(f"  ⚠  WARNING: {len(duplicates)} duplicate key group(s) found!")
        for dup in duplicates:
            print(
                f"     window_start={dup['window_start']}  "
                f"window_end={dup['window_end']}  "
                f"device_id={dup['device_id']}  "
                f"count={dup['count']}"
            )
    else:
        print("  ✓  No duplicate primary-key combinations detected.")

    # ------------------------------------------------------------------
    # Section 3 – Window statistics
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("=== WINDOW STATISTICS")
    print(separator)
    stats = compute_window_stats(database)
    print(f"  Min event count      : {stats['min_event_count']}")
    print(f"  Max event count      : {stats['max_event_count']}")
    avg = stats["avg_event_count"]
    print(f"  Avg event count      : {avg:.2f}" if avg is not None else "  Avg event count      : N/A")
    print(f"  Distinct devices     : {stats['distinct_devices']}")
    print(f"  Earliest window      : {stats['earliest_window']}")
    print(f"  Latest window        : {stats['latest_window']}")

    # ------------------------------------------------------------------
    # Section 4 – Late data accounting
    # ------------------------------------------------------------------
    print(f"\n{separator}")
    print("=== LATE DATA ACCOUNTING")
    print(separator)
    accounting = evaluate_late_data_accounting(log_dir)
    print(f"  Raw events           : {accounting['raw']:,}")
    print(f"  Accepted events      : {accounting['accepted']:,}")
    print(f"  Late events          : {accounting['late']:,}")
    print(f"  Dead-letter events   : {accounting['dead_letter']:,}")
    print(f"  Unclassified events  : {accounting['unclassified']:,}")
    if accounting["unclassified"] != 0:
        print(
            f"\n  ⚠  WARNING: {accounting['unclassified']} event(s) are unaccounted for. "
            "Investigate pipeline logs."
        )
    else:
        print("  ✓  All events are fully accounted for.")

    print(f"\n{separator}")
    print("=== EVALUATION COMPLETE")
    print(separator)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Load configuration, apply environment-variable overrides, and run evaluation."""
    config = load_config()
    database: dict = config.get("database", {})

    # Allow environment variables to override configuration-file values.
    env_overrides: dict[str, str] = {
        "host": "DB_HOST",
        "port": "DB_PORT",
        "dbname": "DB_NAME",
        "user": "DB_USER",
        "password": "DB_PASSWORD",
    }
    for db_key, env_var in env_overrides.items():
        value = os.environ.get(env_var)
        if value is not None:
            database[db_key] = value
            logger.debug("Overriding database.%s from environment variable %s", db_key, env_var)

    log_dir: Path = PROJECT_ROOT / "logs"

    logger.info("Starting batch evaluation (log_dir=%s)", log_dir)
    run_evaluation(database=database, log_dir=log_dir)


if __name__ == "__main__":
    main()
