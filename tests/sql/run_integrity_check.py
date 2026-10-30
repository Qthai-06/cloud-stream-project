"""Run the PostgreSQL integrity checks without shell redirection."""

import argparse
import os
from collections.abc import Sequence

import psycopg2


CHECKS: Sequence[tuple[str, str]] = (
    (
        "required_key_columns_are_not_null",
        """
        SELECT COUNT(*)
        FROM sensor_window_metrics
        WHERE window_start IS NULL
           OR window_end IS NULL
           OR device_id IS NULL
        """,
    ),
    (
        "window_bounds_are_valid",
        """
        SELECT COUNT(*)
        FROM sensor_window_metrics
        WHERE window_end <= window_start
        """,
    ),
    (
        "duplicate_logical_rows",
        """
        SELECT COUNT(*)
        FROM (
            SELECT window_start, window_end, device_id
            FROM sensor_window_metrics
            GROUP BY window_start, window_end, device_id
            HAVING COUNT(*) > 1
        ) duplicates
        """,
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "app_db"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "admin"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*),
                       COALESCE(
                           md5(string_agg(
                               concat_ws(
                                   '|', window_start::text, window_end::text,
                                   device_id, event_count::text,
                                   COALESCE(average_temperature::text, ''),
                                   COALESCE(average_humidity::text, '')
                               ),
                               '||' ORDER BY window_start, window_end, device_id
                           )),
                           md5('')
                       )
                FROM sensor_window_metrics
                """
            )
            row_count, checksum = cursor.fetchone()
            print(f"row_count={row_count}")
            print(f"result_checksum={checksum}")

            failed = False
            for check_name, query in CHECKS:
                cursor.execute(query)
                invalid_count = cursor.fetchone()[0]
                result = "PASS" if invalid_count == 0 else "FAIL"
                print(f"{check_name}: {result} ({invalid_count})")
                failed |= invalid_count != 0
            return 1 if failed else 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
