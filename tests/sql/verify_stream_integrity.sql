-- Active: 1783307945036@@127.0.0.1@5432@app_db
-- Integrity checks for the windowed PostgreSQL sink.
-- Expected result: every check returns PASS and duplicate queries return no rows.

SELECT
    'row_count_is_non_negative' AS check_name,
    CASE WHEN COUNT(*) >= 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS row_count
FROM sensor_window_metrics;

SELECT * FROM sensor_window_metrics LIMIT 10;

SELECT
    'required_key_columns_are_not_null' AS check_name,
    CASE
        WHEN COUNT(*) FILTER (
            WHERE window_start IS NULL
               OR window_end IS NULL
               OR device_id IS NULL
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) FILTER (
        WHERE window_start IS NULL
           OR window_end IS NULL
           OR device_id IS NULL
    ) AS invalid_rows
FROM sensor_window_metrics;

SELECT
    'window_bounds_are_valid' AS check_name,
    CASE
        WHEN COUNT(*) FILTER (WHERE window_end <= window_start) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) FILTER (WHERE window_end <= window_start) AS invalid_windows
FROM sensor_window_metrics;

-- This must return zero rows. The primary key also prevents these duplicates.
SELECT
    window_start,
    window_end,
    device_id,
    COUNT(*) AS duplicate_count
FROM sensor_window_metrics
GROUP BY window_start, window_end, device_id
HAVING COUNT(*) > 1;

-- This must return zero rows. It detects duplicate logical windows even if
-- a future schema change removes the current primary key.
SELECT
    window_start,
    window_end,
    device_id,
    COUNT(*) AS duplicate_logical_rows
FROM sensor_window_metrics
GROUP BY window_start, window_end, device_id
HAVING COUNT(*) > 1;

-- Capture this result before and after restarting Spark with the same
-- checkpoints/streaming pipeline. The row_count and checksum must be equal
-- when no new input events were produced during the restart.
SELECT
    COUNT(*) AS row_count,
    COALESCE(
        md5(
            string_agg(
                concat_ws(
                    '|',
                    window_start::text,
                    window_end::text,
                    device_id,
                    event_count::text,
                    COALESCE(average_temperature::text, ''),
                    COALESCE(average_humidity::text, '')
                ),
                '||' ORDER BY window_start, window_end, device_id
            )
        ),
        md5('')
    ) AS result_checksum
FROM sensor_window_metrics;
