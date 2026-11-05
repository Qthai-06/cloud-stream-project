"""
Streaming metrics listener implementation.
Được phát triển bởi Tân Lưu (Tuần 4 - Đưa các chỉ số Throughput, Checkpoint lên Prometheus & Grafana).
"""

import sys
from pathlib import Path

# Thêm đường dẫn tới thư mục NhutTan
PROJECT_ROOT = Path(__file__).resolve().parents[2]
NHUT_TAN_PATH = PROJECT_ROOT / "NhutTan"
if str(NHUT_TAN_PATH) not in sys.path:
    sys.path.insert(0, str(NHUT_TAN_PATH))

try:
    from NhutTan.src.spark_metrics_listener import (
        MetricsCollector,
        PySparkMetricsListener,
        start_metrics_server,
        GAUGE_INPUT_RATE,
        GAUGE_PROCESSED_RATE,
        GAUGE_BATCH_DURATION,
        GAUGE_CHECKPOINT_DURATION,
        COUNTER_LATE_EVENTS,
        COUNTER_TOTAL_INPUT_ROWS,
    )
except ImportError:
    from src.spark_metrics_listener import (  # type: ignore
        MetricsCollector,
        PySparkMetricsListener,
        start_metrics_server,
        GAUGE_INPUT_RATE,
        GAUGE_PROCESSED_RATE,
        GAUGE_BATCH_DURATION,
        GAUGE_CHECKPOINT_DURATION,
        COUNTER_LATE_EVENTS,
        COUNTER_TOTAL_INPUT_ROWS,
    )

__all__ = [
    "MetricsCollector",
    "PySparkMetricsListener",
    "start_metrics_server",
    "GAUGE_INPUT_RATE",
    "GAUGE_PROCESSED_RATE",
    "GAUGE_BATCH_DURATION",
    "GAUGE_CHECKPOINT_DURATION",
    "COUNTER_LATE_EVENTS",
    "COUNTER_TOTAL_INPUT_ROWS",
]
