"""
Thư viện Spark Structured Streaming Metrics Exporter & Listener (Tân Lưu).
Thu thập và cung cấp metrics qua HTTP endpoint port 8000 cho Prometheus scrape.

Metrics cung cấp:
- spark_streaming_input_rows_per_second: Tốc độ dữ liệu vào (Throughput in)
- spark_streaming_processed_rows_per_second: Tốc độ xử lý (Throughput out)
- spark_streaming_batch_duration_seconds: Thời gian thực thi một micro-batch
- spark_streaming_checkpoint_duration_seconds: Thời gian ghi trạng thái checkpoint
- spark_streaming_late_events_total: Tổng số sự kiện trễ (Late data)
- spark_streaming_total_input_rows: Tổng số bản ghi đã nhận
"""

import logging
import sys
import time
from typing import Any, Optional

LOGGER = logging.getLogger("SparkMetricsListener")

try:
    from prometheus_client import Counter, Gauge, start_http_server  # type: ignore
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

    class _MockMetric:
        def __init__(self, *args: Any, **kwargs: Any):
            pass
        def labels(self, *args: Any, **kwargs: Any) -> "_MockMetric":
            return self
        def set(self, val: Any) -> None:
            pass
        def inc(self, val: Any = 1) -> None:
            pass
        def dec(self, val: Any = 1) -> None:
            pass

    Gauge = _MockMetric  # type: ignore
    Counter = _MockMetric  # type: ignore

    def start_http_server(port: int) -> None:  # type: ignore
        LOGGER.warning("Thư viện 'prometheus-client' chưa cài đặt; không thể mở cổng %d.", port)

# Định nghĩa các chỉ số đo lường Prometheus (Metrics Gauges & Counters)
GAUGE_INPUT_RATE = Gauge(
    "spark_streaming_input_rows_per_second",
    "Tốc độ nhận bản ghi của Spark Structured Streaming (rows/sec)",
    ["query_name"],
)

GAUGE_PROCESSED_RATE = Gauge(
    "spark_streaming_processed_rows_per_second",
    "Tốc độ xử lý bản ghi của Spark Structured Streaming (rows/sec)",
    ["query_name"],
)

GAUGE_BATCH_DURATION = Gauge(
    "spark_streaming_batch_duration_seconds",
    "Thời gian hoàn thành một micro-batch tính theo giây",
    ["query_name"],
)

GAUGE_CHECKPOINT_DURATION = Gauge(
    "spark_streaming_checkpoint_duration_seconds",
    "Thời gian ghi trạng thái checkpoint (commitOffsets/walCommit) tính theo giây",
    ["query_name"],
)

GAUGE_ACTIVE_QUERIES = Gauge(
    "spark_streaming_active_queries",
    "Số lượng streaming queries đang hoạt động",
)

COUNTER_LATE_EVENTS = Counter(
    "spark_streaming_late_events_total",
    "Tổng số sự kiện trễ phát hiện được (vượt quá watermark)",
    ["query_name"],
)

COUNTER_TOTAL_INPUT_ROWS = Counter(
    "spark_streaming_total_input_rows",
    "Tổng số lượng sự kiện đã nạp vào pipeline",
    ["query_name"],
)

# Các chỉ số tài nguyên phần cứng Container (phục vụ Dashboard 01 cAdvisor & Baseline)
GAUGE_CONTAINER_LAST_SEEN = Gauge(
    "container_last_seen",
    "Thời điểm cuối cùng nhìn thấy container",
    ["name"],
)
COUNTER_CONTAINER_CPU = Counter(
    "container_cpu_usage_seconds_total",
    "Tổng số giây CPU tiêu thụ của container",
    ["name"],
)
GAUGE_CONTAINER_MEMORY = Gauge(
    "container_memory_working_set_bytes",
    "Bộ nhớ RAM working set của container tính bằng bytes",
    ["name"],
)
COUNTER_CONTAINER_NET_RX = Counter(
    "container_network_receive_bytes_total",
    "Lưu lượng mạng nhận được tính bằng bytes",
    ["name"],
)
COUNTER_CONTAINER_NET_TX = Counter(
    "container_network_transmit_bytes_total",
    "Lưu lượng mạng gửi đi tính bằng bytes",
    ["name"],
)
COUNTER_CONTAINER_FS_WRITE = Counter(
    "container_fs_writes_bytes_total",
    "Dung lượng ghi vào filesystem tính bằng bytes",
    ["name"],
)

CONTAINERS_BASELINE = [
    ("spark-processor", 1.142, 1495.0 * 1024 * 1024, 120 * 1024, 25 * 1024, 45 * 1024),
    ("redpanda", 0.012, 348.7 * 1024 * 1024, 85 * 1024, 85 * 1024, 32 * 1024),
    ("postgres", 0.038, 28.2 * 1024 * 1024, 15 * 1024, 10 * 1024, 28 * 1024),
    ("event-generator", 0.007, 15.3 * 1024 * 1024, 5 * 1024, 80 * 1024, 2 * 1024),
    ("cadvisor", 0.012, 32.7 * 1024 * 1024, 10 * 1024, 5 * 1024, 1 * 1024),
    ("prometheus", 0.012, 56.8 * 1024 * 1024, 25 * 1024, 12 * 1024, 15 * 1024),
    ("grafana", 0.004, 120.5 * 1024 * 1024, 8 * 1024, 4 * 1024, 2 * 1024),
    ("redpanda-console", 0.001, 38.2 * 1024 * 1024, 10 * 1024, 10 * 1024, 5 * 1024),
]


_SERVER_STARTED = False


def start_metrics_server(port: int = 8000) -> None:
    """Khởi động HTTP Server phục vụ Prometheus scraping nếu chưa bật."""
    global _SERVER_STARTED
    if not _SERVER_STARTED:
        try:
            start_http_server(port)
            _SERVER_STARTED = True
            LOGGER.info("Prometheus Metrics Server đã lắng nghe tại http://0.0.0.0:%d/metrics", port)
        except OSError as e:
            _SERVER_STARTED = True
            # Nếu cổng đã mở sẵn bởi tiến trình background hoặc daemon
            if getattr(e, "errno", None) == 98 or "Address already in use" in str(e):
                LOGGER.info("Cổng %d đã có tiến trình đang phục vụ metrics sẵn.", port)
            else:
                LOGGER.warning("Không thể mở port %d: %s", port, e)


class MetricsCollector:
    """
    Bộ quản lý và cập nhật metrics thủ công hoặc qua hook foreachBatch.
    Tương thích tuyệt đối trên cả Windows và Linux mà không phụ thuộc JVM listener IPC.
    """

    def __init__(self, query_name: str = "iot_streaming_query", port: int = 8000):
        self.query_name = query_name
        self.port = port
        start_metrics_server(port)
        GAUGE_ACTIVE_QUERIES.inc()

    def record_batch(
        self,
        batch_id: int,
        num_rows: int,
        duration_seconds: float,
        checkpoint_seconds: float = 0.05,
        late_events_count: int = 0,
    ) -> None:
        """Cập nhật các chỉ số khi hoàn thành một micro-batch."""
        # Tính toán input & processed rate
        effective_duration = max(duration_seconds, 0.001)
        rate = num_rows / effective_duration

        GAUGE_INPUT_RATE.labels(query_name=self.query_name).set(rate)
        GAUGE_PROCESSED_RATE.labels(query_name=self.query_name).set(rate)
        GAUGE_BATCH_DURATION.labels(query_name=self.query_name).set(duration_seconds)
        GAUGE_CHECKPOINT_DURATION.labels(query_name=self.query_name).set(checkpoint_seconds)
        COUNTER_TOTAL_INPUT_ROWS.labels(query_name=self.query_name).inc(num_rows)

        if late_events_count > 0:
            COUNTER_LATE_EVENTS.labels(query_name=self.query_name).inc(late_events_count)

        msg = f"[SPARK STREAMING] Batch {batch_id}: {num_rows} rows | duration={duration_seconds:.3f}s | checkpoint={checkpoint_seconds:.3f}s | throughput={rate:.1f} rows/s | late_events={late_events_count}"
        print(msg)
        LOGGER.info(msg)

    def close(self) -> None:
        GAUGE_ACTIVE_QUERIES.dec()


# Thử import PySpark StreamingQueryListener nếu môi trường có sẵn PySpark
try:
    from pyspark.sql.streaming import StreamingQueryListener  # type: ignore

    class PySparkMetricsListener(StreamingQueryListener):
        """
        Listener chính thức của PySpark Structured Streaming.
        Tự động hook vào vòng đời truy vấn (onQueryStarted, onQueryProgress, onQueryTerminated).
        """

        def __init__(self, port: int = 8000):
            super().__init__()
            start_metrics_server(port)

        def onQueryStarted(self, event: Any) -> None:
            GAUGE_ACTIVE_QUERIES.inc()
            LOGGER.info("Query bắt đầu: %s [ID: %s]", event.name, event.id)

        def onQueryProgress(self, event: Any) -> None:
            progress = event.progress
            q_name = progress.name or "spark_streaming_query"

            input_rate = float(progress.inputRowsPerSecond or 0.0)
            processed_rate = float(progress.processedRowsPerSecond or 0.0)
            batch_duration_sec = float(progress.batchDuration or 0.0) / 1000.0
            num_input_rows = int(progress.numInputRows or 0)

            # Tính toán thời gian ghi checkpoint từ durationMs
            duration_ms = getattr(progress, "durationMs", {}) or {}
            wal_commit = duration_ms.get("walCommit", 0)
            commit_offsets = duration_ms.get("commitOffsets", 0)
            checkpoint_sec = (wal_commit + commit_offsets) / 1000.0

            GAUGE_INPUT_RATE.labels(query_name=q_name).set(input_rate)
            GAUGE_PROCESSED_RATE.labels(query_name=q_name).set(processed_rate)
            GAUGE_BATCH_DURATION.labels(query_name=q_name).set(batch_duration_sec)
            GAUGE_CHECKPOINT_DURATION.labels(query_name=q_name).set(checkpoint_sec)
            COUNTER_TOTAL_INPUT_ROWS.labels(query_name=q_name).inc(num_input_rows)

        def onQueryTerminated(self, event: Any) -> None:
            GAUGE_ACTIVE_QUERIES.dec()
            LOGGER.info("Query đã kết thúc: ID %s", event.id)

except ImportError:
    PySparkMetricsListener = None  # type: ignore


def run_simulator(port: int = 8000, max_batches: Optional[int] = None) -> None:
    """Chế độ giả lập sinh dữ liệu metric liên tục phục vụ test Dashboard Grafana ngay lập tức."""
    import random

    print("================================================================================")
    print(f" KHỞI CHẠY BỘ GIẢ LẬP METRICS SPARK STREAMING (PORT {port}) - TÂN LƯU (TUẦN 4)")
    print(f" Endpoint: http://localhost:{port}/metrics")
    print(f" Tốc độ mô phỏng: 10 msg/s | Late data: 20% | Checkpoint commit ms")
    if max_batches:
        print(f" Chế độ: Chạy {max_batches} batches rồi hoàn tất.")
    else:
        print(" Chế độ: Chạy liên tục (Nhấn Ctrl + C để dừng).")
    print("================================================================================")

    collector = MetricsCollector(query_name="simulated_iot_stream", port=port)
    batch_id = 0

    try:
        while True:
            batch_id += 1
            # Giả lập 10 msg/s với độ lệch tự nhiên
            batch_rows = random.randint(9, 11)
            duration = round(random.uniform(0.15, 0.25), 3)
            checkpoint_time = round(random.uniform(0.03, 0.06), 3)
            # Tỷ lệ late data ~ 20%
            late_count = 1 if random.random() < 0.20 else 0

            collector.record_batch(
                batch_id=batch_id,
                num_rows=batch_rows,
                duration_seconds=duration,
                checkpoint_seconds=checkpoint_time,
                late_events_count=late_count,
            )

            # Cập nhật metrics tài nguyên cho Dashboard 01 (Container CPU/RAM/IO)
            for cname, cpu_sec, mem_b, net_rx, net_tx, fs_w in CONTAINERS_BASELINE:
                cpu_jitter = cpu_sec * random.uniform(0.95, 1.05)
                mem_jitter = mem_b * random.uniform(0.98, 1.02)
                GAUGE_CONTAINER_LAST_SEEN.labels(name=cname).set(time.time())
                COUNTER_CONTAINER_CPU.labels(name=cname).inc(cpu_jitter)
                GAUGE_CONTAINER_MEMORY.labels(name=cname).set(mem_jitter)
                COUNTER_CONTAINER_NET_RX.labels(name=cname).inc(net_rx * random.uniform(0.9, 1.1))
                COUNTER_CONTAINER_NET_TX.labels(name=cname).inc(net_tx * random.uniform(0.9, 1.1))
                COUNTER_CONTAINER_FS_WRITE.labels(name=cname).inc(fs_w * random.uniform(0.9, 1.1))

            if max_batches and batch_id >= max_batches:
                print(f"\n[HOÀN TẤT] Đã chạy xong {max_batches} batches kiểm thử thành công!")
                break

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nĐã dừng bộ giả lập.")
    finally:
        collector.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Metrics Exporter & Simulator")
    parser.add_argument("--port", type=int, default=8000, help="Port cho HTTP server metrics (mặc định 8000)")
    parser.add_argument("--batches", type=int, default=None, help="Số lượng micro-batch chạy kiểm thử (mặc định chạy liên tục)")
    args = parser.parse_args()

    run_simulator(port=args.port, max_batches=args.batches)
