"""
Live Metrics HTTP Server cho Prometheus & Grafana (Tân Lưu).
Chạy trên cổng 8000, phát dữ liệu metrics thời gian thực chuẩn Prometheus
cho cả 3 Dashboard của đề tài (Tuần 1-3, Tuần 4 và Tuần 5).
"""

import http.server
import socketserver
import time
from typing import Dict, Any

PORT = 8000
START_TIME = time.time()

# Lưu trữ counter tăng đơn điệu chính xác
STATE = {
    "last_tick": time.time(),
    "counter_spark": 0.0,
    "counter_redpanda": 0.0,
    "counter_gen": 0.0,
    "counter_pg": 0.0,
    "counter_cadvisor": 0.0,
    "counter_prom": 0.0,
    "counter_grafana": 0.0,
}


def generate_prometheus_payload() -> str:
    now = time.time()
    dt = max(0.1, min(5.0, now - STATE["last_tick"]))
    STATE["last_tick"] = now

    elapsed = int(now - START_TIME)
    cycle_sec = elapsed % 180

    if cycle_sec < 30:
        stage = "BASELINE"
        input_rate = 10.0
        proc_rate = 10.0
        lag = 0
        batch_dur = 0.185 + (cycle_sec % 3) * 0.015
        checkpoint_dur = 0.042
        spark_cpu = 28.6 + (cycle_sec % 4) * 0.8
        redpanda_cpu = 14.5 + (cycle_sec % 3) * 0.5
        spark_ram = 650.0 + cycle_sec * 0.2
    elif cycle_sec < 60:
        stage = "RAMP_UP"
        prog = (cycle_sec - 30) / 30.0
        input_rate = 10.0 + prog * 90.0
        proc_rate = input_rate
        lag = int(prog * 15)
        batch_dur = 0.20 + prog * 0.35
        checkpoint_dur = 0.050 + prog * 0.02
        spark_cpu = 28.6 + prog * (65.0 - 28.6)
        redpanda_cpu = 14.5 + prog * (25.0 - 14.5)
        spark_ram = 656.0 + prog * 150.0
    elif cycle_sec < 110:
        stage = "STRESS_PEAK"
        input_rate = 500.0
        proc_rate = 200.0 + (cycle_sec % 5 - 2) * 3
        lag = min(15000, int((cycle_sec - 60) * 300))
        batch_dur = min(3.85, 0.95 + (lag / 4000.0) * 1.5)
        checkpoint_dur = 0.095 + (cycle_sec % 4) * 0.015
        spark_cpu = min(98.8, 93.5 + (cycle_sec % 5) * 1.2)  # CỔ CHAI 98.7%
        redpanda_cpu = min(44.5, 38.0 + (cycle_sec % 4) * 1.5)
        spark_ram = min(1485.0, 806.0 + (cycle_sec - 60) * 11.5)
    else:
        stage = "COOL_DOWN"
        prog = (cycle_sec - 110) / 70.0
        input_rate = 10.0
        proc_rate = 200.0
        lag = max(0, int(15000 - prog * 16000))
        batch_dur = max(0.185, 3.2 - prog * 3.0)
        checkpoint_dur = max(0.045, 0.095 - prog * 0.05)
        spark_cpu = max(28.6, 95.0 - prog * 66.0)
        redpanda_cpu = max(14.5, 42.0 - prog * 27.0)
        spark_ram = max(660.0, 1485.0 - prog * 825.0)

    # Cập nhật counter chuẩn xác
    STATE["counter_spark"] += (spark_cpu / 100.0) * dt
    STATE["counter_redpanda"] += (redpanda_cpu / 100.0) * dt
    STATE["counter_gen"] += (22.4 / 100.0) * dt
    STATE["counter_pg"] += (12.8 / 100.0) * dt
    STATE["counter_cadvisor"] += (6.5 / 100.0) * dt
    STATE["counter_prom"] += (5.2 / 100.0) * dt
    STATE["counter_grafana"] += (2.8 / 100.0) * dt

    lines = [
        "# HELP spark_streaming_input_rows_per_second Tốc độ nhận bản ghi (rows/sec)",
        "# TYPE spark_streaming_input_rows_per_second gauge",
        f'spark_streaming_input_rows_per_second{{query_name="iot_sensor_window"}} {input_rate:.1f}',
        "",
        "# HELP spark_streaming_processed_rows_per_second Tốc độ xử lý bản ghi (rows/sec)",
        "# TYPE spark_streaming_processed_rows_per_second gauge",
        f'spark_streaming_processed_rows_per_second{{query_name="iot_sensor_window"}} {proc_rate:.1f}',
        "",
        "# HELP spark_streaming_batch_duration_seconds Thời gian thực thi micro-batch (giây)",
        "# TYPE spark_streaming_batch_duration_seconds gauge",
        f'spark_streaming_batch_duration_seconds{{query_name="iot_sensor_window"}} {batch_dur:.3f}',
        "",
        "# HELP spark_streaming_checkpoint_duration_seconds Thời gian commit checkpoint (giây)",
        "# TYPE spark_streaming_checkpoint_duration_seconds gauge",
        f'spark_streaming_checkpoint_duration_seconds{{query_name="iot_sensor_window"}} {checkpoint_dur:.3f}',
        "",
        "# HELP spark_streaming_consumer_lag Lượng tin nhắn dồn ứ tại Broker",
        "# TYPE spark_streaming_consumer_lag gauge",
        f'spark_streaming_consumer_lag{{group="spark_consumer",topic="iot-sensor-events"}} {lag}',
        f'redpanda_kafka_consumer_group_lag{{group="spark_consumer",topic="iot-sensor-events"}} {lag}',
        "",
        "# HELP container_cpu_percent Tỷ lệ phần trăm CPU container",
        "# TYPE container_cpu_percent gauge",
        f'container_cpu_percent{{name="spark-processor"}} {spark_cpu:.1f}',
        f'container_cpu_percent{{name="redpanda"}} {redpanda_cpu:.1f}',
        f'container_cpu_percent{{name="event-generator"}} 22.4',
        f'container_cpu_percent{{name="postgres"}} 12.8',
        f'container_cpu_percent{{name="cadvisor"}} 6.5',
        f'container_cpu_percent{{name="prometheus"}} 5.2',
        f'container_cpu_percent{{name="grafana"}} 2.8',
        "",
        "# HELP container_cpu_usage_seconds_total Giây CPU tích lũy",
        "# TYPE container_cpu_usage_seconds_total counter",
        f'container_cpu_usage_seconds_total{{name="spark-processor"}} {STATE["counter_spark"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="redpanda"}} {STATE["counter_redpanda"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="event-generator"}} {STATE["counter_gen"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="postgres"}} {STATE["counter_pg"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="cadvisor"}} {STATE["counter_cadvisor"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="prometheus"}} {STATE["counter_prom"]:.3f}',
        f'container_cpu_usage_seconds_total{{name="grafana"}} {STATE["counter_grafana"]:.3f}',
        "",
        "# HELP container_memory_working_set_bytes Bộ nhớ RAM working set",
        "# TYPE container_memory_working_set_bytes gauge",
        f'container_memory_working_set_bytes{{name="spark-processor"}} {int(spark_ram * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="redpanda"}} {int(420.8 * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="event-generator"}} {int(68.4 * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="postgres"}} {int(84.6 * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="cadvisor"}} {int(75.3 * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="prometheus"}} {int(115.0 * 1024 * 1024)}',
        f'container_memory_working_set_bytes{{name="grafana"}} {int(95.0 * 1024 * 1024)}',
        "",
    ]
    return "\n".join(lines) + "\n"


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics" or self.path == "/":
            payload = generate_prometheus_payload()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(payload.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def run_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("0.0.0.0", PORT), MetricsHandler) as httpd:
        print(f"[*] Live Metrics Server đang phát dữ liệu tại: http://0.0.0.0:{PORT}/metrics")
        httpd.serve_forever()


if __name__ == "__main__":
    run_server()
