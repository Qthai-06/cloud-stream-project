# HỆ THỐNG GIÁM SÁT PROMETHEUS, GRAFANA & STREAMING METRICS
## Người thực hiện: **Tân Lưu** (Đề tài 16 — Cloud Stream Project)

Tài liệu này hướng dẫn chi tiết về **Cấu trúc thư mục**, **Cách cài đặt (Setup)** và **Cách chạy (Run)** toàn bộ hệ thống giám sát của **Tân Lưu** (từ Tuần 1 đến Tuần 4) để các thành viên trong nhóm (**Thái Leader**, **Tân Lê**) và giảng viên tiện theo dõi, phối hợp và nghiệm thu.

---

## 1. BẢNG PHÂN CÔNG NHIỆM VỤ & ĐỐI CHIẾU TIÊU CHÍ (DoD)

| Giai đoạn | Nội dung nhiệm vụ | Tiêu chí hoàn thành (DoD) | Trạng thái | Sản phẩm thực hiện |
|:---:|---|---|:---:|---|
| **Tuần 1-2: Nền móng** | Triển khai Prometheus và Grafana. Kết nối vào hệ thống hạ tầng để bắt đầu đọc metrics. | Dashboard hiển thị được trạng thái CPU/RAM cơ bản. | **ĐẠT (PASS)** | • `docker-compose.monitoring.yml`<br>• `prometheus/prometheus.yml`<br>• `grafana/provisioning/` |
| **Tuần 3: Giả lập** | Chạy giám sát baseline: Ghi nhận mức tiêu thụ tài nguyên của hệ thống khi chạy ở tải thấp bình thường (10 msg/s). | Biểu đồ baseline trên Grafana hoạt động, có số liệu thô. | **ĐẠT (PASS)** | • `grafana/dashboards_json/01_...`<br>• `src/record_baseline.py`<br>• `ket_qua/baseline_reference_...` |
| **Tuần 4: Xử lý** | Đưa các chỉ số quan trọng (Throughput, trạng thái Checkpoint) lên dashboard Grafana. | Dashboard hiển thị được tốc độ xử lý thời gian thực. | **ĐẠT (PASS)** | • `grafana/dashboards_json/02_...`<br>• `src/spark_metrics_listener.py`<br>• `src/streaming/metrics_listener.py` |

---

## 2. CẤU TRÚC CHI TIẾT THƯ MỤC `NhutTan/`

```text
NhutTan/
├── README.md                                  # [File này] Hướng dẫn cấu trúc, cài đặt và vận hành
├── run_all_checks.py                          # Script kiểm tra toàn diện hệ thống bằng 1 lệnh duy nhất
├── docker-compose.monitoring.yml              # File triển khai dịch vụ Prometheus & Grafana
│
├── prometheus/                                # Cấu hình Prometheus Server
│   └── prometheus.yml                         # Cấu hình Scrape: cAdvisor (8080), Redpanda (9644), Spark (8000)
│
├── grafana/                                   # Cấu hình trực quan hóa Grafana
│   ├── provisioning/                          # Cơ chế tự động nạp cấu hình (Auto-provisioning)
│   │   ├── datasources/
│   │   │   └── prometheus.yml                 # Tự động kết nối DataSource Prometheus (http://prometheus:9090)
│   │   └── dashboards/
│   │       └── dashboards.yml                 # Tự động nạp các file Dashboards JSON vào Grafana
│   └── dashboards_json/                       # Định nghĩa các Dashboard JSON dựng sẵn
│       ├── 01_container_resources_baseline.json # Dashboard 1: Giám sát CPU/RAM Containers & Baseline
│       └── 02_spark_streaming_performance.json  # Dashboard 2: Throughput, Batch Duration & Checkpoint ms
│
├── src/                                       # Mã nguồn các script thực thi (Python)
│   ├── test_monitoring_connection.py          # Script kiểm thử kết nối hạ tầng giám sát (Tuần 1-2)
│   ├── record_baseline.py                     # Script xuất số liệu thô Baseline ra CSV/JSON (Tuần 3)
│   ├── spark_metrics_listener.py              # Thư viện thu thập metrics cho PySpark (Tuần 4)
│   └── export_report_docx.py                  # Script tự động tạo file Báo cáo Word (.docx)
│
└── ket_qua/                                   # THƯ MỤC CHỨA TOÀN BỘ KẾT QUẢ ĐÃ CHẠY & BÁO CÁO
    ├── BaoCao_Tuan1_den_Tuan4_TanLuu.docx     # 📄 File Word báo cáo hoàn chỉnh nộp bài
    ├── baseline_reference_metrics.csv         # 📊 Bảng số liệu thô Baseline (CSV)
    ├── baseline_reference_metrics.json        # 📊 Bảng số liệu thô Baseline (JSON)
    ├── ket_qua_tuan1_2_kiem_thu_ha_tang.txt   # 📋 Log terminal kiểm thử hạ tầng Tuần 1-2
    ├── ket_qua_tuan3_baseline_tai_nguyen.txt  # 📋 Log terminal đo tài nguyên Baseline Tuần 3
    └── ket_qua_tuan4_throughput_checkpoint.txt# 📋 Log terminal tốc độ Throughput & Checkpoint Tuần 4
```

---

## 3. HƯỚNG DẪN CÀI ĐẶT & THIẾT LẬP (SETUP GUIDE)

### 3.1. Yêu Cầu Tiên Quyết
- **Python**: Khuyến nghị Python 3.10 trở lên.
- **Docker & Docker Compose**: Để chạy cụm container Redpanda, cAdvisor, Prometheus, Grafana.

### 3.2. Cài Đặt Thư Viện Python Cần Thiết
Tại thư mục gốc dự án, kích hoạt môi trường ảo (virtualenv) và cài đặt:
```bash
pip install prometheus-client python-docx pyyaml
```
*(Nếu đã cài `requirements.txt` của dự án thì các thư viện này đã có sẵn).*

### 3.3. Sơ Đồ Cổng Dịch Vụ Của Cụm Giám Sát

| Dịch vụ | Cổng Host | Cổng Container | Giao thức / Mục đích |
|---|:---:|:---:|---|
| **Prometheus** | `9090` | `9090` | Web UI & API truy vấn metrics: `http://localhost:9090` |
| **Grafana** | `3000` | `3000` | Web UI Dashboard: `http://localhost:3000` (User/Pass: `admin`/`admin`) |
| **cAdvisor** | `8080` | `8080` | Thu thập metrics CPU/RAM container (đã có trong Compose gốc) |
| **Redpanda Metrics** | `9644` | `9644` | Metrics broker Kafka: `http://localhost:9644/public_metrics` |
| **Spark Metrics** | `8000` | `8000` | HTTP Server phát chỉ số Throughput & Checkpoint của Spark |

---

## 4. HƯỚNG DẪN CHẠY TỪNG BƯỚC (RUN GUIDE)

### BƯỚC 1: Khởi động Hạ tầng Giám sát Prometheus & Grafana
Di chuyển vào thư mục `NhutTan/` và bật container:
```bash
cd NhutTan
docker compose -f docker-compose.monitoring.yml up -d
```
> **Lưu ý:** Container Prometheus và Grafana sẽ tự động kết nối vào mạng `stream-network` của dự án để đọc trực tiếp dữ liệu từ `cadvisor:8080` và `redpanda:9644`.

---

### BƯỚC 2: Kiểm thử kết nối hạ tầng (Tuần 1-2)
Chạy script kiểm tra để xác nhận Prometheus và Grafana đã sẵn sàng:
```bash
python3 NhutTan/src/test_monitoring_connection.py
```
- **Kết quả:** Hiển thị trạng thái kết nối các cổng 9090, 3000, 8080 và kiểm tra DoD Tuần 1-2 [PASS].
- File log kết quả tự động lưu tại: `NhutTan/ket_qua/ket_qua_tuan1_2_kiem_thu_ha_tang.txt`.

---

### BƯỚC 3: Ghi nhận mức tiêu thụ tài nguyên Baseline (Tuần 3)
Chạy script đo đạc tài nguyên hệ thống ở mức tải thấp (10 msg/s):
```bash
python3 NhutTan/src/record_baseline.py
```
- **Kết quả:** In bảng thống kê CPU %, RAM MB của từng container (`redpanda`, `generator`, `spark`, `cadvisor`, `postgres`, `clickhouse`).
- Dữ liệu thô tự động lưu thành file CSV và JSON tại:
  - `NhutTan/ket_qua/baseline_reference_metrics.csv`
  - `NhutTan/ket_qua/baseline_reference_metrics.json`
- File log kết quả terminal lưu tại: `NhutTan/ket_qua/ket_qua_tuan3_baseline_tai_nguyen.txt`.

---

### BƯỚC 4: Đo Throughput và Checkpoint thời gian thực (Tuần 4)

#### Cách 4A: Chạy bộ giả lập kiểm thử thời gian thực nhanh (độc lập)
Nếu chưa chạy cụm Spark thật, bạn có thể chạy bộ mô phỏng để thấy biểu đồ nhảy ngay trên Grafana:
```bash
# Chạy thử 5 batch rồi dừng:
python3 NhutTan/src/spark_metrics_listener.py --batches 5

# Hoặc chạy liên tục (nhấn Ctrl+C để dừng):
python3 NhutTan/src/spark_metrics_listener.py
```
- **Kết quả:** In ra từng batch xử lý: số rows, duration ms, checkpoint commit ms, throughput rows/s, số late events.
- File log kết quả terminal lưu tại: `NhutTan/ket_qua/ket_qua_tuan4_throughput_checkpoint.txt`.

#### Cách 4B: Tích hợp vào job Spark Structured Streaming thật của nhóm
Trong code Spark (`src/streaming/spark_pipeline.py`), metrics listener đã được nhúng sẵn thông qua `src/streaming/metrics_listener.py`. Bạn chỉ cần thêm 1 dòng hook trong `write_batch`:
```python
from src.streaming.metrics_listener import MetricsCollector

# Khởi tạo collector lắng nghe tại cổng 8000
metrics_collector = MetricsCollector(query_name="iot_windowed_query", port=8000)

def write_batch(batch_df, batch_id, database):
    # ... logic ghi database cũ ...
    # Ghi nhận chỉ số batch vào Prometheus:
    metrics_collector.record_batch(
        batch_id=batch_id,
        num_rows=len(rows),
        duration_seconds=batch_duration,
        checkpoint_seconds=0.045
    )
```

---

### BƯỚC 5: Xem Dashboards trên Grafana
Mở trình duyệt web:
- **Địa chỉ:** [http://localhost:3000](http://localhost:3000)
- **Đăng nhập:** Tài khoản: `admin` / Mật khẩu: `admin` (hoặc bấm xem trực tiếp không cần đăng nhập).
- Vào menu **Dashboards** > Thư mục **Cloud Stream Metrics (Tan Luu)** để xem 2 Dashboards:
  1. `01 - Hạ Tầng & Giám Sát Tài Nguyên Baseline (Tân Lưu)`: Xem CPU %, RAM Working Set, I/O của container.
  2. `02 - Hiệu Năng Spark Streaming & Checkpoint (Tân Lưu)`: Xem Throughput đầu vào vs đầu ra, Checkpoint latency ms, Late events rate.

---

### BƯỚC 6: Xuất lại Báo Cáo Word (.docx)
Khi có số liệu mới, chỉ cần chạy lệnh sau để tự động tạo/cập nhật file Word:
```bash
python3 NhutTan/src/export_report_docx.py
```
- File Word xuất ra tại: [`NhutTan/ket_qua/BaoCao_Tuan1_den_Tuan4_TanLuu.docx`](./ket_qua/BaoCao_Tuan1_den_Tuan4_TanLuu.docx).

---

### BƯỚC 7: Kiểm Tra Toàn Diện Bằng 1 Lệnh Duy Nhất (All-In-One Check)
Để nghiệm thu toàn bộ các bước trên chỉ bằng một lần bấm, chạy:
```bash
python3 NhutTan/run_all_checks.py
```
Lệnh này sẽ tự động:
1. Kiểm tra sự tồn tại của tất cả file cấu hình & mã nguồn.
2. Kiểm tra tính hợp lệ JSON của 2 Dashboards Grafana.
3. Chạy script Baseline Tuần 3 và cập nhật CSV/JSON.
4. Chạy script Metrics Listener Tuần 4 và kiểm tra cổng 8000.
5. Tự động biên dịch lại file báo cáo Word `.docx`.
