# HỆ THỐNG GIÁM SÁT PROMETHEUS, GRAFANA, STREAMING METRICS & ÉP TẢI
## Người thực hiện: **Tân Lưu** (Đề tài 16 — Cloud Stream Project)

Tài liệu này hướng dẫn chi tiết về **Cấu trúc thư mục**, **Cách cài đặt (Setup)** và **Cách chạy (Run)** toàn bộ hệ thống giám sát và bài đo ép tải của **Tân Lưu** (từ Tuần 1 đến Tuần 5) để các thành viên trong nhóm (**Thái Leader**, **Tân Lê**) và giảng viên tiện theo dõi, phối hợp và nghiệm thu.

---

## 1. BẢNG PHÂN CÔNG NHIỆM VỤ & ĐỐI CHIẾU TIÊU CHÍ (DoD TỪ TUẦN 1 ĐẾN TUẦN 5)

| Giai đoạn | Nội dung nhiệm vụ | Tiêu chí hoàn thành (DoD) | Trạng thái | Sản phẩm thực hiện |
|:---:|---|---|:---:|---|
| **Tuần 1-2: Nền móng** | Triển khai Prometheus và Grafana. Kết nối vào hệ thống hạ tầng để bắt đầu đọc metrics. | Dashboard hiển thị được trạng thái CPU/RAM cơ bản. | **ĐẠT (PASS)** ✅ | • `docker-compose.monitoring.yml`<br>• `prometheus/prometheus.yml`<br>• `grafana/provisioning/` |
| **Tuần 3: Giả lập** | Chạy giám sát baseline: Ghi nhận mức tiêu thụ tài nguyên của hệ thống khi chạy ở tải thấp bình thường (10 msg/s). | Biểu đồ baseline trên Grafana hoạt động, có số liệu thô. | **ĐẠT (PASS)** ✅ | • `grafana/dashboards_json/01_...`<br>• `src/record_baseline.py`<br>• `ket_qua/baseline_reference_...` |
| **Tuần 4: Xử lý** | Đưa các chỉ số quan trọng (Throughput, trạng thái Checkpoint) lên dashboard Grafana. | Dashboard hiển thị được tốc độ xử lý thời gian thực. | **ĐẠT (PASS)** ✅ | • `grafana/dashboards_json/02_...`<br>• `src/spark_metrics_listener.py`<br>• `src/streaming/metrics_listener.py` |
| **Tuần 5: Ép tải** | **Tăng vọt tốc độ bơm dữ liệu (Stress test). Theo dõi và bắt khoảnh khắc backpressure và consumer lag tăng cao.** | **Chụp lại được biểu đồ hiển thị điểm nghẽn cổ chai của hệ thống.** | **ĐẠT (PASS)** ✅ | • `grafana/dashboards_json/03_...`<br>• `src/stress_test_backpressure.py`<br>• `src/generate_bottleneck_chart.py`<br>• `ket_qua/bieu_do_ep_tai_backpressure.png`<br>• `ket_qua/bieu_do_tai_nguyen_cpu_ram.png`<br>• `BaoCao_Tuan5_EpTai_TanLuu.md` |

---

## 2. CẤU TRÚC CHI TIẾT THƯ MỤC `NhutTan/`

```text
NhutTan/
├── README.md                                  # [File này] Hướng dẫn cấu trúc, cài đặt và vận hành
├── BaoCao_Tuan5_EpTai_TanLuu.md               # 📄 Báo cáo chuyên sâu Tuần 5 (Ép tải & Điểm nghẽn cổ chai)
├── BaoCao_Tuan1_den_Tuan5_TanLuu.md           # 📄 Báo cáo tổng hợp từ Tuần 1 đến Tuần 5
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
│       ├── 01_container_resources_baseline.json      # Dashboard 1: Giám sát CPU/RAM Containers & Baseline
│       ├── 02_spark_streaming_performance.json       # Dashboard 2: Throughput, Batch Duration & Checkpoint ms
│       └── 03_stress_test_bottleneck_backpressure.json # Dashboard 3: Ép tải, Consumer Lag & Cổ chai Spark CPU
│
├── src/                                       # Mã nguồn các script thực thi (Python)
│   ├── test_monitoring_connection.py          # Script kiểm thử kết nối hạ tầng giám sát (Tuần 1-2)
│   ├── record_baseline.py                     # Script xuất số liệu thô Baseline ra CSV/JSON (Tuần 3)
│   ├── spark_metrics_listener.py              # Thư viện thu thập metrics cho PySpark (Tuần 4)
│   ├── stress_test_backpressure.py            # Kịch bản ép tải, theo dõi Backpressure & Lag (Tuần 5)
│   ├── generate_bottleneck_chart.py           # Sinh hình ảnh biểu đồ điểm nghẽn cổ chai PNG (Tuần 5)
│   └── export_report_docx.py                  # Script tự động tạo file Báo cáo Word (.docx)
│
└── ket_qua/                                   # THƯ MỤC CHỨA TOÀN BỘ KẾT QUẢ ĐÃ CHẠY & BÁO CÁO
    ├── BaoCao_Tuan1_den_Tuan4_TanLuu.docx     # 📄 File Word báo cáo hoàn chỉnh
    ├── baseline_reference_metrics.csv         # 📊 Bảng số liệu thô Baseline (CSV)
    ├── baseline_reference_metrics.json        # 📊 Bảng số liệu thô Baseline (JSON)
    ├── stress_test_metrics.csv                # 📊 Bảng số liệu thô Ép tải 180s (CSV)
    ├── stress_test_metrics.json               # 📊 Bảng số liệu thô Ép tải & Đối chiếu (JSON)
    ├── bieu_do_ep_tai_backpressure.png        # 📈 ẢNH BÀN GIAO: Biểu đồ bắt khoảnh khắc Backpressure
    ├── bieu_do_tai_nguyen_cpu_ram.png         # 📈 ẢNH BÀN GIAO: Biểu đồ điểm nghẽn phần cứng Spark CPU
    ├── ket_qua_tuan1_2_kiem_thu_ha_tang.txt   # 📋 Log terminal kiểm thử hạ tầng Tuần 1-2
    ├── ket_qua_tuan3_baseline_tai_nguyen.txt  # 📋 Log terminal đo tài nguyên Baseline Tuần 3
    ├── ket_qua_tuan4_throughput_checkpoint.txt# 📋 Log terminal tốc độ Throughput & Checkpoint Tuần 4
    └── ket_qua_tuan5_ep_tai_backpressure.txt  # 📋 Log chi tiết thực nghiệm ép tải Tuần 5
```

---

## 3. HƯỚNG DẪN CÀI ĐẶT & THIẾT LẬP (SETUP GUIDE)

### 3.1. Yêu Cầu Tiên Quyết
- **Python**: Khuyến nghị Python 3.10 trở lên.
- **Docker & Docker Compose**: Để chạy cụm container Redpanda, cAdvisor, Prometheus, Grafana.

### 3.2. Cài Đặt Thư Viện Python Cần Thiết
Tại thư mục gốc dự án, kích hoạt môi trường ảo (virtualenv) và cài đặt:
```bash
pip install prometheus-client pyyaml pillow python-docx
```

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
```bash
cd NhutTan
docker compose -f docker-compose.monitoring.yml up -d
```

---

### BƯỚC 2: Kiểm thử kết nối hạ tầng (Tuần 1-2)
```bash
python3 NhutTan/src/test_monitoring_connection.py
```
- Kiểm tra trạng thái các cổng 9090, 3000, 8080.
- Kết quả log lưu tại: `NhutTan/ket_qua/ket_qua_tuan1_2_kiem_thu_ha_tang.txt`.

---

### BƯỚC 3: Ghi nhận mức tiêu thụ tài nguyên Baseline (Tuần 3)
```bash
python3 NhutTan/src/record_baseline.py
```
- Kết quả lưu tại: `NhutTan/ket_qua/baseline_reference_metrics.csv` và `.json`.

---

### BƯỚC 4: Đo Throughput và Checkpoint thời gian thực (Tuần 4)
```bash
python3 NhutTan/src/spark_metrics_listener.py
```
- Phát trực tiếp chỉ số qua port 8000 lên Grafana Dashboard 02.

---

### BƯỚC 5: Thực Hiện Kịch Bản Ép Tải & Bắt Backpressure (Tuần 5)
Chạy script thực nghiệm ép tải (mặc định đỉnh 500 msg/s trong 180s):
```bash
python3 NhutTan/src/stress_test_backpressure.py --rate 500 --duration 180
```
- **Hiện tượng quan sát:**
  - Khoảnh khắc xuất hiện Backpressure tại giây thứ 60 khi Input (500 msg/s) > Processed (200 rows/s).
  - Consumer Lag tích tụ dồn dập từ 0 lên 15,000 tin nhắn.
  - Batch Duration kéo dài lên 3.85s (vượt ngưỡng an toàn 1.0s).
  - Điểm nghẽn cổ chai: Spark CPU chạm 98.7% bão hòa tính toán.
- **Kết quả lưu tại:**
  - `NhutTan/ket_qua/stress_test_metrics.csv` (Dữ liệu chuỗi thời gian 180s).
  - `NhutTan/ket_qua/stress_test_metrics.json`.
  - `NhutTan/ket_qua/ket_qua_tuan5_ep_tai_backpressure.txt`.

---

### BƯỚC 6: Xuất Biểu Đồ Hiển Thị Điểm Nghẽn Cổ Chai PNG (Deliverables Tuần 5)
```bash
python3 NhutTan/src/generate_bottleneck_chart.py
```
- Tự động render 2 hình ảnh đồ thị độ phân giải cao phong cách Grafana Dark Theme:
  1. `NhutTan/ket_qua/bieu_do_ep_tai_backpressure.png`: Biểu đồ Throughput gap, Backpressure callout và Consumer Lag area.
  2. `NhutTan/ket_qua/bieu_do_tai_nguyen_cpu_ram.png`: Đối chiếu mức chiếm dụng CPU/RAM từng container, làm nổi bật cổ chai Spark.

---

### BƯỚC 7: Xem Các Dashboards Trên Grafana
Truy cập: [http://localhost:3000](http://localhost:3000) (admin / admin):
- `01 - Hạ Tầng & Giám Sát Tài Nguyên Baseline`: CPU/RAM/IO Containers.
- `02 - Hiệu Năng Spark Streaming & Checkpoint`: Throughput In/Out & Checkpoint Latency.
- `03 - Ép Tải, Backpressure & Điểm Nghẽn Cổ Chai`: Toàn cảnh kịch bản Stress Test, điểm nghẽn và Consumer Lag.

---

### BƯỚC 8: Kiểm Tra Toàn Diện Bằng 1 Lệnh Duy Nhất (All-In-One Check)
Để nghiệm thu toàn bộ 5 tuần chỉ bằng một lần bấm:
```bash
python3 NhutTan/run_all_checks.py
```
Lệnh sẽ tự động chạy qua cả 7 bước kiểm tra và xuất toàn bộ kết quả, log và báo cáo vào `NhutTan/ket_qua/`.
