# BÁO CÁO TOÀN DIỆN KẾT QUẢ THỰC HIỆN NHIỆM VỤ (TUẦN 1 ĐẾN TUẦN 5)
## Đề tài 16: Hệ Thống Xử Lý Luồng Thời Gian Thực (Cloud Stream Project)
### Sinh viên thực hiện: **Tân Lưu**
**Khoa Công Nghệ Thông Tin — Bộ Môn Điện Toán Đám Mây — Trường Đại học Giao thông vận tải TP.HCM (UTH)**

---

## 1. BẢNG PHÂN CÔNG NHIỆM VỤ & ĐỐI CHIẾU TIÊU CHÍ (DoD TỪ TUẦN 1 ĐẾN TUẦN 5)

| Giai đoạn | Hạng mục công việc (Nhiệm vụ chi tiết) | Người phụ trách | Kết quả bàn giao (Deliverables) | Tiêu chí hoàn thành (DoD) | Trạng thái |
|:---:|---|:---:|---|---|:---:|
| **Tuần 1-2: Nền móng** | Triển khai Prometheus và Grafana. Kết nối vào hệ thống hạ tầng để bắt đầu đọc metrics. | Tân Lưu | Cụm dịch vụ Docker Compose, cấu hình Scrape cAdvisor & Redpanda. | Dashboard hiển thị được trạng thái CPU/RAM cơ bản. | **ĐẠT (PASS)** ✅ |
| **Tuần 3: Giả lập** | Chạy giám sát baseline: Ghi nhận mức tiêu thụ tài nguyên của hệ thống khi chạy ở tải thấp bình thường (10 msg/s). | Tân Lưu | Bộ số liệu thô Baseline (CSV, JSON), log đo đạc tài nguyên. | Biểu đồ baseline trên Grafana hoạt động, có số liệu thô. | **ĐẠT (PASS)** ✅ |
| **Tuần 4: Xử lý** | Đưa các chỉ số quan trọng (Throughput, trạng thái Checkpoint) lên dashboard Grafana. | Tân Lưu | Thư viện Spark Metrics Listener, dashboard thời gian thực. | Dashboard hiển thị được tốc độ xử lý thời gian thực. | **ĐẠT (PASS)** ✅ |
| **Tuần 5: Ép tải** | **Tăng vọt tốc độ bơm dữ liệu (Stress test). Theo dõi và bắt khoảnh khắc backpressure và consumer lag tăng cao.** | **Tân Lưu** | **Script ép tải đa pha, bắt điểm nghẽn cổ chai, biểu đồ đồ thị sắc nét.** | **Chụp lại được biểu đồ hiển thị điểm nghẽn cổ chai của hệ thống.** | **ĐẠT (PASS)** ✅ |

---

## 2. TỔNG QUAN KIẾN TRÚC VÀ CẤU TRÚC THƯ MỤC `NhutTan/`

```text
NhutTan/
├── README.md                                  # Hướng dẫn chi tiết toàn bộ dự án từ Tuần 1 đến Tuần 5
├── BaoCao_Tuan5_EpTai_TanLuu.md               # Báo cáo chuyên sâu Tuần 5 (Ép tải & Điểm nghẽn cổ chai)
├── BaoCao_Tuan1_den_Tuan5_TanLuu.md           # [File này] Báo cáo tổng hợp từ Tuần 1 đến Tuần 5
├── run_all_checks.py                          # Script tự động kiểm tra toàn diện 1-click cho cả 5 tuần
├── docker-compose.monitoring.yml              # Triển khai Prometheus & Grafana độc lập
│
├── prometheus/                                # Cấu hình Prometheus Server
│   └── prometheus.yml                         # Scrape targets: cAdvisor (8080), Redpanda (9644), Spark (8000)
│
├── grafana/                                   # Cấu hình tự động nạp Grafana (Auto-Provisioning)
│   ├── provisioning/
│   │   ├── datasources/prometheus.yml         # Tự động nạp DataSource Prometheus (port 9090)
│   │   └── dashboards/dashboards.yml          # Tự động quét và nạp Dashboards JSON
│   └── dashboards_json/                       # 3 Dashboards hoàn chỉnh theo từng tuần
│       ├── 01_container_resources_baseline.json      # Tuần 1-2 & Tuần 3: CPU, RAM, I/O Containers
│       ├── 02_spark_streaming_performance.json       # Tuần 4: Throughput In/Out & Checkpoint Latency
│       └── 03_stress_test_bottleneck_backpressure.json # Tuần 5: Ép tải, Consumer Lag & Điểm nghẽn CPU
│
├── src/                                       # Mã nguồn các scripts thực thi
│   ├── test_monitoring_connection.py          # Kiểm thử kết nối hạ tầng giám sát (Tuần 1-2)
│   ├── record_baseline.py                     # Trích xuất số liệu Baseline CSV/JSON (Tuần 3)
│   ├── spark_metrics_listener.py              # Exporter & Listener metrics cho PySpark (Tuần 4)
│   ├── stress_test_backpressure.py            # Kịch bản ép tải, theo dõi Backpressure & Lag (Tuần 5)
│   ├── generate_bottleneck_chart.py           # Sinh hình ảnh biểu đồ điểm nghẽn cổ chai (Tuần 5)
│   └── export_report_docx.py                  # Script xuất báo cáo Word .docx
│
└── ket_qua/                                   # Toàn bộ sản phẩm bàn giao, dữ liệu thô & biểu đồ
    ├── BaoCao_Tuan1_den_Tuan4_TanLuu.docx     # Báo cáo Word Tuần 1-4
    ├── baseline_reference_metrics.csv         # Bảng số liệu thô Baseline (CSV)
    ├── baseline_reference_metrics.json        # Bảng số liệu thô Baseline (JSON)
    ├── stress_test_metrics.csv                # Bảng số liệu thô Ép tải 180s (CSV)
    ├── stress_test_metrics.json               # Bảng số liệu thô Ép tải & Đối chiếu (JSON)
    ├── bieu_do_ep_tai_backpressure.png        # 📈 ẢNH BÀN GIAO: Biểu đồ bắt khoảnh khắc Backpressure
    ├── bieu_do_tai_nguyen_cpu_ram.png         # 📈 ẢNH BÀN GIAO: Biểu đồ điểm nghẽn phần cứng Spark CPU
    ├── ket_qua_tuan1_2_kiem_thu_ha_tang.txt   # Log kiểm thử hạ tầng Tuần 1-2
    ├── ket_qua_tuan3_baseline_tai_nguyen.txt  # Log đo đạc tài nguyên Baseline Tuần 3
    ├── ket_qua_tuan4_throughput_checkpoint.txt# Log Throughput & Checkpoint Tuần 4
    └── ket_qua_tuan5_ep_tai_backpressure.txt  # Log chi tiết thực nghiệm ép tải Tuần 5
```

---

## 3. TÓM TẮT KẾT QUẢ CÁC GIAI ĐOẠN ĐÃ THỰC HIỆN

### 3.1. Tuần 1-2: Nền Móng Hạ Tầng Giám Sát
- **Mục tiêu:** Dựng cụm Prometheus và Grafana, thu thập metrics CPU/RAM cơ bản của container từ cAdvisor và Redpanda.
- **Thực hiện:**
  - File `docker-compose.monitoring.yml` kết nối vào mạng `stream-network` của đề tài.
  - Cấu hình Prometheus tự động scrape dữ liệu định kỳ mỗi 2s – 3s.
  - Cấu hình Auto-Provisioning cho Grafana, nạp sẵn DataSource và Dashboard không cần thao tác tay.
  - Script `test_monitoring_connection.py` kiểm tra kết nối tới các cổng 9090, 3000, 8080.
- **Kết quả:** Dashboard `01_container_resources_baseline.json` hiển thị đầy đủ và ổn định chỉ số CPU, RAM, Network I/O và Disk Write I/O.

### 3.2. Tuần 3: Giả Lập & Đo Lường Tài Nguyên Baseline
- **Mục tiêu:** Đo đạc mức tiêu thụ tài nguyên của hệ thống ở mức tải thấp bình thường (`10 msg/s`, `20% late data`) làm thước đo chuẩn (Ground truth).
- **Thực hiện:**
  - Script `record_baseline.py` truy vấn Prometheus API và lưu trữ số liệu thô vào `baseline_reference_metrics.csv` và `.json`.
  - Kết quả đo đạc: Toàn bộ stack chỉ tiêu thụ ~58.7% của 1 core CPU. Spark Processor chiếm 28.6% CPU, Redpanda chiếm 14.5% CPU. RAM của tất cả container nằm an toàn dưới ngưỡng hạn mức.
- **Kết quả:** Biểu đồ Baseline trên Grafana hoạt động tốt, có số liệu thô làm căn cứ đối chiếu.

### 3.3. Tuần 4: Đo Lường Throughput & Trạng Thái Checkpoint
- **Mục tiêu:** Đưa các chỉ số Throughput thời gian thực và thời gian ghi Checkpoint lên Grafana.
- **Thực hiện:**
  - Phát triển module `spark_metrics_listener.py` tích hợp trực tiếp vào `src/streaming/metrics_listener.py`.
  - Mở HTTP Server port 8000 cung cấp các chỉ số chuẩn Prometheus: `spark_streaming_input_rows_per_second`, `spark_streaming_processed_rows_per_second`, `spark_streaming_batch_duration_seconds`, `spark_streaming_checkpoint_duration_seconds`.
  - Dashboard `02_spark_streaming_performance.json` hiển thị mượt mà đồ thị Throughput và Checkpoint Latency (~30ms – 60ms).
- **Kết quả:** Dashboard phản ánh chính xác tốc độ xử lý luồng theo thời gian thực.

---

## 4. KẾT QUẢ ĐỘT PHÁ TUẦN 5: ÉP TẢI & BẮT KHOẢNH KHẮC BACKPRESSURE

### 4.1. Kịch Bản Ép Tải Đa Pha (4 Giai Đoạn Trong 180s)
Kịch bản ép tải được triển khai tự động qua script [`stress_test_backpressure.py`](file:///home/luunhuttan/cloud-stream-project/NhutTan/src/stress_test_backpressure.py):
1. **0s – 30s (Baseline):** Phát 10 msg/s, Spark xử lý 10 rows/s, Consumer Lag = 0, Batch duration = 185ms, CPU Spark = 28.6%.
2. **30s – 60s (Ramp-Up):** Tốc độ phát tăng dần từ 10 lên 100 msg/s. Spark bám kịp tải, CPU Spark tăng lên 65.0%.
3. **60s – 110s (Stress Test Peak):** **Bơm dồn dập 500 msg/s (Gấp 50 lần baseline).**
   - **Khoảnh khắc Backpressure xuất hiện:** Input Rate (500 msg/s) vượt trội so với năng lực xử lý tối đa của Spark (chạm trần tại **~200 rows/s**).
   - **Consumer Lag bùng nổ:** Lượng tin nhắn dồn ứ trên broker Redpanda tăng vọt từ 0 lên đỉnh điểm **`15,000 messages`**.
   - **Batch Duration quá tải:** Kéo dài lên **`3.85 giây`** (vượt xa chu kỳ trigger 1.0s), micro-batches bị tích tụ thành hàng đợi dài.
   - **Bão hòa CPU:** Spark CPU đạt **`98.7%`**, khẳng định đây chính là **ĐIỂM NGHẼN CỔ CHAI (BOTTLENECK)** lớn nhất của hệ thống.
4. **110s – 180s (Cool-Down & Drain):** Giảm tải về 10 msg/s. Spark duy trì tốc độ cao giải tỏa toàn bộ 15,000 tin nhắn tồn đọng về 0 tại giây thứ 160. Batch duration hồi phục về 185ms.

### 4.2. Bảng Đối Chiếu Số Liệu Toàn Diện (Baseline vs Stress Test)

| Thành phần | Tiêu chí đánh giá | Baseline (10 msg/s) | Stress Test (500 msg/s) | Mức độ thay đổi | Đánh giá kỹ thuật |
|---|---|:---:|:---:|:---:|---|
| **Pipeline Throughput** | Input Rate | 10 msg/s | **500 msg/s** | **Tăng 50 lần** | Tải ép cực đại vào broker |
| | Processed Rate | 10 rows/s | **200 rows/s** | Tăng 20 lần | Đạt trần công suất tính toán |
| **Broker Buffer** | Consumer Lag | 0 msgs | **15,000 msgs** | **+15,000 msgs** | Redpanda đệm an toàn, không rớt gói |
| **Spark Latency** | Batch Duration | 0.185 s | **3.850 s** | **Tăng 20.8 lần** | Vượt ngưỡng an toàn trigger 1.0s |
| | Checkpoint Duration | 0.042 s | **0.095 s** | Tăng 2.2 lần | Cam kết an toàn state xuống đĩa |
| | End-to-End Latency | ~0.25 s | **~76.5 s** | Tăng 306 lần | Trễ do thời gian chờ trong hàng đợi |
| **`spark-processor`** | CPU Rate (%) | 28.6% | **98.7%** | **+70.1%** | **ĐIỂM NGHẼN CỔ CHAI CHÍNH** |
| | RAM Working Set | 650.2 MB | **1,485.0 MB** | +834.8 MB | Tăng do lưu trữ đệm state & batch |
| **`redpanda`** | CPU Rate (%) | 14.5% | **42.8%** | +28.3% | Chịu tải tốt, IOPS ổn định |
| | RAM Working Set | 420.8 MB | **712.5 MB** | +291.7 MB | Đệm an toàn trong giới hạn 1.5GB |
| **`postgres`** | CPU Rate (%) | 2.1% | **12.8%** | +10.7% | Hoạt động tốt |

---

## 5. HÌNH ẢNH MINH CHỨNG NGHIỆM THU (DELIVERABLES)

Theo yêu cầu: *"Chụp lại được biểu đồ hiển thị điểm nghẽn cổ chai của hệ thống."*

### 5.1. Biểu Đồ 1: Bắt Khoảnh Khắc Backpressure & Dồn Ứ Consumer Lag
![Biểu đồ Backpressure & Consumer Lag](/home/luunhuttan/cloud-stream-project/NhutTan/ket_qua/bieu_do_ep_tai_backpressure.png)

### 5.2. Biểu Đồ 2: Điểm Nghẽn Phần Cứng — CPU & RAM Container
![Biểu đồ Tiêu thụ Tài nguyên CPU RAM](/home/luunhuttan/cloud-stream-project/NhutTan/ket_qua/bieu_do_tai_nguyen_cpu_ram.png)

---

## 6. SỰ PHỐI HỢP TRONG NHÓM & GIẢI PHÁP TỐI ƯU HÓA

### 6.1. Đánh Giá Phối Hợp Thành Viên
- **Thái (Leader):** Việc tăng số lượng phân vùng (partition) của Redpanda topic `iot-sensor-events` đã giúp broker dàn trải tải I/O đồng đều, ngăn chặn broker bị nghẽn trước Spark.
- **Tân Lê:** Tinh chỉnh watermark 120s và tách bộ định tuyến late data độc lập giúp Spark State Memory không bị phình to vô hạn trong lúc ép tải, tránh được hiện tượng sập container do tràn bộ nhớ (Out Of Memory).

### 6.2. Đề Xuất Giải Pháp Nâng Cao Hiệu Năng
1. **Mở rộng ngang Spark Worker (Horizontal Scaling):** Thêm 2 Spark Worker để nâng trần xử lý từ 200 rows/s lên 600 rows/s.
2. **Kích hoạt Backpressure nội tại:** Cấu hình `spark.streaming.backpressure.enabled = true`.
3. **Chuyển đổi RocksDB StateBackend:** Tối ưu hóa việc ghi đĩa và giảm áp lực Garbage Collection trên JVM.

---

## 7. HƯỚNG DẪN KIỂM TRA & TÁI HIỆN KẾT QUẢ (1 LỆNH DUY NHẤT)

Tại thư mục gốc dự án, thực hiện:
```bash
python3 NhutTan/run_all_checks.py
```
Script sẽ tự động chạy toàn bộ các bài kiểm tra từ Tuần 1 đến Tuần 5, sinh số liệu và báo cáo.
Mọi dữ liệu kiểm chứng đều sẵn sàng tại thư mục [`NhutTan/ket_qua/`](file:///home/luunhuttan/cloud-stream-project/NhutTan/ket_qua/).

