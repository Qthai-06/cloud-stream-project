# HƯỚNG DẪN CÁC BƯỚC KHỞI CHẠY VÀ XEM DASHBOARD GRAFANA
**Học viên:** Tân Lưu (MSSV: 082206000224)  
**Vai trò:** Observability & Testing (Prometheus, Grafana, Stress Test, Bottleneck Analysis)  
**Đề tài 16:** Cloud Stream Project — Hệ thống xử lý luồng dữ liệu IoT thời gian thực

---

## 📌 MỤC LỤC
0. [⭐ CÁCH 0: KHỞI CHẠY 1-CLICK TẤT CẢ TRONG 1 (KHUYÊN DÙNG CHO CÁC BẠN TRONG NHÓM)](#0--cách-0-khởi-chạy-1-click-tất-cả-trong-1-khuyên-dùng)
1. [Tổng quan hệ thống Dashboard](#1-tổng-quan-hệ-thống-dashboard)
2. [Cách 1: Khởi chạy và Xem trực tiếp trên Web UI Grafana (Live Monitoring)](#2-cách-1-khởi-chạy-và-xem-trực-tiếp-trên-web-ui-grafana)
3. [Cách 2: Tự động Render toàn bộ 5 Ảnh Dashboard PNG độ nét cao (Nhanh nhất)](#3-cách-2-tự-động-render-toàn-bộ-5-ảnh-dashboard-png)
4. [Cách 3: Tự động Xuất Báo cáo Word (.docx) chứa sẵn Toàn bộ Dashboard](#4-cách-3-tự-động-xuất-báo-cáo-word-docx)
5. [Cách 4: Chạy kiểm tra tự động toàn diện (All-in-One Verification)](#5-cách-4-chạy-kiểm-tra-tự-động-toàn-diện)
6. [Bảng Tra cứu Cổng Mạng (Port Mapping) & Tài khoản](#6-bảng-tra-cứu-cổng-mạng-port-mapping)
7. [Xử lý sự cố thường gặp (Troubleshooting)](#7-xử-lý-sự-cố-thường-gặp)

---

## 0. ⭐ CÁCH 0: KHỞI CHẠY 1-CLICK TẤT CẢ TRONG 1 (KHUYÊN DÙNG)

> **Dành cho các bạn trong nhóm:** Bạn không cần phải nhớ nhiều lệnh phức tạp, chỉ cần chạy duy nhất **1 file/1 lệnh** dưới đây:

### 🔹 Cách chạy:
* **Trên macOS / Linux:**
  ```bash
  python3 NhutTan/start_dashboard.py
  ```
  *(Hoặc chạy script: `./NhutTan/start_dashboard.sh`)*

* **Trên Windows:**
  ```cmd
  python NhutTan\start_dashboard.py
  ```
  *(Hoặc nhấp đúp chuột vào file `NhutTan\start_dashboard.bat`)*

### 🔹 File này tự động làm mọi thứ:
1. Tự động kiểm tra và cài thư viện `pillow`, `python-docx` nếu máy chưa có.
2. Tự khởi động toàn bộ cụm Dữ liệu thật (Redpanda, Postgres, cAdvisor, Spark, Event Generator, Prometheus, Grafana).
3. Tự động kiểm tra 4 luồng dữ liệu thật:
   - **Việc 1:** CPU/RAM từng container qua cAdvisor -> Prometheus (Job `cadvisor` UP).
   - **Việc 2:** Throughput vào và Consumer Lag qua Redpanda (`19644/public_metrics`).
   - **Việc 3:** Tốc độ xử lý (processed rate) & batch duration từ Spark qua cổng 8000 (Job `spark` UP).
   - **Việc 4:** Độ trễ P95 thực tế tính từ PostgreSQL (`sensor_window_metrics`).
4. Tự render toàn bộ **5 ảnh Dashboard PNG 1400px** vào `NhutTan/ket_qua/`.
5. Tự xuất cả **2 file Word báo cáo chuẩn mẫu** (`NoiDung_TanLuu.docx` & `Code_TanLuu.docx`).
6. Tự động **mở trình duyệt web vào thẳng Grafana** [http://localhost:13000](http://localhost:13000) (hoặc [http://localhost:3000](http://localhost:3000)).

---

---

## 1. TỔNG QUAN HỆ THỐNG DASHBOARD

Hệ thống giám sát của Tân Lưu gồm **3 Dashboards JSON chính thức** được cấu hình sẵn trong Grafana (Auto-Provisioning) và **5 biểu đồ trực quan hóa dữ liệu đo lường**:

| STT | Dashboard / Biểu đồ | Chức năng chính | Metrics thu thập |
| :---: | :--- | :--- | :--- |
| **01** | `01_container_resources_baseline.json`<br>*(dashboard_01_baseline_resources.png)* | Giám sát tài nguyên phần cứng toàn bộ 8 container ở mức tải chuẩn 10 msg/s | `container_cpu_usage_seconds_total`<br>`container_memory_working_set_bytes`<br>`container_network_receive_bytes_total` |
| **02** | `02_spark_streaming_performance.json`<br>*(dashboard_02_spark_performance.png)* | Giám sát hiệu năng xử lý luồng Spark Structured Streaming & Checkpoint | `spark_streaming_input_rate`<br>`spark_streaming_processed_rate`<br>`spark_streaming_batch_duration_seconds`<br>`spark_streaming_checkpoint_duration_seconds` |
| **03** | `03_stress_test_bottleneck_backpressure.json`<br>*(bieu_do_ep_tai_backpressure.png)* | Bắt khoảnh khắc Backpressure, Consumer Lag dồn ứ 15,000 tin nhắn khi ép tải 500 msg/s | `kafka_consumer_group_lag`<br>`spark_streaming_is_backpressure`<br>`input_rate vs processed_rate gap` |
| **04** | Biến thiên Tài nguyên CPU/RAM<br>*(bieu_do_tai_nguyen_cpu_ram.png)* | So sánh tài nguyên trước và sau khi ép tải, chỉ ra Spark CPU 98.7% bão hòa | Biến thiên CPU (%) và RAM (MB) của 6 container chủ chốt |
| **05** | Đường cong Latency vs Throughput<br>*(bieu_do_latency_vs_throughput.png)* | Xác định Điểm gãy công suất (Knee Point: 200 msg/s) và độ trễ đuôi P50/P95/P99 | Phân phối trễ E2E Latency theo định luật Little và lý thuyết hàng đợi |

---

## 2. CÁCH 1: KHỞI CHẠY VÀ XEM TRỰC TIẾP TRÊN WEB UI GRAFANA

Dùng cách này khi bạn muốn bật giao diện web Grafana thực tế, thao tác zoom in/out, đổi mốc thời gian hoặc trình chiếu trực tiếp cho giảng viên xem.

### Bước 1: Khởi động hệ thống Docker Compose
Mở Terminal tại thư mục gốc của project (`cloud-stream-project`), chọn một trong hai phương án:

* **Phương án A — Khởi chạy toàn bộ hệ thống (gồm cả Kafka, Spark, Postgres, Generator, Prometheus, Grafana):**
  ```bash
  docker compose up -d
  ```

* **Phương án B — Chỉ khởi chạy riêng cụm Giám sát của Tân Lưu (Prometheus & Grafana nhẹ):**
  ```bash
  docker compose -f NhutTan/docker-compose.monitoring.yml up -d
  ```

### Bước 2: Kiểm tra các container đã chạy thành công
```bash
docker compose ps
```
> Đảm bảo các container `grafana`, `prometheus`, `cadvisor`, `redpanda` ở trạng thái `Up` (hoặc `healthy`).

### Bước 3: Mở Live Metrics Listener cho Spark (Cổng 8000)
Mở một cửa sổ Terminal mới và chạy lệnh:
```bash
python3 NhutTan/src/start_live_metrics_server.py
```
> Server sẽ mở tại `http://localhost:8000/metrics`, phát 4 chỉ số Spark chuẩn định dạng Prometheus để Prometheus kéo dữ liệu về liên tục mỗi 2 giây.

### Bước 4: Mở trình duyệt truy cập Grafana
* **Địa chỉ URL:** `http://localhost:13000` *(hoặc `http://localhost:3000` nếu chạy docker-compose.monitoring.yml)*
* **Tài khoản đăng nhập:**
  * **Username:** `admin`
  * **Password:** `admin` *(hoặc giá trị trong file `.env`)*

### Bước 5: Xem các Dashboard đã nạp sẵn
Hệ thống đã cấu hình **Auto-Provisioning**, do đó ngay khi vào Grafana bạn **không cần tạo hay import gì bằng tay**:
1. Trên menu bên trái của Grafana, click vào **Dashboards**.
2. Nhấp vào thư mục **Cloud Stream Monitoring (Tan Luu)**.
3. Bạn sẽ thấy ngay 3 Dashboard:
   - **01 - Hạ Tầng & Giám Sát Tài Nguyên Baseline (Tân Lưu)**
   - **02 - Hiệu Năng Spark Streaming & Checkpoint (Tân Lưu)**
   - **03 - Ép Tải, Backpressure & Điểm Nghẽn Cổ Chai (Tuần 5 - Tân Lưu)**
4. Nhấp vào từng dashboard để xem các biểu đồ hiển thị thời gian thực!

> 💡 **Mẹo:** Ở góc trên bên phải của Grafana, chọn khoảng thời gian **Last 5 minutes** và chế độ tự làm mới **Auto refresh: 5s** để xem dữ liệu cập nhật động liên tục.

### Bước 6: Chạy kịch bản Ép tải để thấy biểu đồ nhảy số trực tiếp
Để quan sát khoảnh khắc Backpressure bùng nổ, Consumer Lag tăng vọt từ 0 lên 15,000 tin nhắn và Spark CPU chạm đỉnh trên màn hình Grafana:
```bash
python3 NhutTan/src/stress_test_backpressure.py
```
Kịch bản sẽ tự động chạy trong 180 giây:
- **0s - 30s:** Chạy tải chuẩn 10 msg/s (Consumer Lag = 0).
- **30s - 60s:** Tăng dần tải lên 100 msg/s.
- **60s - 110s:** Bơm dồn dập **500 msg/s** (Spark bị nghẽn ở 200 rows/s, Consumer Lag vọt lên 15,000 tin nhắn).
- **110s - 180s:** Hạ tải về 10 msg/s và xả sạch hàng đợi.

---

## 3. CÁCH 2: TỰ ĐỘNG RENDER TOÀN BỘ 5 ẢNH DASHBOARD PNG (NHANH NHẤT)

Dùng cách này khi bạn cần **lấy nhanh hình ảnh biểu đồ sắc nét** để chèn vào báo cáo Word, làm slide thuyết trình, hoặc khi máy tính không bật Docker.

Chạy lệnh duy nhất sau tại Terminal:
```bash
python3 NhutTan/src/generate_all_dashboards.py
```

### Kết quả xuất ra:
Toàn bộ 5 ảnh biểu đồ chuẩn giao diện Grafana Dark Slate (`#181B1F`) độ phân giải cao (1400px) sẽ được lưu tại thư mục `NhutTan/ket_qua/`:
1. `dashboard_01_baseline_resources.png`: Hạ tầng & Tài nguyên Baseline (CPU %, RAM MB, Network/Disk I/O).
2. `dashboard_02_spark_performance.png`: Hiệu năng Spark Streaming & Checkpoint (Throughput In/Out, Batch Duration).
3. `bieu_do_ep_tai_backpressure.png`: Biểu đồ Ép tải Throughput Gap, Backpressure & Consumer Lag 15,000 tin nhắn.
4. `bieu_do_tai_nguyen_cpu_ram.png`: So sánh biến thiên tài nguyên phần cứng, chứng minh Spark CPU 98.7% là điểm nghẽn.
5. `bieu_do_latency_vs_throughput.png`: Đường cong tương quan Latency vs Throughput (Knee Point 200 rows/s và trễ đuôi P99).

---

## 4. CÁCH 3: TỰ ĐỘNG XUẤT BÁO CÁO WORD (.DOCX) CHỨA SẴN TOÀN BỘ DASHBOARD

Nếu bạn muốn tạo lại file Word báo cáo nộp bài hoàn chỉnh, chuẩn 100% định dạng file mẫu của nhóm:
```bash
python3 NhutTan/src/build_exact_format_reports.py
```

Lệnh này sẽ tự động:
- Lắp ráp đầy đủ nội dung các mục **2.4, 3.2, 4.1 và 4.3** theo yêu cầu của Tân Lưu.
- Nhúng trực tiếp **cả 5 biểu đồ Dashboard Grafana độ nét cao** vào đúng vị trí từng mục.
- Chèn khung code định dạng Google Docs và bảng ma trận số liệu đo đạc.
- Xuất ra 2 file báo cáo tại `NhutTan/ket_qua/`:
  - `NoiDung_TanLuu.docx` (và đồng bộ `BaoCao_NoiDung_TanLuu.docx`)
  - `Code_TanLuu.docx` (và đồng bộ `BaoCao_Code_TanLuu.docx`)

---

## 5. CÁCH 4: CHẠY KIỂM TRA TỰ ĐỘNG TOÀN DIỆN (ALL-IN-ONE)

Để kiểm tra xem toàn bộ các thành phần của Tân Lưu (từ Tuần 1 đến Tuần 5) có hoạt động hoàn hảo và đáp ứng tiêu chí nghiệm thu (DoD) hay không:
```bash
python3 NhutTan/run_all_checks.py
```

Script sẽ tự động chạy qua 7 bước:
1. `[PASS]` Kiểm tra sự tồn tại của toàn bộ file mã nguồn và cấu hình.
2. `[PASS]` Kiểm tra tính hợp lệ JSON cú pháp của 3 Dashboard Grafana.
3. `[PASS]` Thu thập và ghi nhận số liệu Baseline (Tuần 3).
4. `[PASS]` Kiểm thử Metrics Exporter & HTTP Server cổng 8000 (Tuần 4).
5. `[PASS]` Mô phỏng kịch bản Ép tải dồn dập & Bắt Backpressure (Tuần 5).
6. `[PASS]` Kết xuất toàn bộ ảnh biểu đồ PNG chất lượng cao.
7. `[PASS]` Đóng gói và cập nhật các file Báo cáo Word và Markdown.

---

## 6. BẢNG TRA CỨU CỔNG MẠNG (PORT MAPPING) & TÀI KHOẢN

| Dịch vụ | Cổng Host (Máy tính của bạn) | Cổng Container nội bộ | Chức năng | URL truy cập |
| :--- | :---: | :---: | :--- | :--- |
| **Grafana UI** | `13000` *(hoặc `3000`)* | `3000` | Giao diện trực quan hóa Dashboard | `http://localhost:13000` |
| **Prometheus UI** | `19090` *(hoặc `9090`)* | `9090` | Thu thập metrics và truy vấn PromQL | `http://localhost:19090` |
| **cAdvisor UI** | `8080` | `8080` | Giám sát tài nguyên cgroups của Docker | `http://localhost:8080` |
| **Redpanda Console**| `18080` | `8080` | Quản lý topic Kafka, Consumer Group & Lag | `http://localhost:18080` |
| **Redpanda Admin** | `19644` | `9644` | Endpoint `/public_metrics` cho Prometheus | `http://localhost:19644/public_metrics` |
| **Spark Metrics** | `8000` | `8000` | Endpoint `/metrics` Spark Streaming | `http://localhost:8000/metrics` |

---

## 7. XỬ LÝ SỰ CỐ THƯỜNG GẶP (TROUBLESHOOTING)

### 1. Grafana báo lỗi "Port is already allocated" (Xung đột cổng)
* **Nguyên nhân:** Cổng `3000` hoặc `9090` đang bị dịch vụ khác trên máy host chiếm dụng.
* **Khắc phục:** File `docker-compose.yml` gốc đã đổi sang cổng an toàn:
  * Grafana: `http://localhost:13000`
  * Prometheus: `http://localhost:19090`

### 2. Dashboard trên Grafana báo "No data"
* **Kiểm tra Prometheus Targets:** Truy cập `http://localhost:19090/targets` (hoặc `http://localhost:9090/targets`).
* Đảm bảo các endpoint `cadvisor:8080`, `redpanda:9644` và `spark-processor:8000` hiển thị trạng thái màu xanh lá (`UP`).
* Nếu `spark-processor:8000` màu đỏ (`DOWN`), hãy chạy script mở listener:
  ```bash
  python3 NhutTan/src/start_live_metrics_server.py
  ```

### 3. Muốn nạp lại (re-import) Dashboard JSON thủ công
Nếu vì lý do nào đó Auto-Provisioning chưa tự nạp:
1. Vào Grafana `http://localhost:13000` -> Click biểu tượng dấu `+` (hoặc Dashboards -> New) -> Chọn **Import**.
2. Nhấn nút **Upload JSON file**.
3. Chọn các file JSON tại thư mục:
   - `NhutTan/grafana/dashboards_json/01_container_resources_baseline.json`
   - `NhutTan/grafana/dashboards_json/02_spark_streaming_performance.json`
   - `NhutTan/grafana/dashboards_json/03_stress_test_bottleneck_backpressure.json`
4. Chọn DataSource là **Prometheus** và nhấn **Import**.

---
*Chúc bạn thực hiện thành công và đạt điểm tối đa cho phần đồ án của mình! 🎉*

