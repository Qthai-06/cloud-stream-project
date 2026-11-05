# Demo Cloud Stream Project

## 1. Project này làm gì?

Project mô phỏng một hệ thống xử lý dữ liệu IoT theo thời gian thực:

```text
Data Generator
      |
      v
Redpanda/Kafka  --->  Spark Structured Streaming  --->  PostgreSQL
      |                         |
      v                         v
Prometheus/cAdvisor       Checkpoint + Window + Watermark
      |
      v
Grafana Dashboard
```

Luồng chính thực hiện các công việc sau:

- Sinh sự kiện cảm biến liên tục.
- Gửi sự kiện vào topic Kafka-compatible `iot-sensor-events`.
- Tạo window một phút bằng Spark.
- Xử lý dữ liệu đến trễ bằng watermark 120 giây.
- Ghi kết quả tổng hợp vào bảng PostgreSQL `sensor_window_metrics`.
- Lưu checkpoint tại `checkpoints/spark_pipeline`.
- Thu thập metrics CPU/RAM, throughput, batch duration và consumer lag.
- Hiển thị metrics bằng Grafana.

## 2. Các thành phần chính

| Thành phần | Vai trò | Cổng trên máy |
|---|---|---:|
| Redpanda | Kafka-compatible broker | `19092` |
| Redpanda Console | Xem topic và message | `18080` |
| PostgreSQL | Lưu kết quả window | `15432` |
| Spark processor | Đọc Kafka, window, watermark, ghi PostgreSQL | Không expose |
| Data generator | Sinh dữ liệu IoT | Không expose |
| cAdvisor | Metrics tài nguyên container | `8080` |
| Prometheus gốc | Metrics hạ tầng cơ bản | `19090` |
| Grafana gốc | Monitoring hạ tầng | `13000` |
| NhutTan Prometheus | Metrics cho dashboard demo | `9090` |
| NhutTan Grafana | Ba dashboard demo dựng sẵn | `3000` |

Compose gốc và bộ monitoring `NhutTan` dùng chung network `stream-network`,
nhưng dùng container name riêng để không xung đột.

## 3. Yêu cầu trước khi chạy

- Windows với Docker Desktop đang chạy.
- Docker Compose v2.
- Python 3.10 trở lên nếu chạy metrics server hoặc script kiểm thử.
- Repository được mở tại:

```powershell
F:\Python\cloud-stream-project
```

## 4. Chuẩn bị biến môi trường

Từ thư mục gốc:

```powershell
Set-Location F:\Python\cloud-stream-project
Copy-Item .env.example .env
```

Mở `.env` và đặt các biến:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=<mat-khau-postgresql>
POSTGRES_DB=app_db

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<mat-khau-grafana>
```

Không commit file `.env`. Nếu PostgreSQL đã chạy trước đó với volume hiện có,
password trong `.env` phải khớp password của database hiện tại.

Kiểm tra cấu hình trước khi khởi động:

```powershell
docker compose config
docker compose -f .\NhutTan\docker-compose.monitoring.yml config
```

## 5. Chạy pipeline chính

### Bước 1: Khởi động Redpanda, PostgreSQL và Spark

```powershell
docker compose up -d --build
```

Lệnh này khởi động toàn bộ Compose gốc, gồm cả monitoring cơ bản.

Kiểm tra trạng thái:

```powershell
docker compose ps -a
```

Các service quan trọng cần ở trạng thái `Up` hoặc `healthy`:

- `redpanda`
- `postgres`
- `event-generator`
- `spark-processor`
- `prometheus`
- `grafana`

### Bước 2: Kiểm tra log generator và Spark

Mở PowerShell riêng cho từng log:

```powershell
docker compose logs -f data-generator
```

```powershell
docker compose logs -f stream-processor
```

Generator phải kết nối tới `redpanda:29092`. Spark phải khởi tạo Kafka source
và không có lỗi kết nối PostgreSQL.

### Bước 3: Kiểm tra topic Redpanda

```powershell
docker exec redpanda rpk topic list
```

Topic cần kiểm tra là:

```text
iot-sensor-events
```

Xem thử message:

```powershell
docker exec redpanda rpk topic consume iot-sensor-events --num 5
```

### Bước 4: Kiểm tra dữ liệu PostgreSQL

```powershell
docker compose exec postgres psql `
  -U $env:POSTGRES_USER `
  -d $env:POSTGRES_DB `
  -c "SELECT COUNT(*) FROM sensor_window_metrics;"
```

Xem các window mới nhất:

```powershell
docker compose exec postgres psql `
  -U $env:POSTGRES_USER `
  -d $env:POSTGRES_DB `
  -c "SELECT * FROM sensor_window_metrics ORDER BY updated_at DESC LIMIT 10;"
```

## 6. Chạy dashboard giống ảnh demo

Dashboard dựng sẵn nằm trong `NhutTan/`. Bộ này đọc các metrics do Prometheus
`NhutTan` thu thập và tự động nạp ba file JSON dashboard.

### Bước 1: Chạy live metrics server

Mở PowerShell riêng tại thư mục gốc:

```powershell
Set-Location F:\Python\cloud-stream-project
python .\NhutTan\src\start_live_metrics_server.py
```

Giữ cửa sổ này chạy. Kiểm tra metrics:

```powershell
Invoke-WebRequest http://localhost:8000/metrics
```

Metrics server này cung cấp metrics demo cho throughput, batch duration,
checkpoint duration, consumer lag và CPU/RAM. Đây là số liệu mô phỏng theo
timeline baseline, ramp-up, stress peak và cool-down.

Metrics server không phải Spark exporter. Khi trình bày kết quả, phải ghi rõ
đây là metrics mô phỏng nếu chưa đối chiếu với log Spark, query progress hoặc
exporter thực tế.

### Bước 2: Khởi động Prometheus và Grafana của `NhutTan`

Mở PowerShell thứ hai:

```powershell
Set-Location F:\Python\cloud-stream-project
docker compose -f .\NhutTan\docker-compose.monitoring.yml up -d
```

Kiểm tra:

```powershell
docker compose -f .\NhutTan\docker-compose.monitoring.yml ps
```

### Bước 3: Mở giao diện

Truy cập:

- Grafana dashboard: http://localhost:3000
- Prometheus targets: http://localhost:9090/targets

Tài khoản Grafana của bộ `NhutTan`:

```text
Username: admin
Password: admin
```

Vào **Dashboards** và chọn một trong ba dashboard:

1. `01 - Hạ Tầng & Giám Sát Tài Nguyên Baseline`
2. `02 - Hiệu Năng Spark Streaming & Checkpoint`
3. `03 - Ép Tải, Backpressure & Điểm Nghẽn Cổ Chai`

Dashboard số 2 là dashboard nên mở để trình bày throughput và checkpoint.
Dashboard số 3 dùng để trình bày consumer lag và backpressure.

## 7. Kiểm tra Prometheus khi dashboard trống

Mở http://localhost:9090/targets. Các target quan trọng cần có trạng thái `UP`:

- `cadvisor:8080`
- `redpanda:9644`
- `host.docker.internal:8000`

Trong Prometheus, thử các truy vấn:

```promql
up
```

```promql
spark_streaming_input_rows_per_second
```

```promql
spark_streaming_processed_rows_per_second
```

```promql
spark_streaming_consumer_lag
```

Nếu Prometheus có kết quả nhưng Grafana trống, kiểm tra datasource Grafana
đang trỏ tới `http://nhuttan-prometheus:9090`, sau đó refresh dashboard.

Nếu target `host.docker.internal:8000` ở trạng thái `DOWN`, kiểm tra cửa sổ
PowerShell đang chạy `start_live_metrics_server.py`, rồi thử:

```powershell
Invoke-WebRequest http://localhost:8000/metrics
```

## 8. Chạy baseline và stress test

Các script sau có thể ghi file kết quả vào `NhutTan/ket_qua/`.

### Baseline

```powershell
python .\NhutTan\src\record_baseline.py
```

### Stress test mô phỏng

```powershell
python .\NhutTan\src\stress_test_backpressure.py --rate 500 --duration 180
```

### Sinh biểu đồ PNG

```powershell
python .\NhutTan\src\generate_bottleneck_chart.py
```

### Chạy kiểm tra tổng hợp

```powershell
python .\NhutTan\run_all_checks.py
```

Các script stress-test và live metrics server trong `NhutTan` phục vụ demo và
minh họa. Không trình bày số liệu mô phỏng như số đo trực tiếp của Spark nếu
chưa đối chiếu với metrics thực tế.

## 9. Demo checkpoint và recovery

Runbook backup/restore và recovery đầy đủ nằm tại
[docs/infrastructure-recovery.md](./infrastructure-recovery.md).

Checkpoint của Spark nằm tại:

```text
.\checkpoints\spark_pipeline
```

Kiểm tra file checkpoint:

```powershell
Get-ChildItem .\checkpoints -Recurse
```

Dừng và khởi động lại riêng Spark, không xóa volume:

```powershell
docker compose stop stream-processor
docker compose start stream-processor
```

Sau đó kiểm tra log:

```powershell
docker compose logs --tail=100 stream-processor
```

Để chạy đầy đủ recovery drill và kiểm tra row count, duplicate, window cùng
checkpoint:

```powershell
.\ops\recovery_drill.ps1
```

Drill đã được kiểm chứng local dưới 5 phút (lần nghiệm thu gần nhất khoảng
`19.5 giây`). Row count có thể tăng trong lúc restart vì generator vẫn chạy;
tiêu chí là row count không giảm, duplicate bằng `0` và window hợp lệ.

Không dùng `docker compose down --volumes` nếu muốn giữ PostgreSQL, Redpanda,
Grafana hoặc checkpoint.

## 10. Các URL cần trình bày

| Nội dung demo | URL |
|---|---|
| Redpanda Console | http://localhost:18080 |
| Grafana hạ tầng gốc | http://localhost:13000 |
| Prometheus hạ tầng gốc | http://localhost:19090 |
| cAdvisor | http://localhost:8080 |
| Grafana dashboard mẫu | http://localhost:3000 |
| Prometheus dashboard mẫu | http://localhost:9090 |

## 11. Dừng project

Dừng monitoring `NhutTan`:

```powershell
docker compose -f .\NhutTan\docker-compose.monitoring.yml down
```

Dừng pipeline chính nhưng giữ lại volume:

```powershell
docker compose down --remove-orphans
```

Không dùng lệnh dưới đây trừ khi chủ động muốn xóa dữ liệu persistent:

```powershell
docker compose down --volumes
```

## 12. Checklist nghiệm thu

- [ ] `docker compose config` chạy không lỗi.
- [ ] Redpanda ở trạng thái healthy.
- [ ] Topic `iot-sensor-events` tồn tại.
- [ ] Generator gửi được message.
- [ ] Spark processor đang `Up`.
- [ ] PostgreSQL có bảng `sensor_window_metrics`.
- [ ] PostgreSQL có dữ liệu window.
- [ ] Checkpoint xuất hiện trong `checkpoints/spark_pipeline`.
- [ ] `http://localhost:9090/targets` có target metrics cần thiết.
- [ ] Grafana `http://localhost:3000` hiển thị ba dashboard.
- [ ] Đã phân biệt số liệu thực tế và metrics mô phỏng.
- [ ] Recovery drill hoàn thành dưới 5 phút và có log bằng chứng.
