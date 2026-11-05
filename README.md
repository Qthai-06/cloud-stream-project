# Đề tài 16 — Cloud Stream IoT

Hệ thống này mô phỏng một pipeline xử lý dữ liệu IoT theo thời gian thực:

```text
Event Generator -> Redpanda/Kafka -> Spark Structured Streaming -> PostgreSQL
                         |                    |
                         v                    v
                    Prometheus/cAdvisor   Window + Watermark + Checkpoint
                         |
                         v
                      Grafana
```

Mục tiêu của project là quan sát throughput, late data, watermark,
backpressure, consumer lag và khả năng khôi phục sau khi container hoặc tiến
trình bị dừng. PostgreSQL là sink chính; ClickHouse không thuộc stack hiện tại.

## 1. Project đang xử lý gì?

- Generator phát sự kiện cảm biến nhiệt độ/độ ẩm vào topic
  `iot-sensor-events`.
- Tốc độ mặc định là `10 msg/s`, với khoảng `20%` event có timestamp trễ
  `30–120 giây`.
- Spark đọc Kafka, ép kiểu `event_time`, áp dụng window một phút và watermark
  `120 seconds`.
- Kết quả được upsert vào bảng PostgreSQL `sensor_window_metrics`.
- Checkpoint và offset Spark được lưu tại
  `checkpoints/spark_pipeline`.
- cAdvisor và Prometheus gốc thu thập metrics hạ tầng.
- Bộ monitoring trong `NhutTan/` cung cấp ba dashboard dựng sẵn cho baseline,
  Spark performance và stress/backpressure.

`is_late_simulated` chỉ là nhãn để đối chiếu generator. Pipeline không dùng
nhãn này để quyết định late data; quyết định phải dựa trên event time và
watermark.

## 2. Thành phần và cổng

| Thành phần | Vai trò | Cổng host |
|---|---|---:|
| Redpanda | Kafka-compatible broker | `19092` |
| Redpanda Console | Xem topic/message | `18080` |
| PostgreSQL | Sink kết quả window | `15432` |
| Spark processor | Window, watermark, checkpoint, PostgreSQL sink | nội bộ |
| Event generator | Sinh dữ liệu IoT | nội bộ |
| cAdvisor | CPU/RAM/I/O container | `8080` |
| Prometheus gốc | Metrics hạ tầng | `19090` |
| Grafana gốc | Monitoring hạ tầng | `13000` |
| NhutTan Prometheus | Metrics cho dashboard demo | `9090` |
| NhutTan Grafana | Dashboard dựng sẵn | `3000` |

Compose gốc và bộ monitoring `NhutTan` dùng network `stream-network`. Hai bộ
monitoring dùng container name riêng (`prometheus`/`grafana` và
`nhuttan-prometheus`/`nhuttan-grafana`) nên có thể chạy đồng thời.

## 3. Yêu cầu và cấu hình secret

Yêu cầu:

- Docker Desktop đang chạy.
- Docker Compose v2.
- Python 3.10+ nếu chạy live metrics server hoặc script `NhutTan`.
- Docker Desktop có tối thiểu 6 GB RAM; khuyến nghị 8 GB.
- Các cổng ở bảng trên không bị ứng dụng khác chiếm.

Tạo `.env` từ mẫu:

```powershell
Set-Location F:\Python\cloud-stream-project
Copy-Item .env.example .env
```

Đặt password riêng trong `.env`:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=<postgres-password>
POSTGRES_DB=app_db
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<grafana-password>
```

Không commit `.env`, backup database, checkpoint hoặc dữ liệu trong `volumes/`.
Nếu thư mục PostgreSQL đã có dữ liệu, đổi `POSTGRES_PASSWORD` trong `.env`
không tự đổi password trong database; phải dùng password hiện tại hoặc reset
password có kiểm soát.

Kiểm tra cấu hình:

```powershell
docker compose config
docker compose -f .\NhutTan\docker-compose.monitoring.yml config
```

## 4. Chạy pipeline chính

```powershell
docker compose up -d --build
docker compose ps -a
```

Các service cần ở trạng thái `Up` hoặc `healthy`:

```text
redpanda
postgres
event-generator
spark-processor
prometheus
grafana
```

Kiểm tra broker:

```powershell
docker exec redpanda rpk cluster health --api-urls localhost:9644
docker exec redpanda rpk topic list
docker exec redpanda rpk topic consume iot-sensor-events --num 5
```

Kiểm tra log:

```powershell
docker compose logs --tail=100 data-generator
docker compose logs --tail=100 stream-processor
```

Kiểm tra PostgreSQL:

```powershell
docker compose exec postgres psql `
  -U $env:POSTGRES_USER `
  -d $env:POSTGRES_DB `
  -c "SELECT COUNT(*) FROM sensor_window_metrics;"
```

Nếu dùng PowerShell chưa có biến môi trường, truyền trực tiếp:

```powershell
docker exec postgres psql -U admin -d app_db `
  -c "SELECT COUNT(*) FROM sensor_window_metrics;"
```

Kết quả window chỉ xuất hiện sau khi Spark nhận đủ dữ liệu và watermark tiến
qua window. Vì vậy chạy vài giây có thể vẫn thấy `0` dòng.

## 5. Mở giao diện

- Redpanda Console: http://localhost:18080
- Grafana hạ tầng: http://localhost:13000
- Prometheus hạ tầng: http://localhost:19090
- cAdvisor: http://localhost:8080

Grafana gốc dùng `GRAFANA_ADMIN_USER` và `GRAFANA_ADMIN_PASSWORD` trong `.env`.

Để xem dashboard giống ảnh demo, chạy thêm live metrics server và monitoring
`NhutTan`:

```powershell
python .\NhutTan\src\start_live_metrics_server.py
```

PowerShell thứ hai:

```powershell
docker compose -f .\NhutTan\docker-compose.monitoring.yml up -d
```

Mở:

- Grafana dashboard: http://localhost:3000
- Prometheus targets: http://localhost:9090/targets

Tài khoản bộ `NhutTan` là `admin`/`admin` trong cấu hình hiện tại. Ba dashboard
được provision từ `NhutTan/grafana/dashboards_json/`.

**Phân biệt số liệu:** `start_live_metrics_server.py` phát metrics mô phỏng
theo các giai đoạn baseline/ramp-up/stress/cool-down. Không được trình bày
metrics này là số đo trực tiếp của Spark nếu chưa đối chiếu với log và
Prometheus target thực tế.

## 6. Kiểm tra dashboard

Tại http://localhost:9090/targets, các target cần thiết gồm:

- `cadvisor:8080`
- `redpanda:9644`
- `host.docker.internal:8000`

PromQL mẫu:

```promql
up
spark_streaming_input_rows_per_second
spark_streaming_processed_rows_per_second
spark_streaming_consumer_lag
```

Datasource của Grafana `NhutTan` phải trỏ tới:

```text
http://nhuttan-prometheus:9090
```

## 7. Recovery và nghiệm thu Tuần 6

Các script vận hành nằm trong `ops/`:

- `backup_postgres.ps1`: `pg_dump` custom format + SHA-256.
- `restore_postgres.ps1`: restore có cờ xác nhận.
- `verify_recovery.ps1`: row count, checksum, duplicate và window validity.
- `recovery_drill.ps1`: backup, restart PostgreSQL/Spark, kiểm tra checkpoint,
  đo thời gian và ghi transcript.

Chạy recovery drill không xóa volume:

```powershell
.\ops\recovery_drill.ps1
```

Kết quả đạt yêu cầu phải có:

```text
Recovery drill completed in < 300 seconds.
```

Runbook chi tiết: [docs/infrastructure-recovery.md](./docs/infrastructure-recovery.md).

Không dùng lệnh dưới đây trong nghiệm thu nếu chưa muốn xóa dữ liệu:

```powershell
docker compose down --volumes
```

## 8. Thực nghiệm baseline và stress

Các script dashboard:

```powershell
python .\NhutTan\src\record_baseline.py
python .\NhutTan\src\stress_test_backpressure.py --rate 500 --duration 180
python .\NhutTan\src\generate_bottleneck_chart.py
```

Các script này có thể ghi file vào `NhutTan/ket_qua/`. Kết quả phải lưu cùng
commit, cấu hình generator, số partition, window, watermark, thời gian chạy và
trạng thái container.

## 9. Ước tính chi phí

Project chạy local bằng Docker nên không phát sinh chi phí cloud bắt buộc.

| Khoản mục | Ước tính |
|---|---|
| Docker Desktop/local machine | Theo máy người dùng |
| Redpanda/PostgreSQL/Spark/Prometheus/Grafana | 0 phí license trong demo |
| Điện, CPU, RAM và dung lượng đĩa | Phụ thuộc máy local |
| Cloud deployment nếu triển khai sau này | Phải tính riêng VM, disk, network, backup và monitoring |

Đây là ước tính cho môi trường demo, không phải báo giá production.

## 10. Rủi ro bảo mật và vận hành

- Không commit `.env`, password, backup hoặc database volume.
- Đổi password mặc định `admin/admin` trước khi expose Grafana ra mạng.
- Không expose PostgreSQL/Redpanda trực tiếp ra Internet.
- Pin image version khi benchmark; không đổi `latest` giữa các lần đo.
- Giới hạn quyền ghi của thư mục checkpoint/backup.
- Backup phải có checksum và được lưu ngoài máy chạy service nếu cần DR.
- Dashboard `NhutTan` có anonymous viewer trong compose hiện tại; chỉ dùng cho
  mạng demo nội bộ.
- `docker compose down --volumes` có thể xóa dữ liệu persistent.

## 11. Dọn dẹp sau demo

Dừng stack nhưng giữ volume:

```powershell
docker compose -f .\NhutTan\docker-compose.monitoring.yml down
docker compose down --remove-orphans
```

Chỉ xóa dữ liệu sau khi đã lưu kết quả:

- `volumes/`
- `checkpoints/`
- `backups/`
- `NhutTan/ket_qua/`

## 12. Cấu trúc repository

```text
configs/        Cấu hình Kafka, window, watermark và schema PostgreSQL
src/            Generator và Spark Structured Streaming
tests/          Integrity checks và smoke tests
benchmarks/     Kịch bản benchmark
ops/            Backup, restore và recovery drill
docs/           Demo, recovery runbook và tài liệu triển khai
NhutTan/        Monitoring dashboards và stress-test artifacts
checkpoints/    Spark checkpoint bind mount, không commit
volumes/        Dữ liệu runtime bind mount, không commit
```

Tài liệu chạy từng bước: [docs/Demo.md](./docs/Demo.md).

## 13. Trạng thái nghiệm thu

- Pipeline Redpanda -> Generator -> Spark -> PostgreSQL: chạy được sau khi
  topic có leader và PostgreSQL credentials đúng.
- Checkpoint bind mount: đã khai báo và có thể kiểm tra tại
  `checkpoints/spark_pipeline`.
- Monitoring/dashboard `NhutTan`: đã provision ba dashboard.
- Recovery drill: đã kiểm chứng dưới 5 phút trong môi trường local.
- Restore PostgreSQL: có script và đã kiểm thử bằng backup custom format.
- Stress-test: phần dashboard có metrics mô phỏng; cần ghi rõ điều này trong
  báo cáo nếu chưa dùng exporter Spark thực tế.
