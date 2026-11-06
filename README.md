# Đề tài 16 — Cloud Stream IoT

Kho mã nguồn này xây dựng một pipeline xử lý sự kiện IoT (nhiệt độ và độ ẩm)
để **đo lường độ trễ đầu cuối, độ chính xác khi có dữ liệu đến muộn và hành vi
backpressure**. Đây không phải ứng dụng tối ưu cho trường hợp bình thường:
hạ tầng bị giới hạn tài nguyên có chủ đích để tạo ra tải và sự cố có thể quan
sát, lặp lại.

## Câu hỏi nghiên cứu

Window, watermark, checkpoint và cơ chế giao nhận thông điệp ảnh hưởng thế nào
đến độ trễ và độ chính xác của kết quả khi dữ liệu đến muộn?

Kết luận phải được chứng minh bằng số liệu và biểu đồ, tối thiểu gồm:

- throughput và consumer lag;
- độ trễ từ `event_time` đến khi ghi vào PostgreSQL;
- tỷ lệ dữ liệu được gán đúng window, dữ liệu bị loại và dữ liệu được xử lý lại;
- thời gian và trạng thái khôi phục từ checkpoint;
- mức sử dụng CPU, RAM và I/O của các container.

## Data flow

```text
Event Generator (10 msg/s)
        |
        | Kafka protocol, key = device_id
        v
Redpanda / topic: iot-sensor-events
        |
        | Spark Structured Streaming (đang hoàn thiện)
        | window + watermark + checkpoint
        v
PostgreSQL
        |
        +--> Prometheus/Grafana (throughput, lag, checkpoint, latency)
        +--> cAdvisor (CPU, RAM, I/O container)
```

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
| Grafana gốc | Monitoring hạ tầng | `13300` |
| NhutTan Prometheus | Metrics cho dashboard demo | `9090` |
| NhutTan Grafana | Dashboard dựng sẵn | `3000` |
Mọi event được kiểm tra độc lập bằng
[`src/generator/event_schema.py`](./src/generator/event_schema.py) trước khi
được serialize và gửi vào Kafka. Validator kiểm tra các field bắt buộc, kiểu
dữ liệu, timestamp có timezone, miền giá trị nhiệt độ/độ ẩm và trạng thái.
Spark Structured Streaming dùng cùng định nghĩa schema để tránh lệch contract
giữa producer và consumer.

## Thành phần hạ tầng

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

`down` không xóa các volume bind mount trong `volumes/`. Dữ liệu Redpanda và
PostgreSQL vì thế vẫn tồn tại giữa các lần khởi động. Chỉ xóa dữ liệu thí
nghiệm sau khi đã lưu kết quả và xác nhận không cần khôi phục.

### A. Kiểm tra trạng thái

```powershell
docker compose ps
docker compose logs -f data-generator
```

Generator phải hiện tỷ lệ late data thực tế gần 20% theo thời gian đủ dài.
Sai khác ngắn hạn là bình thường vì việc chọn late data dùng xác suất ngẫu
nhiên.

## Sơ đồ cổng dịch vụ

| Dịch vụ | Host | Container | Giao thức / mục đích |
|---|---:|---:|---|
| Redpanda Kafka API | `19092` | `9092` | Kafka client từ máy host |
| Redpanda Console | `18080` | `8080` | Web UI: `http://localhost:18080` |
| PostgreSQL | `15432` | `5432` | SQL sink và integrity checks |
| cAdvisor | `8080` | `8080` | Metrics/UI: `http://localhost:8080` |
| Redpanda Pandaproxy | `8082` | `8082` | HTTP proxy, nếu cần |

Từ một container trên `stream-network`, dùng địa chỉ nội bộ:

```text
redpanda:29092
postgres:5432
```

Không dùng `localhost:19092` từ generator hoặc Spark chạy trong Docker; trong
container, `localhost` trỏ về chính container đó.

## Cấu trúc repository

```text
.
├── benchmarks/                 # Kịch bản benchmark partition và watermark
├── configs/
│   ├── database_schema.sql     # Schema đích (đang hoàn thiện)
│   └── pipeline_config.yaml    # Broker, topic và generator
├── src/
│   ├── generator/              # Kafka producer và mô phỏng late data
│   ├── streaming/              # Spark pipeline và metrics listener
│   ├── batch/                  # Đánh giá kết quả sau thí nghiệm
│   └── utils/                  # Nạp cấu hình, kết nối CSDL
├── tests/                      # Smoke test và kiểm thử hồi quy
├── volumes/                    # Bind mount dữ liệu local của dịch vụ
├── Dockerfile.generator
├── docker-compose.yml
└── requirements.txt
```

### Kiểm thử tích hợp Spark window

Chạy bộ test hồi quy bằng Python trong môi trường dự án:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_smoke_pipeline -v
```

Test `SparkWindowIntegrationTests` khởi tạo Spark local thật, đọc các payload
event JSON qua `EVENT_SCHEMA`, chạy aggregation event-time của pipeline và
kiểm tra window 1 phút, nhóm độc lập theo `device_id`, số lượng event và giá
trị trung bình. Test không kết nối Kafka/PostgreSQL nên có thể tái lập độc lập
với hạ tầng Docker.

Test watermark dùng socket source và memory sink của Spark Structured Streaming
để đẩy từng micro-batch thực, nâng watermark lên trước, sau đó gửi một event có
`event_time` cũ hơn watermark. Kết quả chứng minh event cũ không làm tăng
window đã đóng. Trên Windows, test micro-batch này cần Hadoop native runtime
hoạt động; test cutoff bằng DataFrame vẫn chạy để kiểm tra record cũ bị route.
Test PostgreSQL sẽ chạy khi `POSTGRES_PASSWORD` được cấu hình
và PostgreSQL khả dụng; test thực hiện insert, upsert cùng khóa chính rồi đọc
lại giá trị đã cập nhật.

Router lưu ba lớp bằng chứng: `raw_events.jsonl` cho mọi payload đầu vào,
`accepted_events.jsonl` cho payload hợp lệ không trễ, và
`dead_letter.jsonl` cho payload không parse được. `late_data.jsonl` chỉ chứa
payload hợp lệ nhưng có `event_time` nhỏ hơn watermark Spark thực tế. Bài test
đối chiếu số dòng raw với tổng accepted + late + dead-letter để phát hiện mất
dữ liệu.

`WatermarkProgressTracker` lấy `eventTime.watermark` từ
`StreamingQueryProgress`; router không còn tự tính cutoff từ
`max_event_time` của batch. Nếu Spark chưa báo watermark, router không tự
gắn nhãn late và vẫn lưu raw payload.

Khi toàn bộ Compose đang chạy, có thể bật test xuyên suốt Kafka → Spark →
PostgreSQL bằng:

```powershell
$env:RUN_LIVE_INTEGRATION = "1"
.\.venv\Scripts\python.exe -m unittest tests.test_smoke_pipeline.KafkaSparkPostgreSQLIntegrationTests -v
```

Test này gửi ba event thật vào Kafka, chờ Spark đóng window bằng watermark,
đọc row tương ứng từ PostgreSQL rồi xóa dữ liệu thử nghiệm. Không bật cờ này
thì test được skip để không phụ thuộc hạ tầng đang chạy.

Checkpoint của Spark được mount tại `/opt/workspace/checkpoints` trong
container. Biến `CHECKPOINT_ROOT` được cấu hình cùng giá trị này để
`checkpoint_location: checkpoints/spark_pipeline` được resolve thành
`/opt/workspace/checkpoints/spark_pipeline`, bảo đảm state nằm trên volume
bền vững.

Các module Spark, metrics, database và benchmark hiện còn là khung ban đầu.
README mô tả đích thiết kế; không coi các placeholder này là bằng chứng rằng
chức năng đã hoàn tất.

## Kịch bản thực nghiệm

Mỗi lần chạy phải ghi lại commit, cấu hình generator, số partition, watermark,
kích thước window, thời điểm bắt đầu/kết thúc và trạng thái các container.

### Baseline

1. Khởi chạy Compose với tốc độ mặc định `10 msg/s`.
2. Để hệ thống ổn định trong một khoảng thời gian cố định.
3. Ghi throughput, consumer lag, latency, CPU/RAM và số bản ghi vào
   PostgreSQL.
4. Lưu metrics thô cùng thời điểm đo; không chỉ lưu ảnh dashboard.

### Late data và watermark

Chạy cùng một tập cấu hình với 20% timestamp trễ 30–120 giây, sau đó so sánh
nhiều watermark. Đối chiếu ba nhóm:

- bản ghi đến trước watermark và được tính đúng window;
- bản ghi đến sau watermark, được đưa vào nhánh late-data/log riêng;
- bản ghi bị xử lý lại sau restart.

Mọi bản ghi bị loại phải có lý do, timestamp và định danh sự kiện để truy vết.
Không được dùng một giá trị watermark quá nhỏ chỉ để làm đẹp throughput.

### Stress test và backpressure

Tăng tốc độ generator hoặc số partition theo từng nấc, giữ nguyên thời lượng
đo ở mỗi nấc. Điểm backpressure được xác định khi tốc độ phát vượt khả năng
tiêu thụ: consumer lag tăng liên tục, throughput không còn tăng tương ứng và
latency tăng. Đồng thời quan sát giới hạn RAM `1.5G` của Redpanda, `2G` của
PostgreSQL và tài nguyên do cAdvisor báo cáo.

Không kết luận “hệ thống chịu tải” chỉ từ việc container còn sống. Kết luận
phải đi kèm đường biểu diễn input rate, processed rate, lag và latency.

### Partition và khôi phục

1. Tăng số partition của topic, đo lại throughput và lag.
2. Dừng có chủ đích tiến trình Spark hoặc container PostgreSQL.
3. Khởi động lại từ checkpoint gần nhất.
4. So sánh số lượng và định danh bản ghi trước/sau khôi phục để kiểm tra
   duplicate.
5. Mô phỏng mất mạng nội bộ hoặc DB unavailable, ghi nhận thời gian phát hiện,
   thời gian phục hồi và số bản ghi bị ảnh hưởng.

Checkpoint phải nằm trên volume bền vững, tách khỏi image/container. Trước khi
chạy thí nghiệm kill/restart, cần hoàn thiện volume mount checkpoint trong
Compose và xác nhận quyền ghi của user chạy Spark.

## Quan sát và số liệu

Mục tiêu tích hợp:

- cAdvisor: CPU, memory working set, network I/O, filesystem I/O;
- Prometheus: scrape metrics của Spark, generator, Redpanda và PostgreSQL;
- Grafana: dashboard throughput, input/processed rate, consumer lag,
  checkpoint duration/status, watermark, late-data count và end-to-end latency.

Mỗi dashboard phải có khoảng thời gian, đơn vị và nguồn dữ liệu rõ ràng.
Metrics cần được lưu cùng dữ liệu thô để có thể tái lập biểu đồ sau sự cố.

## Phân công và ranh giới trách nhiệm

| Thành viên | Phạm vi chính | KPI lõi | Rủi ro cần quản trị |
|---|---|---|---|
| Thái (Leader) | Kiến trúc, Docker/IaC, broker, DB, volume checkpoint | Hạ tầng dựng lại được và không lộ secret | Cạn tài nguyên, mất volume, rò rỉ mật khẩu |
| Tân Lê | Generator, schema sự kiện, Spark window/watermark/checkpoint | 20% late data tái lập được; không vứt dữ liệu im lặng | Watermark sai, parse timestamp sai |
| Tân Lưu | Prometheus/Grafana, benchmark, kill/recovery, phân tích | Có số liệu thô và biểu đồ cho từng kịch bản | Thiếu bằng chứng khi hệ thống sập |

## Tiêu chí hoàn thành

- Có baseline và stress test chạy lại được từ một commit cụ thể.
- Có biểu đồ đối chiếu trước/sau khi đổi watermark và partition.
- Xác định được thời điểm consumer lag và backpressure tăng cao.
- Có bằng chứng checkpoint phục hồi được pipeline.
- Có truy vấn kiểm tra duplicate và tính toàn vẹn dữ liệu.
- Có log riêng cho late data bị định tuyến sau watermark.
- Có hướng dẫn phục hồi khi broker, DB hoặc mạng nội bộ không khả dụng.
- Dọn tài nguyên sau thí nghiệm và đóng version tag cuối cùng trên Git.

## Dọn dẹp

Dừng stack nhưng giữ dữ liệu volume:

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
- Grafana hạ tầng: http://localhost:13300
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

Chỉ sau khi đã xuất metrics, log, kết quả PostgreSQL và checkpoint cần thiết,
mới xóa dữ liệu thí nghiệm trong các thư mục bind mount. Không commit dữ liệu
runtime trong `volumes/` hoặc bất kỳ secret nào vào repository.
Chỉ xóa dữ liệu sau khi đã lưu kết quả:

- `volumes/`
- `checkpoints/`
- `backups/`
- `NhutTan/ket_qua/`

Compose đã có Redpanda, Redpanda Console, PostgreSQL, event generator và
cAdvisor với các giới hạn tài nguyên phục vụ thí nghiệm. Spark Structured
Streaming, checkpoint bền vững, Prometheus/Grafana, sink hoàn chỉnh và các
benchmark vẫn là các hạng mục cần triển khai tiếp theo theo kế hoạch nghiên
cứu.
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
