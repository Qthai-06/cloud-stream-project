# Đề tài 16 — Hệ thống xử lý luồng thời gian thực

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

Generator chạy trong mạng Docker `stream-network`, vì vậy kết nối đến broker
dùng `redpanda:29092`. Máy host kết nối đến Kafka API bằng
`localhost:19092`.

### Hành vi của generator

Thiết lập mặc định trong
[`configs/pipeline_config.yaml`](./configs/pipeline_config.yaml):

- tốc độ: `10 msg/s`;
- tỷ lệ late data mục tiêu: `20%`;
- độ trễ timestamp của late data: ngẫu nhiên `30–120 giây`;
- thiết bị: `sensor-A101` đến `sensor-A105`;
- topic: `iot-sensor-events`.

`event_time` của late data bị lùi so với thời gian phát. Trường
`is_late_simulated` chỉ là nhãn phục vụ đối chiếu; pipeline phải xác định dữ
liệu muộn bằng watermark, không được dựa vào nhãn này.

Mọi event được kiểm tra độc lập bằng
[`src/generator/event_schema.py`](./src/generator/event_schema.py) trước khi
được serialize và gửi vào Kafka. Validator kiểm tra các field bắt buộc, kiểu
dữ liệu, timestamp có timezone, miền giá trị nhiệt độ/độ ẩm và trạng thái.
Spark Structured Streaming dùng cùng định nghĩa schema để tránh lệch contract
giữa producer và consumer.

## Thành phần hạ tầng

| Thành phần | Vai trò | Giới hạn / trạng thái |
|---|---|---|
| Redpanda | Message broker tương thích Kafka | 1 CPU, giới hạn RAM `1.5G`; Redpanda tự giới hạn bộ nhớ khả dụng ở `1G` |
| Redpanda Console | Quan sát topic, partition và consumer | Đã có trong Compose |
| PostgreSQL | Kho nhận kết quả streaming và integrity checks | Container `postgres` |
| Event Generator | Phát sự kiện IoT liên tục | 10 msg/s, chạy nội bộ Docker |
| cAdvisor | Đo CPU, RAM, filesystem và network của container | Đã có trong Compose |
| Spark Structured Streaming | Window, watermark, checkpoint và PostgreSQL sink | Service `stream-processor` trong Compose |
| Prometheus/Grafana | Lưu và trực quan hóa metrics | Hạng mục tích hợp tiếp theo; chưa có service trong Compose hiện tại |

PostgreSQL là database duy nhất của pipeline và được dùng bởi cả sink streaming
và bộ kiểm tra integrity.

## Yêu cầu tiên quyết

- Docker Engine có hỗ trợ Docker Compose v2 (`docker compose version`);
- Git;
- tối thiểu **6 GB RAM khả dụng** cho Docker Desktop/VM. Khuyến nghị 8 GB
  hoặc hơn để còn chỗ cho hệ điều hành và công cụ quan sát;
- tối thiểu 2 CPU logic. Khi chạy stress test nên dùng 4 CPU nếu máy cho phép;
- không có dịch vụ khác chiếm các cổng được liệt kê bên dưới;
- trên Docker Desktop, bật chia sẻ thư mục chứa repository.

Các giới hạn `deploy.resources` trong Compose được dùng để tạo điểm nghẽn;
chúng không thay thế việc cấp đủ RAM cho Docker VM.

## Khởi chạy

### 1. Kiểm tra cấu hình

Các biến `POSTGRES_USER`, `POSTGRES_PASSWORD` và `POSTGRES_DB` được Compose
đọc từ file `.env`. Không đưa mật khẩu thật vào Git. Nếu repository chưa có
`.env`, tạo file cục bộ với các giá trị phù hợp trước khi khởi chạy.

Kiểm tra file Compose:

```powershell
docker compose config
```

### 2. Dọn môi trường cũ và khởi chạy

Lệnh chuẩn để loại bỏ container mồ côi và dựng lại generator:

```powershell
docker compose down --remove-orphans
docker compose up -d --build
```

`down` không xóa các volume bind mount trong `volumes/`. Dữ liệu Redpanda và
PostgreSQL vì thế vẫn tồn tại giữa các lần khởi động. Chỉ xóa dữ liệu thí
nghiệm sau khi đã lưu kết quả và xác nhận không cần khôi phục.

### 3. Kiểm tra trạng thái

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
docker compose down --remove-orphans
```

Chỉ sau khi đã xuất metrics, log, kết quả PostgreSQL và checkpoint cần thiết,
mới xóa dữ liệu thí nghiệm trong các thư mục bind mount. Không commit dữ liệu
runtime trong `volumes/` hoặc bất kỳ secret nào vào repository.

## Trạng thái hiện tại

Compose đã có Redpanda, Redpanda Console, PostgreSQL, event generator và
cAdvisor với các giới hạn tài nguyên phục vụ thí nghiệm. Spark Structured
Streaming, checkpoint bền vững, Prometheus/Grafana, sink hoàn chỉnh và các
benchmark vẫn là các hạng mục cần triển khai tiếp theo theo kế hoạch nghiên
cứu.
