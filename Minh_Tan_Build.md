# Tuần 1 - 2

### BƯỚC 1: LỰA CHỌN ENGINE – NÊN CHỌN SPARK HAY FLINK?

Đối với Đề tài 16 (yêu cầu đo P95 Latency, kiểm tra Watermark, Crash-Recovery với Checkpoint và đối chiếu kết quả Batch/Stream):

* **Lời khuyên :** Nên chọn **Apache Spark (PySpark - Structured Streaming)** làm engine chính.
  * **Lý do:** PySpark rất ổn định trên Python, tích hợp thư viện Kafka connector cực mượt (`spark-sql-kafka`), dễ dàng debug thư mục `checkpointLocation`, và hỗ trợ API Micro-batch giúp đối chiếu trực tiếp với kết quả Batch (Ground Truth).
  * *(Nếu bạn chọn Apache Flink: PyFlink đòi hỏi cấu hình môi trường Java-Python IPC phức tạp hơn và việc cấu hình StateBackend RocksDB trên máy cá nhân dễ phát sinh lỗi tài nguyên).*

Dưới đây là hướng dẫn thiết lập **PySpark**.

---

### BƯỚC 2: CÀI ĐẶT MÔI TRƯỜNG PYSPARK

Vì hạ tầng (Redpanda, DB) đã nằm trong Docker, **cách tối ưu nhất là chạy Engine tính toán trên môi trường ảo Python (Virtual Environment) cục bộ của máy** để tiện code, debug và theo dõi log chi tiết mà không bị gò bó tài nguyên container.

#### 1. Yêu cầu tiên quyết: Cài đặt Java (Bắt buộc cho Spark/Flink)
Spark chạy trên JVM, do đó máy bạn bắt buộc phải có Java (khuyến nghị **Java 11** hoặc **Java 17**):
* **Kiểm tra:**
  ```bash
  java -version
  ```
* Nếu chưa có:
  * **Ubuntu/Debian:** `sudo apt install openjdk-17-jdk`
  * **macOS:** `brew install openjdk@17`
  * **Windows:** Tải OpenJDK 17 và cài biến môi trường `JAVA_HOME`.
* ***Nếu gặp khó khăn hãy bấm vào docs để xem "Đề tài 16" có thẻ con cài jdk 17*** <https://docs.google.com/document/d/1r1w4Kp32SeUYfza2sVOvXKVmNnVLlb1jRNFFDiJtAB4/edit?usp=sharing>
#### 2. Khởi tạo môi trường ảo Python
Mở terminal tại thư mục gốc dự án:
```bash
# Tạo môi trường ảo (khuyến nghị Python 3.10 hoặc 3.11)
python3 -m venv .venv

# Kích hoạt môi trường ảo:
# Trên Linux/macOS:
source .venv/bin/activate
# Trên Windows (PowerShell):
# .venv\Scripts\Activate.ps1
```

#### 3. Tạo file `requirements.txt`
Tạo file `requirements.txt` ở thư mục gốc với các thư viện chuẩn cho toàn bộ 5 tuần:
```text
# Streaming Engine
pyspark==3.5.1

# Kafka Producer & Ingestion
kafka-python==2.0.2

# Database Drivers
psycopg2-binary==2.9.9

# Metrics & Utilities
pyyaml==6.0.1
prometheus-client==0.20.0
numpy==1.26.4
pandas==2.2.2
```
Cài đặt dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

### BƯỚC 3: CẤU TRÚC THƯ MỤC MÃ NGUỒN CHUẨN (PROJECT REPO LAYOUT)

Một dự án Data Engineering chuẩn cần tách biệt rõ ràng giữa **Tầng sinh dữ liệu (Generator)**, **Tầng tính toán luồng (Streaming)**, **Tầng kiểm chứng (Batch Evaluation)** và **Tầng cấu hình**.

Bạn hãy tạo cây thư mục theo cấu trúc chuẩn sau:

```text
cloud-stream-project/
├── .env.example
├── .env
├── .gitignore
├── docker-compose.yml          # Đã có: Redpanda, Postgres, ClickHouse
├── requirements.txt
├── README.md
│
├── configs/                    # Toàn bộ cấu hình hệ thống
│   ├── pipeline_config.yaml    # Cấu hình Kafka topic, window size, watermark delay
│   └── database_schema.sql     # Script khởi tạo bảng ClickHouse / Postgres
│
├── checkpoints/                # Nơi Spark lưu trữ State Store và Offset (Cần gitignore)
│
├── src/
│   ├── __init__.py
│   ├── generator/              # TẦNG 1: Sinh luồng sự kiện (Tuần 2)
│   │   ├── __init__.py
│   │   └── event_producer.py   # Script sinh dữ liệu cảm biến + tiêm late data
│   │
│   ├── streaming/              # TẦNG 2: Xử lý luồng Spark Structured Streaming (Tuần 2 & 3)
│   │   ├── __init__.py
│   │   ├── spark_pipeline.py   # Job chính: Đọc Kafka -> Watermark -> Window -> Sink
│   │   └── metrics_listener.py # StreamingQueryListener đo Latency & Throughput
│   │
│   ├── batch/                  # TẦNG 3: Đối chiếu kiểm chứng Ground Truth (Tuần 5)
│   │   ├── __init__.py
│   │   └── batch_evaluator.py  # Đọc file tĩnh, chạy batch window để so sánh kết quả
│   │
│   └── utils/                  # Thư viện tiện ích dùng chung
│       ├── __init__.py
│       ├── config_loader.py    # Đọc file YAML
│       └── db_connector.py     # Kết nối ClickHouse / Postgres
│
├── benchmarks/                 # Tự động hóa đo đạc thực nghiệm (Tuần 4)
│   ├── run_watermark_test.sh   # Kịch bản đổi mốc watermark (5s, 30s, 120s)
│   └── run_partition_test.sh   # Kịch bản test 1 partition vs đa partition
│
└── tests/
    └── test_smoke_pipeline.py  # Kiểm thử kết nối hạ tầng
```

> **Lưu ý quan trọng cho `.gitignore`:**
> Mở file `.gitignore` và thêm ngay 2 dòng này để tránh commit rác hoặc file dữ liệu nặng lên GitHub:
> ```text
> .venv/
> checkpoints/
> *.log
> ```

---

### BƯỚC 4: VIẾT SMOKE TEST KIỂM TRA PIPELINE SPARK - KAFKA

Để chứng minh môi trường đã cài đặt thành công 100%, bạn hãy tạo file kiểm thử nhanh: `src/streaming/smoke_test.py`. File này sẽ khởi tạo Spark Session, nạp gói thư viện `spark-sql-kafka` và kết nối thử tới Redpanda cổng `localhost:9092`.

```python
# src/streaming/smoke_test.py
import os
from pyspark.sql import SparkSession

def main():
    print(">>> Đang khởi tạo Spark Session với Kafka Connector...")
    
    # Khai báo gói kết nối Kafka tương ứng với Spark 3.5.1
    # Spark sẽ tự động tải JAR package từ Maven repository ở lần chạy đầu tiên
    spark = (
        SparkSession.builder
        .appName("SmokeTest-Redpanda-Spark")
        .master("local") # Chạy 2 core cục bộ
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2") # Giữ số partition nhỏ để tối ưu RAM laptop
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    print(f" Spark Version: {spark.version} - Khởi tạo thành công!")

    # Test đọc từ Redpanda (Kafka API)
    try:
        print(">>> Thử kết nối tới Redpanda tại localhost:9092...")
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test-topic")
            .option("startingOffsets", "latest")
            .load()
        )
        print(" Kết nối Spark <-> Redpanda thành công! Pipeline sẵn sàng nhận sự kiện.")
    except Exception as e:
        print(f" Lỗi kết nối Redpanda: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

#### Chạy kiểm tra:
1. Đảm bảo Docker đang chạy: `docker compose up -d`.
2. Chạy smoke test:
   ```bash
   python src/streaming/smoke_test.py
   ```
*(Ở lần chạy đầu tiên, Spark sẽ mất khoảng 30–60 giây để tải thư viện Maven `spark-sql-kafka-0-10_2.12`. Khi màn hình hiện thông báo kết nối thành công, bạn đã hoàn tất toàn bộ môi trường).*

# Tuần 3: Xây dựng Event Data Generator & Giả lập Dữ liệu Trễ (Late Data)
### BƯỚC 1: XÂY DỰNG EVENT GENERATOR

* **File mã nguồn:** `src/generator/event_producer.py`
* **File cấu hình:** `configs/pipeline_config.yaml`
* **Nghiệp vụ giả lập:** Tạo luồng dữ liệu cảm biến nhiệt độ và độ ẩm IoT bao gồm các trường: `device_id`, `event_time`, `temperature`, `humidity`, `status`.

### BƯỚC 2: CÀI ĐẶT CƠ CHẾ GIẢ LẬP DỮ LIỆU ĐẾN MUỘN (LATE DATA)

* **Tỉ lệ trễ:** Khởi tạo thuật toán ngẫu nhiên đảm bảo **20%** tổng số sự kiện phát sinh bị trễ timestamp (`event_time`).
* **Độ trễ:** Lùi `event_time` của các sự kiện này từ **30 đến 120 giây** so với thời gian phát thực tế (Processing Time).
* **Partition Key:** Sử dụng `device_id` làm Partition Key để đảm bảo dữ liệu của từng cảm biến giữ đúng thứ tự trên Kafka Partition.

### BƯỚC 3: THỰC NGHIỆM KẾT NỐI VÀ KIỂM TRA

* **Kafka Topic:** `iot-sensor-events` (Sử dụng Redpanda tại `localhost:9092`).
* **Tốc độ phát:** Cấu hình linh hoạt (mặc định: **10 msg/sec**).
* **Kiểm chứng:**
* Xác nhận luồng dữ liệu được đẩy thành công thông qua **Redpanda Console UI** (`http://localhost:8080`).
* Kiểm tra các gói tin JSON để đảm bảo có đầy đủ thông tin timestamp và cờ đánh dấu `is_late_simulated`.

Quy trình ngắn gọn cần nhớ khởi động docker:

```powershell
docker info
cd "đường dẫn của dự án ví dụ: D:\UTH\Dien_Toan_Dam_May\cloud-stream-project"
docker compose up -d
docker compose ps
Test-NetConnection localhost -Port 9092

Dữ liệu sinh ra từ `event_producer.py` **không được lưu trực tiếp thành file trong thư mục project**. Nó được gửi vào Kafka-compatible broker là **Redpanda**, trong topic:

```yaml
iot-sensor-events
```

Cấu hình nằm trong `pipeline_config.yaml`.

## Xem dữ liệu bằng Redpanda Console

Mở trình duyệt:

```text
http://localhost:8080
```

Sau đó:

1. Chọn topic `iot-sensor-events`.
2. Chọn phần **Messages**.
3. Chọn partition.
4. Bấm **Consume** hoặc **Refresh**.
5. Xem các message JSON được producer gửi lên.

Dữ liệu sẽ có dạng tương tự:

```json
{
  "device_id": "sensor-A101",
  "event_time": "23/10/2026 16:20:00",
  "temperature": 35.42,
  "humidity": 68.15,
  "status": "OK",
  "is_late_simulated": false
}
```

## Xem bằng command line

Liệt kê topic:

```powershell
docker exec redpanda rpk topic list
```

Xem thông tin topic:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Đọc message mới:

```powershell
docker exec -it redpanda rpk topic consume iot-sensor-events
```

Lệnh này sẽ chờ và hiển thị các message mới được gửi vào topic.

Muốn đọc lại các message đã có từ đầu topic:

```powershell
docker exec -it redpanda rpk topic consume iot-sensor-events --offset start
```

Dừng lệnh xem dữ liệu bằng:

```text
Ctrl + C
```

## Dữ liệu vật lý nằm ở đâu?

Trong `docker-compose.yml`, Redpanda có volume:

```yaml
volumes:
  - ./volumes/redpanda:/var/lib/redpanda/data
```

Vì vậy dữ liệu Kafka/Redpanda được lưu vật lý trong thư mục:

```text
D:\UTH\Dien_Toan_Dam_May\cloud-stream-project\volumes\redpanda
```

Tuy nhiên, **không nên mở trực tiếp các file bên trong thư mục này**, vì đó là dữ liệu nội bộ của Redpanda. Muốn xem dữ liệu, nên dùng:

- Redpanda Console tại `http://localhost:8080`
- Lệnh `rpk topic consume`
- Consumer Python/Spark ở các bước sau

## Luồng dữ liệu hiện tại

```text
event_producer.py
        |
        | gửi JSON qua localhost:9092
        v
Redpanda
        |
        | topic: iot-sensor-events
        v
Redpanda Console / Spark Streaming / Consumer
```

## Quy trình kiểm tra đầy đủ

Terminal 1: khởi động Redpanda:

```powershell
docker compose up -d redpanda
```

Terminal 2: chạy producer:

```powershell
.\.venv\Scripts\python.exe .\src\generator\event_producer.py
```

Terminal 3: xem dữ liệu:

```powershell
docker exec -it redpanda rpk topic consume iot-sensor-events
```

Hoặc mở trình duyệt:

```text
http://localhost:8080
```

Lưu ý: Kafka lưu message theo retention policy. Dữ liệu không phải là file xuất ra vĩnh viễn; về sau Spark Streaming sẽ đọc dữ liệu từ topic rồi ghi kết quả sang database, file hoặc hệ thống lưu trữ khác.

# Tuần 4: Viết logic Window, thiết lập watermark và kích hoạt checkpoint. Ghi kết quả vào CSDL.

Ở **Tuần 4**, cần chạy 2 file chính:

1. `event_producer.py`: tạo dữ liệu cảm biến và gửi vào Redpanda/Kafka.
2. `spark_pipeline.py`: đọc dữ liệu từ Kafka, xử lý window/watermark/checkpoint rồi ghi kết quả vào PostgreSQL.

Hai file này phải chạy **đồng thời ở 2 terminal khác nhau**.

## Thứ tự chạy

### Terminal 1: Khởi động Docker

Mở PowerShell thứ nhất:

```powershell
cd "D:\UTH\Dien_Toan_Dam_May\cloud-stream-project"

docker compose up -d
```

Kiểm tra container:

```powershell
docker ps
```

Cần có ít nhất:

- `redpanda`
- `postgres`

Không nên chỉ chạy Redpanda vì `spark_pipeline.py` còn ghi kết quả vào PostgreSQL.

---

### Terminal 2: Chạy Spark pipeline trước

Mở PowerShell thứ hai:

```powershell
cd "D:\UTH\Dien_Toan_Dam_May\cloud-stream-project"

.\.venv\Scripts\Activate.ps1
```

Nếu cần thiết lập Java 17 trong terminal hiện tại:
bấm kiểm tra coi cái này có đứng đầu chưa `C:\Program Files\Java\jdk-17' `Lệnh: `where.exe java`, nếu đứng đầu trong powershell rồi thì không sử dụng lệnh phía dưới đâu.
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
```

Kiểm tra:

```powershell
java -version
```

Phải hiện Java 17.

Sau đó chạy pipeline:

```powershell
.\.venv\Scripts\python.exe .\src\streaming\spark_pipeline.py
```

Khi chạy thành công, terminal sẽ giữ trạng thái đang chạy và hiển thị tương tự:

```text
Pipeline đang chạy. Nhấn Ctrl+C để dừng.
```

Nên chạy Spark trước vì file này đang dùng:

```python
.option("startingOffsets", "latest")
```

Nó chỉ đọc các message mới được gửi sau thời điểm pipeline bắt đầu.

---

### Terminal 3: Chạy event producer

Mở PowerShell thứ ba:

```powershell
cd "D:\UTH\Dien_Toan_Dam_May\cloud-stream-project"

.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe .\src\generator\event_producer.py
```

Nếu chạy thành công, bạn sẽ thấy:

```text
Khởi tạo Kafka Producer kết nối với Redpanda...
>>> Bắt đầu chuyền dữ liệu vào iot-sensor-events
>>> Cấu hình: 10 msg/s | 20% Late Data
```

Sau đó producer sẽ liên tục gửi dữ liệu vào topic `iot-sensor-events`.

## Sơ đồ chạy

```text
event_producer.py
        |
        | gửi dữ liệu JSON
        v
Redpanda - localhost:9092
        |
        | Spark đọc dữ liệu
        v
spark_pipeline.py
        |
        | window + watermark
        v
PostgreSQL - sensor_window_metrics
```

## Kiểm tra message Kafka

Có thể mở thêm Terminal 4:

```powershell
docker exec -it redpanda rpk topic list
```

Kiểm tra topic:

```powershell
docker exec -it redpanda rpk topic describe iot-sensor-events
```

Xem message:

```powershell
docker exec -it redpanda rpk topic consume iot-sensor-events
```

Dừng consumer bằng:

```text
Ctrl + C
```

## Kiểm tra kết quả trong PostgreSQL

Kết nối vào PostgreSQL:

```powershell
docker exec -it postgres psql -U admin -d app_db
```

Trong PostgreSQL, chạy:

```sql
SELECT *
FROM sensor_window_metrics
ORDER BY updated_at DESC
LIMIT 20;
```


## Lưu ý về thời gian xử lý

Trong `pipeline_config.yaml` hiện tại:

```yaml
window_duration: "1 minute"
watermark_delay: "120 seconds"
```

Vì watermark là 120 giây, kết quả có thể chưa ghi ngay lập tức. Hãy để:

- Spark chạy trước
- Producer chạy liên tục
- chờ khoảng 2–3 phút
- sau đó kiểm tra PostgreSQL trong vscode

## Cách dừng

Dừng producer:

```text
Ctrl + C
```

Dừng Spark:

```text
Ctrl + C
```

Dừng Docker sau khi kiểm tra xong:

```powershell
docker compose down
```

Lưu ý: Tuần 4 chạy cả `spark_pipeline.py` và `event_producer.py`, nhưng cần chạy Spark trước rồi mới chạy producer, mỗi file trong một terminal riêng.

---

# Tuần 5: Tinh chỉnh Watermark và định tuyến Late Data

### ĐANG THỰC HIỆN GÌ?

Tạo job độc lập `src/streaming/late_data_router.py` để đọc sự kiện từ Kafka topic `iot-sensor-events`. Job dùng watermark `120 seconds` từ `configs/pipeline_config.yaml`; mỗi micro-batch sẽ chủ động so sánh `event_time` với mốc watermark hiện tại. Sự kiện quá cũ không bị bỏ qua im lặng mà được định tuyến vào file JSONL để đối chiếu.

Các file cũ không bị thay đổi. Kết quả của Tuần 4 vẫn được giữ nguyên; job Tuần 5 dùng checkpoint riêng để không xung đột với pipeline cũ.

### ĐẦU VÀO

* **Kafka broker:** `localhost:9092`
* **Kafka topic:** `iot-sensor-events`
* **Dữ liệu đầu vào:** JSON gồm `device_id`, `event_time`, `temperature`, `humidity`, `status` và `is_late_simulated`.
* **Watermark:** giá trị `streaming.watermark_delay`, mặc định `120 seconds`.

### KẾT QUẢ ĐẦU RA

* **File bằng chứng late data:** `logs/late_data.jsonl`
* **Checkpoint riêng:** `checkpoints/late_data_router/`
* Mỗi dòng trong `late_data.jsonl` gồm `detected_at`, `batch_id`, `watermark_cutoff`, lý do lọc và toàn bộ event bị định tuyến.

Ví dụ một dòng log:

```json
{"batch_id": 4, "watermark_delay_seconds": 120, "reason": "event_time_older_than_watermark", "event": {"device_id": "sensor-A101", "is_late_simulated": true}}
```

File chỉ xuất hiện khi có sự kiện quá trễ. Nếu chưa có file, hãy để producer chạy đủ lâu hoặc phát một sự kiện có `event_time` cũ hơn 120 giây.

### CÁCH CHẠY

1. Khởi động Docker và Redpanda:

   ```cmd
   cd /d D:\UTH\Dien_Toan_Dam_May\cloud-stream-project
   docker compose up -d redpanda
   ```

2. Terminal 1 - chạy late-data router:

   ```cmd
   .\.venv\Scripts\python.exe .\src\streaming\late_data_router.py
   ```

3. Terminal 2 - chạy producer:

   ```cmd
   .\.venv\Scripts\python.exe .\src\generator\event_producer.py
   ```

   Để nhìn thấy log nhanh trong lúc kiểm thử, có thể chạy router với watermark ngắn hơn mức trễ của producer:

   ```cmd
   .\.venv\Scripts\python.exe .\src\streaming\late_data_router.py --watermark-delay "30 seconds"
   ```

   Khi chạy thực nghiệm chính thức, bỏ tùy chọn này để dùng `120 seconds` trong cấu hình.

4. Kiểm tra file log khi router đã chạy vài phút:

   ```cmd
   type logs\late_data.jsonl
   ```

   Hoặc trong PowerShell:

   ```powershell
   Get-Content .\logs\late_data.jsonl
   ```

5. Kiểm tra checkpoint:

   ```cmd
   dir checkpoints\late_data_router
   ```

Không xóa `checkpoints\late_data_router` khi muốn job tiếp tục từ offset cũ. Nếu xóa checkpoint để chạy lại từ đầu, hãy đổi tên hoặc xóa riêng `logs\late_data.jsonl` trước để tránh nhầm kết quả của các lần chạy.

# Tuần 5: KIỂM TRA TÍNH TOÀN VẸN VÀ KHÔNG NHÂN ĐÔI SAU KHI RESTART

#### ĐANG THỰC HIỆN GÌ?

Bổ sung bộ truy vấn `tests/sql/verify_stream_integrity.sql` để kiểm tra dữ liệu do Spark ghi vào bảng `sensor_window_metrics`. Bộ kiểm tra xác nhận khóa logic của mỗi kết quả (`window_start`, `window_end`, `device_id`) không bị nhân đôi, các cột bắt buộc không bị `NULL`, biên cửa sổ hợp lệ và kết quả có checksum để đối chiếu trước/sau khi khởi động lại.

Pipeline Tuần 4 dùng khóa chính trên ba cột logic và `ON CONFLICT DO UPDATE`. Vì vậy khi Spark chạy lại cùng micro-batch, sink cập nhật đúng dòng cũ thay vì thêm dòng mới. Phải giữ nguyên checkpoint khi restart; xóa checkpoint sẽ làm bài kiểm tra semantics không còn ý nghĩa vì pipeline có thể đọc lại dữ liệu từ đầu.

#### ĐẦU VÀO

* **Bảng kiểm tra:** `sensor_window_metrics` trong PostgreSQL.
* **Truy vấn:** `tests/sql/verify_stream_integrity.sql`.
* **Checkpoint cần giữ nguyên:** `checkpoints/spark_pipeline/`.

#### KẾT QUẢ CẦN ĐẠT

* Các kiểm tra trạng thái phải trả về `PASS`.
* Hai truy vấn phát hiện duplicate phải trả về **0 dòng**.
* `row_count` và `result_checksum` phải giống nhau trước và sau khi restart nếu không có event mới trong thời gian kiểm tra.

Đây là bằng chứng đầu ra **idempotent và at-least-once an toàn ở sink**: nếu một batch được gửi lại, PostgreSQL không tạo thêm dòng logic. Không kết luận exactly-once end-to-end chỉ từ bảng này; exactly-once tuyệt đối còn phụ thuộc source, checkpoint và việc không xóa checkpoint.

#### CÁCH CHẠY

1. Đảm bảo PostgreSQL và pipeline đã chạy:

   ```cmd
   cd /d D:\UTH\Dien_Toan_Dam_May\cloud-stream-project
   docker compose up -d postgres redpanda
   ```

2. Chạy bộ kiểm tra lần đầu và lưu lại `row_count`, `result_checksum`. Trong PowerShell, không dùng toán tử `<` vì đó không phải cú pháp chuyển hướng file hợp lệ của PowerShell. Dùng script kiểm tra trực tiếp:

   ```powershell
   .\.venv\Scripts\python.exe .\tests\sql\run_integrity_check.py `
     --host 127.0.0.1 `
     --port 15432 `
     --user admin `
     --database app_db `
     --password $env:POSTGRES_PASSWORD
   ```

   Nếu chạy trong CMD, có thể dùng file SQL:

   ```cmd
   docker exec -i postgres psql -U admin -d app_db < tests\sql\verify_stream_integrity.sql
   ```

3. Dừng Spark bằng `Ctrl+C`, nhưng **không xóa** `checkpoints\spark_pipeline`.

4. Khởi động lại đúng pipeline với checkpoint cũ:

   ```cmd
   .\.venv\Scripts\python.exe .\src\streaming\spark_pipeline.py
   ```

5. Khi pipeline đã khởi động xong, dừng producer để không phát sinh event mới, rồi chạy lại:

   ```powershell
   .\.venv\Scripts\python.exe .\tests\sql\run_integrity_check.py --user admin --database app_db
   ```

6. Đối chiếu hai lần chạy:

   * Duplicate query không trả về dòng nào.
   * `row_count` không tăng chỉ vì restart.
   * `result_checksum` giữ nguyên nếu không có input mới.

Nếu cần kiểm tra riêng các dòng trùng bằng PowerShell:

```powershell
docker exec -it postgres psql -U admin -d app_db -c "SELECT window_start, window_end, device_id, COUNT(*) FROM sensor_window_metrics GROUP BY window_start, window_end, device_id HAVING COUNT(*) > 1;"
```

Kết quả mong đợi là không có dòng nào.

Nếu muốn chạy file SQL trong PowerShell thay vì script Python, dùng pipe:

```powershell
Get-Content .\tests\sql\verify_stream_integrity.sql |
    docker exec -i postgres psql -U admin -d app_db
```

Trong PowerShell, không viết:

```powershell
docker exec -i postgres psql -U admin -d app_db < tests\sql\verify_stream_integrity.sql
```

vì toán tử `<` gây lỗi `RedirectionNotSupported`.
