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
├── docker-compose.yml          # Đã có: Redpanda và Postgres
├── requirements.txt
├── README.md
│
├── configs/                    # Toàn bộ cấu hình hệ thống
│   ├── pipeline_config.yaml    # Cấu hình Kafka topic, window size, watermark delay
│   └── database_schema.sql     # Script khởi tạo bảng PostgreSQL
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
│       └── db_connector.py     # Kết nối PostgreSQL
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

Để chứng minh môi trường đã cài đặt thành công 100%, bạn hãy tạo file kiểm thử nhanh: `src/streaming/smoke_test.py`. File này sẽ khởi tạo Spark Session, nạp gói thư viện `spark-sql-kafka` và kết nối thử tới Redpanda. Trong Docker dùng `redpanda:29092`, còn máy host dùng `localhost:19092`.

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
        print(">>> Thử kết nối tới Redpanda tại redpanda:29092...")
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "redpanda:29092")
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
   ```powershell
   $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
   $env:HADOOP_HOME = "C:\hadoop"
   $env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
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

* **Kafka Topic:** `iot-sensor-events` (Docker dùng `redpanda:29092`, host dùng `localhost:19092`).
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
Test-NetConnection localhost -Port 19092

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
        | gửi JSON qua redpanda:29092
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
Redpanda - redpanda:29092
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

* **Kafka broker:** `redpanda:29092` trong Docker, `localhost:19092` trên host
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
     --port 5432 `
     --user admin `
     --database app_db `
     --password "YOUR_PASSWORD"
   ```

   Nếu chạy trong CMD, có thể dùng file SQL:

   ```cmd
   docker exec -i postgres psql -U admin -d app_db < tests\sql\verify_stream_integrity.sql
   ```

3. Dừng Spark bằng `Ctrl+C`, nhưng **không xóa** `checkpoints\spark_pipeline`.

4. Khởi động lại đúng pipeline với checkpoint cũ:

   ```powershell
   $env:POSTGRES_HOST = "127.0.0.1"
   $env:POSTGRES_PORT = "5432"
   $env:POSTGRES_USER = "admin"
   $env:POSTGRES_DB = "app_db"
   $env:POSTGRES_PASSWORD = "mat_khau_postgres"
   $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
   $env:HADOOP_HOME = "C:\hadoop"
   $env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
   .\.venv\Scripts\python.exe -m src.streaming.spark_pipeline
   ```

5. Khi pipeline đã khởi động xong, dừng producer để không phát sinh event mới, rồi chạy lại:

   ```powershell
   $env:PGPASSWORD = "YOUR_PASSWORD"
   .\.venv\Scripts\python.exe .\tests\sql\run_integrity_check.py `
   --host 127.0.0.1 `
   --port 15432 `
   --user admin `
   --database app_db
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

---

# Tuần 6: Cải thiện Kịch bản nghiệm thu đầy đủ: Generator → Kafka → Spark → PostgreSQL

Phần này là checklist kiểm thử thực tế cho 6 yêu cầu cải thiện. Chỉ thực hiện
trên môi trường thử nghiệm. Không xóa volume PostgreSQL, dữ liệu Redpanda hoặc
checkpoint khi đang kiểm tra restart.

## 1. Chuẩn bị và xác nhận một database duy nhất

Pipeline sử dụng **PostgreSQL duy nhất**:

```text
event_producer.py
        ↓
Redpanda/Kafka: iot-sensor-events
        ↓
Spark Structured Streaming
        ↓
PostgreSQL: sensor_window_metrics
```

Schema được tạo tự động bởi `spark_pipeline.py` từ
`configs/database_schema.sql`. Kiểm tra schema sau khi PostgreSQL đã chạy:

```powershell
docker exec postgres psql -U admin -d app_db -c "\dt"
docker exec postgres psql -U admin -d app_db -c "\d sensor_window_metrics"
```

Kết quả cần có bảng `sensor_window_metrics` với khóa chính:

```text
(window_start, window_end, device_id)
```

Kiểm tra không còn database ClickHouse trong Compose:

```powershell
docker compose config --services
```

Danh sách cần có `redpanda`, `postgres`, `data-generator`,
`stream-processor`; không cần service `clickhouse`.

## 2. Xác nhận cấu hình Docker và host

### 2.1. Cấu hình trong Docker

Các container dùng địa chỉ nội bộ Docker:

| Thành phần | Địa chỉ trong Docker |
|---|---|
| Redpanda Kafka | `redpanda:29092` |
| PostgreSQL | `postgres:5432` |
| Kafka topic | `iot-sensor-events` |
| Spark checkpoint | `/opt/workspace/checkpoints` |

Kiểm tra biến môi trường thực tế của Spark:

```powershell
docker compose config
docker inspect spark-processor --format "{{range .Config.Env}}{{println .}}{{end}}"
```

Phải thấy tối thiểu:

```text
KAFKA_BOOTSTRAP_SERVERS=redpanda:29092
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=app_db
```

### 2.2. Cấu hình khi chạy từ máy host

Các tiến trình chạy ngoài Docker dùng:

| Thành phần | Địa chỉ từ host |
|---|---|
| Redpanda Kafka | `localhost:19092` |
| PostgreSQL | `localhost:15432` |

Không dùng `localhost` bên trong container. Không dùng
`redpanda:29092` từ PowerShell trên host.

Kiểm tra cổng:

```powershell
Test-NetConnection localhost -Port 19092
Test-NetConnection localhost -Port 15432
```

Hai lệnh cần trả về `TcpTestSucceeded: True`.

## 3. Khởi động pipeline và test end-to-end

### 3.1. Dọn checkpoint chỉ trước lần chạy baseline

Chỉ chạy bước này khi bắt đầu một bộ test mới, không chạy trong test restart:

```powershell
Remove-Item -Recurse -Force .\checkpoints\spark_pipeline
Remove-Item -Recurse -Force .\checkpoints\late_data_router -ErrorAction SilentlyContinue
Remove-Item -Force .\logs\late_data.jsonl -ErrorAction SilentlyContinue
```

### 3.2. Khởi động hạ tầng

Từ thư mục gốc repository:

```powershell
docker compose up -d redpanda postgres
docker compose ps
```

Chỉ tiếp tục khi `redpanda` và `postgres` ở trạng thái `running`.

### 3.3. Chạy Spark trước Generator

Có hai cách chạy. Chỉ chọn **một** cách để tránh chạy trùng pipeline.

#### Cách A: Chạy Spark bằng Docker

```powershell
docker compose up -d --build stream-processor
docker compose logs -f stream-processor
```

Log đạt yêu cầu phải có thông báo tương tự:

```text
Pipeline dang chay. Nhan Ctrl+C de dung.
```

Dockerfile của `stream-processor` phải khởi chạy:

```powershell 
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
$env:POSTGRES_HOST = "127.0.0.1"
$env:POSTGRES_PORT = "15432"
$env:POSTGRES_USER = "admin"
$env:POSTGRES_DB = "app_db"
$env:POSTGRES_PASSWORD = "YOUR_PASSWORD"
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;
$env:HADOOP_HOME\bin;$env:Path"
.\.venv\Scripts\python.exe -m src.streaming.spark_pipeline
```

Xác nhận entrypoint thực tế:

```powershell
docker inspect spark-processor --format "{{json .Config.Cmd}}"
```

#### Cách B: Chạy Spark từ host

```powershell
.\.venv\Scripts\python.exe .\src\streaming\spark_pipeline.py
```

Khi chạy từ host, cấu hình Kafka phải là `localhost:19092` và PostgreSQL là
`localhost:15432` thông qua biến môi trường của terminal hiện tại.

### 3.4. Chạy Generator

Nếu Spark chạy trong Docker, chạy Generator trong Docker. Nếu thấy docker ```powershellcompose logs -f data-generator``` chạy lâu hãy sử dụng lệnh thứ 2:

```powershell
docker compose up -d --build data-generator
docker compose logs -f data-generator
```

Nếu Spark chạy từ host, có thể chạy Generator từ host trong terminal khác:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
.\.venv\Scripts\python.exe .\src\generator\event_producer.py
```

Log cần thể hiện:

```text
>>> Bắt đầu chuyền dữ liệu vào iot-sensor-events
>>> Cấu hình: 10 msg/s | 20% Late Data
```

### 3.5. Xác nhận dữ liệu đi hết pipeline

Để Generator chạy ít nhất 3 phút vì watermark hiện tại là `120 seconds`.
Sau đó kiểm tra Kafka:

```powershell
docker exec redpanda rpk topic describe iot-sensor-events
docker exec redpanda rpk topic consume iot-sensor-events --offset start
```

Kiểm tra PostgreSQL:

```powershell
docker exec postgres psql -U admin -d app_db -c "SELECT COUNT(*) AS row_count FROM sensor_window_metrics;"
docker exec postgres psql -U admin -d app_db -c "SELECT * FROM sensor_window_metrics ORDER BY updated_at DESC LIMIT 10;"
```

**Đạt E2E** khi:

- Kafka có message trong topic `iot-sensor-events`.
- Spark không báo lỗi đọc Kafka hoặc ghi PostgreSQL.
- `sensor_window_metrics` có ít nhất một dòng.
- `window_end > window_start`.
- `device_id`, `event_count` không bị `NULL`.

## 4. Test restart và giữ checkpoint

### 4.1. Ghi nhận trạng thái trước restart

Giữ Generator chạy ổn định, mở terminal mới và chạy:

```powershell
python .\tests\sql\run_integrity_check.py `
  --host 127.0.0.1 `
  --port 15432 `
  --user admin `
  --database app_db `
  --password "YOUR_PASSWORD"
```

Nếu biến môi trường chưa có:

```powershell
$env:POSTGRES_PASSWORD = "mat_khau_trong_file_env"
```

Lưu lại hai dòng:

```text
row_count=...
result_checksum=...
```

Kiểm tra checkpoint đang tồn tại:

```powershell
Get-ChildItem .\checkpoints\spark_pipeline -Recurse
```

### 4.2. Dừng và khởi động lại Spark

Nếu chạy Spark bằng Docker:

```powershell
docker compose stop stream-processor
docker compose start stream-processor
docker compose logs -f stream-processor
```

Nếu chạy Spark từ host, dừng bằng `Ctrl+C`, sau đó chạy lại:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
$env:POSTGRES_HOST = "127.0.0.1"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_USER = "admin"
$env:POSTGRES_DB = "app_db"
$env:POSTGRES_PASSWORD = "mat_khau_postgres"
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
.\.venv\Scripts\python.exe -m src.streaming.spark_pipeline
```

**Không xóa** `.\checkpoints\spark_pipeline`.

### 4.3. Đối chiếu sau restart

Tạm dừng Generator để không phát sinh event mới, chờ Spark khởi động xong rồi
chạy lại integrity check:

```powershell
.\.venv\Scripts\python.exe .\tests\sql\run_integrity_check.py `
  --host 127.0.0.1 `
  --port 15432 `
  --user admin `
  --database app_db `
  --password $env:POSTGRES_PASSWORD
```

**Đạt test restart** khi:

- Các check trả về `PASS`.
- `duplicate_logical_rows: PASS (0)`.
- Checkpoint vẫn còn.
- `row_count` và `result_checksum` không đổi nếu không có event mới.
- Pipeline tiếp tục đọc event mới sau khi Generator chạy lại.

## 5. Test late data thực tế và log truy vết

### 5.1. Chạy late-data router riêng

Router phải dùng checkpoint riêng, không dùng chung với pipeline chính:

```powershell
docker compose up -d redpanda
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
.\.venv\Scripts\python.exe -m src.streaming.late_data_router `
  --watermark-delay "30 seconds"
```

Dùng `30 seconds` chỉ để nghiệm thu nhanh. Khi chạy thực nghiệm chính thức,
bỏ tùy chọn này để dùng `120 seconds` trong cấu hình.

Mở terminal khác và chạy Generator:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
.\.venv\Scripts\python.exe .\src\generator\event_producer.py
```

Để Generator chạy đủ lâu cho late event xuất hiện. Kiểm tra:

```powershell
Get-ChildItem .\checkpoints\late_data_router -Recurse
Get-Content .\logs\late_data.jsonl
```

### 5.2. Tiêu chí log có thể truy vết

Mỗi dòng trong `logs\late_data.jsonl` phải là một JSON object hợp lệ và có:

```text
detected_at
batch_id
watermark_delay_seconds
watermark_cutoff
reason
event
```

Trong `event` cần truy được tối thiểu:

```text
device_id
event_time
temperature
humidity
is_late_simulated
```

Kiểm tra nhanh số dòng log:

```powershell
(Get-Content .\logs\late_data.jsonl).Count
```

**Đạt late-data test** khi có ít nhất một dòng với:

```text
reason=event_time_older_than_watermark
```

và `event_time` nhỏ hơn `watermark_cutoff`. Không kết luận late data chỉ dựa
trên `is_late_simulated`; nhãn này chỉ dùng để đối chiếu.

## 6. Bảng nghiệm thu cuối cùng

| # | Hạng mục | Bằng chứng cần lưu | Kết quả |
|---|---|---|---|
| 1 | Một database và schema | `\d sensor_window_metrics`, không có service ClickHouse | PASS  |
| 2 | Docker/host rõ ràng | `docker compose config`, kiểm tra port 19092/15432 | PASS  |
| 3 | E2E Generator → Kafka → Spark → DB | Kafka có message, PostgreSQL có row | PASS  |
| 4 | Restart giữ checkpoint | Checksum trước/sau, thư mục checkpoint còn nguyên | PASS  |
| 5 | Late data thực tế | `logs/late_data.jsonl` có JSON truy vết hợp lệ | BLOCKED: compose hiện tại chưa chạy `late_data_router` |
| 6 | Docker entrypoint | `docker inspect` hiển thị `spark_pipeline.py` | PASS  |

Khi hoàn tất, lưu lại thời điểm test, commit đang chạy, cấu hình watermark,
window, số message Kafka, `row_count`, `result_checksum`, trạng thái container
và đường dẫn các file log/checkpoint. Không xóa các bằng chứng này trước khi
đối chiếu báo cáo.

---

# Tuần 6: Cải thiện lần 2 — Checklist bằng chứng nghiệm thu

Mục này là checklist bổ sung cần hoàn thiện để đủ điều kiện nghiệm thu. Không
đánh dấu trước khi đã chạy thực tế và lưu lại log, ảnh chụp màn hình hoặc file
kết quả tương ứng. Mỗi bằng chứng nên ghi rõ thời gian chạy, commit đang dùng,
cấu hình watermark/window và trạng thái các container.

## Checklist bắt buộc

- [ ] **Chạy thực tế luồng tích hợp Generator → Redpanda → Spark → PostgreSQL.**
  - Khởi động toàn bộ Compose:
    ```powershell
    docker compose up -d --build
    docker compose ps
    ```
  - Chạy generator đủ lâu để có dữ liệu:
    ```powershell
    docker compose logs -f data-generator
    ```
  - Lưu log cho thấy generator phát event, Redpanda nhận message, Spark xử lý
    và PostgreSQL có row trong `sensor_window_metrics`.

- [ ] **Trích xuất log/ảnh chụp chứng minh số lượng event đầu vào khớp với số
  lượng window output.**
  - Ghi số event đầu vào từ log generator hoặc consumer Kafka.
  - Ghi số window output từ PostgreSQL:
    ```powershell
    docker exec postgres psql -U admin -d app_db -c `
      "SELECT COUNT(*) AS window_count FROM sensor_window_metrics;"
    ```
  - Lưu câu lệnh, thời gian lấy số liệu và ảnh/log kết quả. Nếu một window
    gộp nhiều event, phải kèm tổng `event_count`:
    ```powershell
    docker exec postgres psql -U admin -d app_db -c `
      "SELECT COALESCE(SUM(event_count), 0) AS processed_event_count,
              COUNT(*) AS window_count
       FROM sensor_window_metrics;"
    ```
  - Đối chiếu `processed_event_count` với số event hợp lệ đã phát trong cùng
    khoảng thời gian; ghi rõ event late và dead-letter nếu không thuộc output
    window.

- [ ] **Xác nhận đường dẫn checkpoint thực tế nằm đúng bên trong thư mục bind
  mount.**
  - Kiểm tra mount:
    ```powershell
    docker inspect spark-processor --format `
      "{{range .Mounts}}{{println .Source \" -> \" .Destination}}{{end}}"
    ```
  - Kiểm tra checkpoint trong container:
    ```powershell
    docker exec spark-processor sh -c `
      "find /opt/workspace/checkpoints -maxdepth 3 -type f | sort | head -50"
    ```
  - Bằng chứng đạt: checkpoint nằm dưới
    `/opt/workspace/checkpoints`, không nằm riêng dưới `/app/checkpoints`.

- [ ] **Thực hiện dừng Spark/container và khởi động lại với checkpoint cũ.**
  - Ghi lại danh sách file checkpoint trước khi dừng:
    ```powershell
    Get-ChildItem .\checkpoints -Recurse |
      Select-Object FullName,Length,LastWriteTime
    ```
  - Dừng riêng Spark, không xóa volume/checkpoint:
    ```powershell
    docker compose stop stream-processor
    docker compose start stream-processor
    docker compose logs --tail 100 stream-processor
    ```
  - Xác nhận pipeline tiếp tục dùng checkpoint cũ và không tạo lại state từ
    đầu.

- [ ] **So sánh checksum và kiểm tra duplicate data trước và sau khi restart.**
  - Xuất dữ liệu ổn định theo thứ tự trước khi restart:
    ```powershell
    docker exec postgres psql -U admin -d app_db -At -F "|" -c `
      "SELECT window_start,window_end,device_id,event_count,
              average_temperature,average_humidity
       FROM sensor_window_metrics
       ORDER BY window_start,window_end,device_id;" `
      > .\logs\before_restart_rows.txt
    ```
  - Tạo checksum:
    ```powershell
    Get-FileHash .\logs\before_restart_rows.txt -Algorithm SHA256
    ```
  - Sau restart, chờ pipeline ổn định rồi xuất lại thành
    `.\logs\after_restart_rows.txt` và chạy `Get-FileHash`.
  - Kiểm tra duplicate theo khóa chính:
    ```powershell
    docker exec postgres psql -U admin -d app_db -c `
      "SELECT window_start, window_end, device_id, COUNT(*) AS duplicates
       FROM sensor_window_metrics
       GROUP BY window_start, window_end, device_id
       HAVING COUNT(*) > 1;"
    ```
  - Lưu checksum trước/sau, row count trước/sau và kết quả truy vấn duplicate.

- [ ] **Thực nghiệm kiểm thử Watermark với các gói dữ liệu:**
  - [ ] **Trong khoảng thời gian watermark:** event có `event_time` còn nằm
    trong vùng cho phép; xác nhận được xử lý vào window.
  - [ ] **Vừa vượt ngưỡng watermark:** event có `event_time` nhỏ hơn watermark
    một khoảng vừa đủ; xác nhận được ghi vào `logs\late_data.jsonl`.
  - [ ] **Trễ sâu hơn watermark:** event có `event_time` cũ rõ rệt; xác nhận
    bị route vào late-data/dead-letter theo chính sách và có lý do.
  - Với mỗi gói, lưu payload đầu vào, `event_time`, watermark thực tế,
    `watermark_cutoff`, batch id và kết quả PostgreSQL/log.

- [ ] **Xuất báo cáo chi tiết record nào được xử lý, record nào bị loại bỏ và
  lý do tương ứng.**
  - Lưu các file:
    ```text
    logs/raw_events.jsonl
    logs/accepted_events.jsonl
    logs/late_data.jsonl
    logs/dead_letter.jsonl
    ```
  - Mỗi record phải truy được tối thiểu `batch_id`, `device_id` hoặc
    `raw_payload`, `event_time` nếu parse được, trạng thái xử lý và lý do.
  - Tổng hợp số lượng:
    ```powershell
    Get-Content .\logs\raw_events.jsonl | Measure-Object -Line
    Get-Content .\logs\accepted_events.jsonl | Measure-Object -Line
    Get-Content .\logs\late_data.jsonl | Measure-Object -Line
    Get-Content .\logs\dead_letter.jsonl | Measure-Object -Line
    ```
  - Bằng chứng đạt khi tổng raw được phân loại đầy đủ thành accepted, late và
    dead-letter, không có record không xác định trạng thái.

- [ ] **Thống kê tỷ lệ late data thực tế sau một khoảng thời gian chạy giả
  lập đủ dài.**
  - Ghi thời điểm bắt đầu/kết thúc, tổng event và số event late:
    ```powershell
    $raw = (Get-Content .\logs\raw_events.jsonl).Count
    $late = (Get-Content .\logs\late_data.jsonl).Count
    [pscustomobject]@{
      raw_events = $raw
      late_events = $late
      late_ratio = if ($raw -gt 0) { $late / $raw } else { 0 }
    }
    ```
  - Lặp lại với thời lượng đủ dài, không kết luận từ một vài micro-batch.
  - Đính kèm cấu hình mục tiêu `late_data_percent: 0.20` và khoảng thời gian
    chạy để giải thích sai số ngẫu nhiên.

- [ ] **Bổ sung bài test tự động cho CSDL sink và cơ chế `ON CONFLICT`.**
  - Chạy:
    ```powershell
    .\.venv\Scripts\python.exe -m unittest `
      tests.test_smoke_pipeline.PostgreSQLIntegrationTests -v
    ```
  - Bằng chứng phải có insert thành công, ghi lại cùng khóa
    `(window_start, window_end, device_id)` và đọc lại giá trị đã được update,
    không tạo thêm dòng duplicate.
  - Nếu PostgreSQL chưa chạy, không ghi nhận PASS; phải khởi động dịch vụ và
    chạy lại test integration.

- [ ] **Cập nhật tập tệp môi trường/dependencies kiểm thử bao gồm
  `kafka-python`, `pyspark`, `psycopg2-binary`.**
  - Kiểm tra manifest:
    ```powershell
    Select-String -Path .\requirements.txt `
      -Pattern "kafka-python|pyspark|psycopg2-binary"
    ```
  - Cài đặt trong đúng virtual environment:
    ```powershell
    .\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
    ```
  - Xác minh import:
    ```powershell
    .\.venv\Scripts\python.exe -c `
      "import kafka, pyspark, psycopg2; print('dependencies: OK')"
    ```
  - Lưu kết quả `pip freeze` hoặc ảnh terminal cùng phiên bản Python đang dùng.
