# Tuần 1

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
clickhouse-driver==0.2.7

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

