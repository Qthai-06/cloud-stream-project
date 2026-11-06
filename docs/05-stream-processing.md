# Chuyên Đề 05: Xử Lý Luồng Với Spark Structured Streaming

## 1. Tổng Quan Spark Structured Streaming

**Apache Spark Structured Streaming** là động cơ xử lý luồng có khả năng chịu lỗi và mở rộng cao, được xây dựng trên nền tảng tối ưu hóa truy vấn Spark SQL (Catalyst Optimizer) và thực thi qua mô hình **Micro-batch Processing**. 

Trong dự án **Cloud Stream IoT**, Structured Streaming đóng vai trò "trái tim" tính toán, chịu trách nhiệm liên tục quét các phân vùng Redpanda, giải mã gói tin JSON, áp dụng cơ chế cửa sổ thời gian sự kiện (**Event-Time Windowing**), điều phối ngưỡng trễ (**Watermark**) và cập nhật kết quả tổng hợp vào PostgreSQL.

---

## 2. Cấu Hình Nguồn Dữ Liệu Kafka (Kafka Source Configuration)

Khối đọc luồng được khởi tạo tại hàm `main()` của [`src/streaming/spark_pipeline.py`](../src/streaming/spark_pipeline.py):

```python
events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka["bootstrap_servers"])
    .option("subscribe", kafka["topic_name"])
    .option("startingOffsets", "earliest")
    .load()
)
```

### Phân tích các tham số then chốt:
1. `kafka.bootstrap.servers`: Trong môi trường container, biến môi trường `KAFKA_BOOTSTRAP_SERVERS` trỏ tới `redpanda:29092`.
2. `subscribe`: Đăng ký lắng nghe topic `iot-sensor-events`.
3. `startingOffsets: earliest`: 
   - **Khi khởi chạy lần đầu tiên (hoặc khi thư mục checkpoint rỗng)**: Spark sẽ đọc từ offset số 0 của toàn bộ các partition, đảm bảo không bỏ sót bất kỳ thông điệp nào đã được sinh ra trước đó.
   - **Khi khởi động lại từ checkpoint hiện có**: Tham số `earliest` sẽ tự động bị ghi đè bởi offset cuối cùng đã được cam kết an toàn trong tệp trạng thái checkpoint của Spark.

---

## 3. Phân Tích Cú Pháp, Ép Kiểu & Schema Enforcement

Sau khi nạp dòng dữ liệu thô nhị phân từ Kafka, Spark thực hiện chuyển đổi và lọc phòng vệ:

```python
.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
.select("event.*")
.withColumn("event_time", col("event_time").cast("timestamp"))
.filter(
    col("device_id").isNotNull()
    & col("event_time").isNotNull()
    & col("temperature").isNotNull()
    & col("humidity").isNotNull()
)
```

### Cơ chế kiểm soát chất lượng dữ liệu:
- Gói tin nhị phân `value` được chuyển thành chuỗi UTF-8.
- Hàm `from_json()` áp dụng cấu trúc tĩnh `EVENT_SCHEMA` (`StructType`). Mọi trường không hợp lệ sẽ được chuyển thành giá trị `NULL`.
- `event_time` được ép kiểu từ chuỗi ISO-8601 sang kiểu dữ liệu `TimestampType` chuẩn của Spark SQL, đồng thời Spark thiết lập múi giờ phiên làm việc đồng nhất là `UTC` (`spark.sql.session.timeZone = "UTC"`).
- Khối `.filter()` hoạt động như một tầng van chắn (Guard Rail), loại bỏ ngay lập tức các dòng hỏng cấu trúc trước khi đưa vào bộ nhớ trạng thái tính toán.

---

## 4. Ngữ Nghĩa Cửa Sổ Tumbling Window & Ngưỡng Watermark

Khối tổng hợp logic được hiện thực tại hàm `build_windowed_events()`:

```python
def build_windowed_events(events: DataFrame, streaming: dict[str, Any]) -> DataFrame:
    return (
        events.withWatermark("event_time", streaming["watermark_delay"])
        .groupBy(
            window(col("event_time"), streaming["window_duration"]),
            col("device_id"),
        )
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("average_temperature"),
            avg("humidity").alias("average_humidity"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id",
            "event_count",
            "average_temperature",
            "average_humidity",
        )
    )
```

### 4.1. Cửa Sổ Cố Định Không Chồng Lấn (Tumbling Window)
- Tham số: `window_duration: "1 minute"` (60 giây).
- Các cửa sổ được phân chia theo biên thời gian tự nhiên: `[14:00:00, 14:01:00)`, `[14:01:00, 14:02:00)`.
- Một sự kiện chỉ thuộc về duy nhất một cửa sổ dựa trên mốc `event_time`.

### 4.2. Cơ Chế Hoạt Động Của Watermark
- Tham số cấu hình: `watermark_delay: "120 seconds"` (2 phút).
- **Nguyên lý tính toán**: Tại mỗi micro-batch kết thúc, Spark xác định giá trị thời gian sự kiện lớn nhất từng quan sát được qua công thức:
  $$T_{watermark} = \max(\text{event\_time}) - 120\text{s}$$

```mermaid
gantt
    title Minh Họa Sự Tiến Triển Của Watermark Và Cửa Sổ Đóng
    dateFormat X
    axisFormat %s
    section Sự Kiện Đến
    Event E1 (t=14:00:15) :active, e1, 15, 16
    Event E2 (t=14:03:00) :active, e2, 180, 181
    section Watermark
    Watermark tại E1 (t < 0) :milestone, wm1, 0, 0
    Watermark tại E2 (14:03:00 - 120s = 14:01:00) :milestone, wm2, 60, 60
    section Trạng Thái Window
    Window [14:00 - 14:01] (ĐANG MỞ) :done, w1, 0, 60
    Window [14:00 - 14:01] (CHÍNH THỨC ĐÓNG) :crit, w1_closed, 60, 61
    Late Event E3 (t=14:00:45 đến tại giây 190) :crit, e3, 190, 191
```

- **Quy tắc cắt bỏ (Drop Rule)**: Khi $T_{watermark}$ vượt qua mốc kết thúc của một cửa sổ (`window_end`), cửa sổ đó được coi là **đã đóng hoàn toàn**. Mọi bản ghi đến sau có $t_{event} < T_{watermark}$ sẽ bị Spark tự động loại bỏ khỏi việc tính toán tổng hợp, giải phóng bộ nhớ RAM của trạng thái lưu trữ (State Store).

---

## 5. Chế Độ Xuất Bản Dữ Liệu (Output Mode) & foreachBatch

Trong Spark Structured Streaming, các truy vấn tổng hợp có watermark hỗ trợ hai chế độ xuất: **Append** và **Update**.

### Tại sao dự án chọn `outputMode("update")`?
- **Chế độ Append**: Chỉ phát dữ liệu của một cửa sổ khi cửa sổ đó đã đóng hoàn toàn (khi watermark vượt qua `window_end`). Nhược điểm: Phải chờ ít nhất $60s + 120s = 180s$ (3 phút) mới thấy kết quả đầu tiên tại database, gây độ trễ hiển thị rất lớn.
- **Chế độ Update**: Mỗi khi có dữ liệu mới làm thay đổi giá trị trung bình hoặc số lượng của cửa sổ (kể cả khi cửa sổ chưa đóng), Spark sẽ phát ngay dòng dữ liệu cập nhật đó trong micro-batch hiện tại ra sink.
- **Kết hợp với foreachBatch**: Cho phép can thiệp bằng mã nguồn Python tùy biến để ghi kết quả vào PostgreSQL:

```python
query = (
    build_windowed_events(events, streaming)
    .writeStream.outputMode("update")
    .option("checkpointLocation", str(resolve_checkpoint_path(checkpoint_location)))
    .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, pool))
    .start()
)
```

---

## 6. Điều Phối Kết Nối PostgreSQL Qua PsycopgConnectionPool

Việc khởi tạo kết nối cơ sở dữ liệu bên trong hàm `foreachBatch` nếu không được kiểm soát sẽ dẫn đến việc mở hàng trăm socket TCP khi có nhiều batch liên tục. Để khắc phục, dự án triển khai lớp `PsycopgConnectionPool`:

```python
class PsycopgConnectionPool:
    def __init__(self, database: dict[str, Any], minconn: int = 1, maxconn: int = 4) -> None:
        self.table = database["table"]
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn,
            maxconn,
            host=database["host"],
            port=database["port"],
            dbname=database["name"],
            user=database["user"],
            password=database["password"],
        )
```

### Hàm thực thi ghi batch (`write_batch`):
- Sử dụng lệnh `batch_df.collect()` để nạp kết quả đã tổng hợp của micro-batch về phía Driver.
- Lấy một kết nối rỗi từ pool qua `pool.getconn()`.
- Thực thi ghi đồng loạt qua `cursor.executemany()` với câu lệnh SQL `ON CONFLICT DO UPDATE`.
- Trả kết nối về pool trong khối lệnh `finally: pool.putconn(connection)`.

---

## 7. Cơ Chế Checkpoint Bền Vững & Phân Giải Đường Dẫn

Để đảm bảo khả năng phục hồi sau thảm họa, vị trí checkpoint phải được ghi xuống ổ đĩa bền vững thay vì lưu trữ tạm thời trong bộ nhớ container.

### Hàm phân giải đường dẫn (`resolve_checkpoint_path`):
```python
def resolve_checkpoint_path(configured_path: str) -> Path:
    path = Path(configured_path)
    if path.is_absolute():
        return path
    checkpoint_root = Path(
        os.getenv("CHECKPOINT_ROOT", str(PROJECT_ROOT / "checkpoints"))
    )
    return checkpoint_root / path.name
```
- Khi chạy cục bộ trên máy host (ví dụ khi chạy Unit Test): Đường dẫn tương đối `checkpoints/spark_pipeline` được phân giải thành `F:\Python\cloud-stream-project\checkpoints\spark_pipeline`.
- Khi chạy trong container Docker: Biến môi trường `CHECKPOINT_ROOT=/opt/workspace/checkpoints` phân giải thành `/opt/workspace/checkpoints/spark_pipeline`, trỏ trực tiếp vào volume bind mount.

### Cấu trúc nội bộ thư mục Checkpoint của Spark:
- `offsets/`: Lưu danh sách các offset của từng partition Kafka được xử lý tại mỗi batch ID.
- `commits/`: Ghi nhận nhật ký batch đã hoàn tất thành công.
- `state/0/0/`: Chứa các tệp delta trạng thái (`.delta`) và snapshot trạng thái (`.snapshot`) của bộ nhớ tổng hợp cửa sổ.
- `metadata`: Chứa ID duy nhất của Streaming Query.

---

## 8. So Sánh Lý Thuyết vs Thực Tế Cài Đặt (Theory vs. Implementation)

| Khía Cạnh | Lý Thuyết Chuẩn Của Spark | Cài Đặt Thực Tế Trong Dự Án | Đánh Giá Kỹ Thuật |
|---|---|---|---|
| **Cơ chế Sink** | Sử dụng Spark JDBC Sink có sẵn (`format("jdbc")`) | Tự cài đặt `foreachBatch` kết hợp `PsycopgConnectionPool` | **Vượt trội**: Khắc phục nhược điểm JDBC sink không hỗ trợ câu lệnh `ON CONFLICT DO UPDATE` của PostgreSQL. |
| **Xử lý Dữ liệu muộn** | Spark tự động drop im lặng (silent drop) không ghi vết | Xây dựng riêng `late_data_router.py` sử dụng `WatermarkProgressTracker` để lưu vết 4 tệp JSONL | **Chặt chẽ**: Cung cấp bằng chứng kiểm toán (Audit Trail) phục vụ đối chiếu, không để mất dấu dữ liệu. |
| **Tính toán Watermark** | Spark tính toán ngầm định bên trong JVM | Trích xuất watermark từ `StreamingQueryProgress.eventTime["watermark"]` | **Chính xác**: Giữ vững tính khách quan của hệ thống, không tự tính toán giả lập ngoài tiến trình. |
| **Số luồng thực thi** | Cụm phân tán nhiều Executor trên Kubernetes/YARN | Chạy `master("local[2]")` với 2 core trong 1 container | **Phù hợp mục tiêu**: Đủ để đo lường bão hòa CPU và backpressure trong môi trường phòng lab. |

---

## 9. Giới Hạn Của Tầng Xử Lý (Processing Limitations)

1. **Thao tác `collect()` về Driver trong `write_batch()`**:
   - `write_batch` sử dụng `batch_df.collect()` để lấy dữ liệu về Driver trước khi ghi vào Database.
   - Khi tốc độ sự kiện ở mức bình thường (10 - 100 msg/s), số lượng dòng tổng hợp theo cửa sổ mỗi phút là rất nhỏ ($\le 5$ dòng cho 5 cảm biến), việc `collect()` diễn ra cực kỳ nhanh chóng.
   - Tuy nhiên, nếu mở rộng lên $100,000$ cảm biến, việc `collect()` sẽ gây nghẽn bộ nhớ tại Driver (Driver Out-of-Memory). Khi đó bắt buộc phải chuyển sang kỹ thuật ghi phân tán qua `foreachPartition`.
2. **Xung đột Hadoop Winutils trên Windows**:
   - Khi chạy unit test trên Windows gốc mà không có Docker, Spark Structured Streaming micro-batch có thể bị lỗi liên quan đến `winutils.exe` hoặc thư viện Hadoop native. Dự án đã xử lý bằng cách thêm `@unittest.skipIf(os.name == 'nt')` cho các bài test micro-batch socket, trong khi các bài test logic DataFrame vẫn chạy đầy đủ.

