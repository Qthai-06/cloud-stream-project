# Đặc Tả Cấu Hình Hệ Thống (Configurations Guide) — `configs/`

Thư mục `configs/` quản lý toàn bộ các tệp cấu hình tham số tĩnh, lược đồ cơ sở dữ liệu và cấu hình thu thập số liệu giám sát của dự án **Cloud Stream IoT**.

---

## 1. Cấu Trúc Thư Mục Cấu Hình

```text
configs/
├── pipeline_config.yaml        # Tham số cấu hình chính cho Kafka, Generator, Spark và Database
├── database_schema.sql         # Lược đồ DDL bảng đích PostgreSQL
├── prometheus.yml              # Cấu hình cào số liệu của Prometheus gốc
└── grafana/                    # Cấu hình provisioning tự động cho Grafana
    └── provisioning/
        ├── datasources/        # Khai báo datasource Prometheus tự động nạp
        └── dashboards/         # Khai báo đường dẫn nạp tự động các dashboard JSON
```

---

## 2. Chi Tiết Tệp `pipeline_config.yaml`

Tệp cấu hình trung tâm nạp bởi `src.utils.config_loader`:

```yaml
kafka:
  bootstrap_servers: "redpanda:29092"
  topic_name: "iot-sensor-events"

generator:
  rate_per_sec: 10
  late_data_percent: 0.20
  min_delay_sec: 30
  max_delay_sec: 120
  devices:
  - "sensor-A101"
  - "sensor-A102"
  - "sensor-A103"
  - "sensor-A104"
  - "sensor-A105"

streaming:
  window_duration: "1 minute"
  watermark_delay: "120 seconds"
  checkpoint_location: "checkpoints/spark_pipeline"

database:
  host: "127.0.0.1"
  port: 15432
  user: "admin"
  name: "app_db"
  password: "${POSTGRES_PASSWORD}"
  table: "sensor_window_metrics"
```

### Bảng Giải Thích Chi Tiết Từng Tham Số:

| Khối Cấu Hình | Tên Tham Số | Kiểu | Giá Trị Mặc Định | Ý Nghĩa Kỹ Thuật & Tác Động |
|---|---|:---:|:---:|---|
| **`kafka`** | `bootstrap_servers` | `string` | `"redpanda:29092"` | Địa chỉ mạng kết nối broker. Ghi đè bởi biến `KAFKA_BOOTSTRAP_SERVERS` trong Docker container. |
| | `topic_name` | `string` | `"iot-sensor-events"` | Tên topic Kafka tiếp nhận toàn bộ sự kiện cảm biến. |
| **`generator`** | `rate_per_sec` | `integer` | `10` | Tốc độ phát sự kiện danh định ($10$ msg/s). Thời gian ngủ giữa 2 thông điệp là $1.0 / 10 = 0.1$s. |
| | `late_data_percent` | `float` | `0.20` | Tỷ lệ xác suất ($20\%$) một sự kiện bị lùi thời gian để mô phỏng dữ liệu đến muộn. |
| | `min_delay_sec` | `integer` | `30` | Ngưỡng thời gian lùi tối thiểu ($30$ giây). |
| | `max_delay_sec` | `integer` | `120` | Ngưỡng thời gian lùi tối đa ($120$ giây). Độ trễ ngẫu nhiên phân bổ đều trong $[30, 120]$s. |
| | `devices` | `list` | 5 sensor IDs | Danh sách các thiết bị cảm biến công nghiệp tham gia phát dữ liệu. |
| **`streaming`** | `window_duration` | `string` | `"1 minute"` | Kích thước cửa sổ Tumbling Window cố định của Spark Structured Streaming ($60$ giây). |
| | `watermark_delay` | `string` | `"120 seconds"` | Ngưỡng trễ Watermark tối đa cho phép dung nạp sự kiện ($2$ phút). |
| | `checkpoint_location` | `string` | `"checkpoints/spark_pipeline"` | Đường dẫn tương đối lưu trữ trạng thái delta và commit offset của Spark. |
| **`database`** | `host` | `string` | `"127.0.0.1"` | Địa chỉ host của PostgreSQL. Trong container Docker được ghi đè thành `postgres`. |
| | `port` | `integer` | `15432` | Cổng kết nối host. Trong container Docker được ghi đè thành `5432`. |
| | `user` | `string` | `"admin"` | Tên tài khoản cơ sở dữ liệu. |
| | `name` | `string` | `"app_db"` | Tên cơ sở dữ liệu đích tiếp nhận kết quả. |
| | `password` | `string` | `"${POSTGRES_PASSWORD}"` | Mật khẩu truy cập. Bắt buộc nạp từ biến môi trường của tệp `.env`. |
| | `table` | `string` | `"sensor_window_metrics"` | Tên bảng đích lưu trữ kết quả phân tích cửa sổ. |

---

## 3. Lược Đồ Cơ Sở Dữ Liệu (`database_schema.sql`)

Tệp DDL định nghĩa bảng đích trong cơ sở dữ liệu PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS sensor_window_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    event_count BIGINT NOT NULL,
    average_temperature DOUBLE PRECISION,
    average_humidity DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_end, device_id)
);
```

### Đặc tính kỹ thuật của lược đồ:
1. `TIMESTAMPTZ` (Timestamp with time zone): Đảm bảo lưu trữ mốc thời gian chuẩn hóa có múi giờ UTC, ngăn chặn sai lệch giờ cục bộ giữa các hệ điều hành.
2. `PRIMARY KEY (window_start, window_end, device_id)`: Khóa chính tổng hợp 3 trường. Đảm bảo tính duy nhất tuyệt đối cho mỗi khung thời gian 1 phút của từng thiết bị.
3. `updated_at`: Tự động gán thời điểm hệ thống ghi nhận bản ghi (`DEFAULT CURRENT_TIMESTAMP`), hỗ trợ tính toán độ trễ đầu cuối $L_{E2E} = \text{updated\_at} - \text{window\_end}$.

---

## 4. Cấu Hình Prometheus Gốc (`configs/prometheus.yml`)

Tệp cấu hình cơ sở dữ liệu chuỗi thời gian cho container `prometheus` (cổng 19090):

```yaml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: cadvisor
    static_configs:
      - targets: ["cadvisor:8080"]

  - job_name: redpanda
    metrics_path: /public_metrics
    static_configs:
      - targets: ["redpanda:9644"]
```

### Phân tích cấu hình cào số liệu:
- `scrape_interval: 10s`: Định kỳ 10 giây một lần, Prometheus sẽ gửi yêu cầu HTTP GET để thu thập số liệu.
- `job_name: cadvisor`: Thu thập các chỉ số tài nguyên phần cứng cgroups (CPU, RAM, Disk I/O, Network) của tất cả container từ endpoint `cadvisor:8080/metrics`.
- `job_name: redpanda`: Thu thập các chỉ số thông lượng broker, phân vùng, partition commit log và consumer lag từ endpoint quản trị `redpanda:9644/public_metrics`.

---

## 5. Cơ Chế Ghi Đè Cấu Hình Bằng Biến Môi Trường (Environment Overrides)

Hàm `load_config()` trong `src.utils.config_loader` triển khai cơ chế ưu tiên biến môi trường (**Environment Variable Precedence**) theo thứ tự sau:

$$\text{Giá Trị Cuối Cùng} = \text{Biến Môi Trường (Docker)} \succ \text{Biến Môi Trường (.env)} \succ \text{Giá Trị Mặc Định Trong YAML}$$

### Danh sách các biến môi trường ghi đè được hỗ trợ:
- `KAFKA_BOOTSTRAP_SERVERS` $\rightarrow$ ghi đè `kafka.bootstrap_servers`
- `DB_HOST` hoặc `POSTGRES_HOST` $\rightarrow$ ghi đè `database.host`
- `DB_PORT` hoặc `POSTGRES_PORT` $\rightarrow$ ghi đè `database.port`
- `DB_USER` hoặc `POSTGRES_USER` $\rightarrow$ ghi đè `database.user`
- `DB_NAME` hoặc `POSTGRES_DB` $\rightarrow$ ghi đè `database.name`
- `DB_PASSWORD` hoặc `POSTGRES_PASSWORD` $\rightarrow$ ghi đè `database.password`
- `CHECKPOINT_LOCATION` $\rightarrow$ ghi đè `streaming.checkpoint_location`
- `CHECKPOINT_ROOT` $\rightarrow$ thiết lập thư mục gốc phân giải đường dẫn checkpoint bền vững.

