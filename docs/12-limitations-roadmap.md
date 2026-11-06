# Chuyên Đề 12: Hạn Chế Kỹ Thuật & Lộ Trình Phát Triển

## 1. Tổng Hợp Trạng Thái Triển Khai Hiện Tại (Implementation Status Audit)

Dưới đây là bảng đánh giá trung thực, khách quan và minh bạch về toàn bộ các thành phần kỹ thuật trong kho mã nguồn `cloud-stream-project`:

| Thành Phần Kỹ Thuật | Phân Loại Trạng Thái | Mô Tả Hiện Trạng Mã Nguồn & Vận Hành | Mức Độ Hoàn Thiện |
|---|:---:|---|:---:|
| **Event Generator** | **IMPLEMENTED** | Phát sự kiện chuẩn rate 10 msg/s, 20% simulated late data, xử lý đa tiến trình an toàn với KafkaProducer. | 100% |
| **Event Schema & Validator** | **IMPLEMENTED** | Kiểm thực strict typing, kiểm tra miền giá trị vật lý và múi giờ ISO-8601 UTC độc lập. | 100% |
| **Redpanda Broker & Console** | **IMPLEMENTED** | Container v24.3.8 và Console v2.4.3 hoạt động ổn định, healthcheck tự động, phân vùng theo `device_id`. | 100% |
| **PostgreSQL Sink & DDL** | **IMPLEMENTED** | Bảng `sensor_window_metrics` có khóa chính tổng hợp, câu lệnh UPSERT `ON CONFLICT DO UPDATE` chống trùng lặp. | 100% |
| **Spark Core Pipeline** | **IMPLEMENTED** | Structured Streaming tumbling window 1 phút, watermark 120s, foreachBatch với `PsycopgConnectionPool`. | 95% |
| **Late Data Router** | **PARTIAL** | Script `late_data_router.py` phân tầng 4 tệp JSONL qua `WatermarkProgressTracker`; chưa tích hợp vào Compose chính. | 75% |
| **Batch Evaluator** | **IMPLEMENTED** | Script `batch_evaluator.py` thẩm định cân bằng sự kiện $N_{unclassified} == 0$ và kiểm tra duplicate DB. | 90% |
| **Spark Checkpointing** | **IMPLEMENTED** | Checkpoint bind mount bền vững ra ổ đĩa host, khôi phục offset chính xác khi restart. | 100% |
| **SRE Disaster Recovery** | **IMPLEMENTED** | Bộ 4 scripts PowerShell trong `ops/` sao lưu custom format, checksum SHA-256, drill đo thời gian < 300s. | 100% |
| **Test Suites** | **IMPLEMENTED** | 4 lớp kiểm thử trong `test_smoke_pipeline.py` và kịch bản `run_integrity_check.py` đạt 100% PASS. | 90% |
| **Monitoring Gốc** | **PARTIAL** | Prometheus v2.54.1 và Grafana 11.2.0 chạy cổng 19090/13300; chưa cào metrics từ Spark processor. | 60% |
| **Monitoring NhutTan** | **IMPLEMENTED** | Compose riêng tại cổng 9090/3000, 3 dashboard JSON hoàn chỉnh, tự động provisioning. | 90% |
| **Spark Metrics Exporter** | **SIMULATED** | `start_live_metrics_server.py` phát số liệu giả lập 4 pha phục vụ demo; chưa nối trực tiếp JVM JMX của Spark. | 40% |
| **Benchmark Shell Scripts** | **PLACEHOLDER** | `run_partition_test.sh` và `run_watermark_test.sh` mới chỉ có 2 dòng chú thích, chưa có mã thực thi. | 10% |
| **ClickHouse Storage Sink** | **PLANNED** | Thư mục `volumes/clickhouse` tồn tại nhưng dịch vụ không có trong `docker-compose.yml` và pipeline. | 15% |

---

## 2. Các Lỗi & Vấn Đề Cấu Hình Đã Biết (Known Issues & Configuration Gotchas)

1. **Vấn đề phân quyền thư mục Checkpoint của non-root user**:
   - Container `spark-processor` chạy với user `spark` (UID 1001) để giảm attack surface.
   - Khi chạy trên Windows Docker Desktop với volume bind-mount `./checkpoints`, quyền sở hữu tệp tin đôi khi bị ánh xạ thành `root:root`, khiến tiến trình Spark bị lỗi `java.io.IOException: Permission denied`.
   - *Biện pháp hiện tại*: Cần cấp quyền trước bằng `chown -R spark:spark /opt/workspace` hoặc cấu hình Docker file sharing.
2. **Sự cố Leaderless Partitions trên Redpanda khi tắt cưỡng bức**:
   - Khi chạy lệnh `docker compose kill` hoặc khởi động lại máy đột ngột, Redpanda có thể mất trạng thái đồng thuận Raft leader của topic `iot-sensor-events`.
   - Tiến trình Spark tiếp theo sẽ bị treo ở trạng thái chờ phân vùng gán (`Timed out waiting for a node assignment`).
3. **Mâu thuẫn cổng giữa hai cụm Monitoring**:
   - Việc duy trì đồng thời cụm Prometheus gốc (cổng 19090) và cụm Prometheus NhutTan (cổng 9090) dễ gây hiểu lầm cho người vận hành nếu không chỉ định rõ ngữ cảnh.

---

## 3. Giới Hạn Kỹ Thuật (Technical Limitations)

1. **Thao tác Gom Nhóm Cục Bộ Tại Driver (`batch_df.collect()`)**:
   - Trong `write_batch()` của `spark_pipeline.py`, Spark nạp toàn bộ kết quả tổng hợp của micro-batch về Driver thông qua lệnh `.collect()`.
   - Mô hình này hoạt động hoàn hảo trong phòng thí nghiệm với 5 cảm biến ($\le 5$ dòng/phút), nhưng sẽ trở thành điểm nghẽn nghiêm trọng (OOM Driver Crash) nếu quy mô mở rộng lên hàng trăm nghìn cảm biến.
2. **Đơn Luồng Tiêu Thụ Trong Môi Trường Local**:
   - Spark chạy ở chế độ `local[2]` với 2 core tính toán. Khi tốc độ nạp vượt quá 200 msg/s, Spark không thể tự động co giãn thêm tài nguyên để xả lag nếu không được cấu hình cụm đa node.
3. **Chưa Tích Hợp Schema Registry Tập Trung**:
   - Các định nghĩa Schema được chia sẻ thủ công qua mã nguồn Python (`event_schema.py`). Thiếu một hệ thống quản lý hợp đồng phiên bản như Confluent Schema Registry hoặc Apicurio.

---

## 4. Bảo Mật & Quản Trị Bí Mật (Security & Secrets Management)

1. **Mật khẩu cơ sở dữ liệu mặc định**:
   - Dự án đã có cơ chế phòng vệ chặn khởi chạy nếu thiếu biến `${POSTGRES_PASSWORD:?}`, tuy nhiên tệp `.env.example` và tài liệu vẫn còn lưu các chuỗi mật khẩu gợi ý dễ đoán.
2. **Thiếu mã hóa truyền tải nội bộ (No In-Transit TLS/SSL)**:
   - Toàn bộ giao tiếp mạng giữa Generator $\leftrightarrow$ Redpanda $\leftrightarrow$ Spark $\leftrightarrow$ PostgreSQL đều sử dụng kênh truyền bản rõ (`PLAINTEXT` / unencrypted TCP), chỉ an toàn trong mạng ảo Docker cô lập, không đáp ứng tiêu chuẩn an toàn cho môi trường sản xuất.
3. **Grafana Anonymous Viewer**:
   - Cấu hình cụm giám sát `NhutTan` cho phép người dùng ẩn danh (anonymous access) xem dashboard để thuận tiện demo, tiềm ẩn nguy cơ lộ thông tin hạ tầng nếu vô tình mở cổng ra Internet.

---

## 5. Đánh Giá Khả Năng Sẵn Sàng Lên Đám Mây (Cloud-Native Readiness)

| Tiêu Chí Sẵn Sàng | Trạng Thái Hiện Tại (Local Docker) | Khoảng Thiếu Hụt Để Lên Cloud (Gap Analysis) |
|---|:---:|---|
| **Điều phối tiến trình** | Docker Compose v2 (Đơn node) | Cần chuyển đổi sang Kubernetes Manifests hoặc Helm Charts. |
| **Khả năng tự động co giãn (Autoscaling)** | Không hỗ trợ (Tài nguyên tĩnh) | Cần tích hợp KEDA (Kubernetes Event-driven Autoscaling) để co giãn Pod dựa trên Kafka Consumer Lag. |
| **Lưu trữ trạng thái bền vững** | Bind mount ổ đĩa máy chủ | Cần chuyển sang S3/GCS Object Storage kết hợp cơ chế RocksDB State Store của Spark. |
| **Dịch vụ Database** | PostgreSQL container đơn | Cần chuyển sang dịch vụ Managed Cloud (Amazon Aurora / Google Cloud SQL) có Multi-AZ và Read Replicas. |
| **Giám sát & Quản lý Log** | Prometheus cục bộ & JSONL files | Cần tích hợp OpenTelemetry Collector đẩy logs và metrics về Datadog / Grafana Cloud / CloudWatch. |

---

## 6. Lộ Trình Phát Triển Ưu Tiên (Prioritized Roadmap)

Bảng lộ trình kỹ thuật được phân loại theo mức độ ưu tiên từ cao xuống thấp:

| Mức Độ Ưu Tiên | Hạng Mục Kỹ Thuật | Lý Do & Giá Trị Đem Lại | Nỗ Lực (Effort) | Trạng Thái Hiện Tại |
|:---:|---|---|:---:|:---:|
| **P0 (Bắt buộc)** | **Hiện thực hóa Spark Metrics Exporter thực tế** | Thay thế server số liệu giả lập (`start_live_metrics_server.py`) bằng Prometheus JMX Exporter hoặc Spark MetricSource listener thực gắn trực tiếp vào JVM container để xuất bản throughput và lag thực tế. | Vừa (2-3 ngày) | **PLANNED** |
| **P0 (Bắt buộc)** | **Đo lường phân bổ độ trễ P95/P99 chính thức** | Bổ sung hàm đo lường tự động độ trễ từ lúc cảm biến phát sinh (`event_time`) đến khi ghi vào database (`updated_at`), xuất phân bổ phân vị P50, P95, P99 lên Grafana. | Nhỏ (1 ngày) | **PLANNED** |
| **P1 (Quan trọng)** | **Hiện thực hóa kịch bản Benchmark tự động** | Viết mã hoàn chỉnh cho hai tệp placeholder `run_partition_test.sh` và `run_watermark_test.sh`, tự động thay đổi cấu hình, đo đạc và xuất kết quả ra CSV. | Vừa (2 ngày) | **PLACEHOLDER** |
| **P1 (Quan trọng)** | **Tích hợp kho dữ liệu cột ClickHouse** | Bổ sung container ClickHouse vào `docker-compose.yml`, viết connector hoặc Kafka engine table để biến ClickHouse thành OLAP sink song song với PostgreSQL. | Lớn (4-5 ngày) | **PLANNED** |
| **P2 (Nâng cao)** | **Tối ưu hóa ghi phân tán `foreachPartition`** | Thay thế `batch_df.collect()` trong `write_batch()` bằng `foreachPartition` để cho phép các Worker ghi song song trực tiếp vào Database, chuẩn bị cho tải lớn. | Vừa (2 ngày) | **PLANNED** |
| **P2 (Nâng cao)** | **Xây dựng Sổ tay Triển khai Cloud (Cloud Guide)** | Cung cấp tài liệu hướng dẫn triển khai hệ thống lên AWS EMR / MSK hoặc GCP Dataproc / PubSub với ước tính chi phí hạ tầng thực tế. | Lớn (3-4 ngày) | **PLANNED** |
| **P3 (Hoàn thiện)** | **Tích hợp Confluent Schema Registry & Avro** | Thay thế JSON schema thuần túy bằng chuẩn hóa Avro nhị phân để giảm 60% băng thông mạng và hỗ trợ tiến hóa schema tương thích ngược (Backward Compatibility). | Lớn (4 ngày) | **PLANNED** |

---

## 7. Đánh Giá Kỹ Thuật Tổng Thể (Overall Technical Verdict)

Dự án **Cloud Stream IoT (Đề tài 16)** là một hệ thống thực nghiệm xử lý luồng có nền tảng kiến trúc vững chắc, tuân thủ các nguyên lý thiết kế hệ thống phân tán hiện đại. 

### Điểm mạnh nổi bật:
- Triển khai chuẩn hóa, sạch sẽ và có tính lặp lại cao qua Docker Compose.
- Giải quyết xuất sắc bài toán xử lý sự kiện đến muộn thông qua cơ chế Tumbling Window và Watermark.
- Đạt được ngữ nghĩa xử lý tương đương **Effectively Exactly-Once** tại tầng lưu trữ nhờ kỹ thuật UPSERT Idempotency kết hợp Checkpoint.
- Quy trình vận hành, diễn tập thảm họa (Disaster Recovery Drill) đạt tiêu chuẩn SRE chuyên nghiệp với RTO đo đạc ấn tượng (~19.5s).
- Hệ thống kiểm thử hồi quy 4 cấp độ bao phủ chặt chẽ logic nghiệp vụ.

### Điểm cần hoàn thiện để nâng tầm công nghiệp:
- Thay thế hoàn toàn các thành phần mô phỏng số liệu bằng exporter runtime thực tế.
- Tự động hóa các kịch bản đo tải benchmark còn đang ở dạng placeholder.
- Mở rộng tầng phân tích OLAP với ClickHouse và chuẩn bị hạ tầng co giãn Cloud-Native.

