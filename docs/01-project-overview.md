# Chuyên Đề 01: Tổng Quan Dự Án & Động Lực Nghiên Cứu

## 1. Bối Cảnh & Động Lực Nghiên Cứu

Trong kỷ nguyên Internet vạn vật (Internet of Things - IoT) và hệ thống công nghiệp thông minh (Industry 4.0), hàng triệu cảm biến biên (edge sensors) liên tục thu thập và phát tán dữ liệu đo lường môi trường (nhiệt độ, độ ẩm, áp suất, độ rung). Các luồng dữ liệu này đòi hỏi khả năng xử lý thời gian thực (real-time stream processing) nhằm phát hiện kịp thời các bất thường vận hành, cảnh báo cháy nổ, bảo trì dự đoán và kiểm soát chuỗi cung ứng.

Tuy nhiên, trong môi trường vật lý thực tế, hạ tầng mạng truyền thông không dây (4G/5G, LoRaWAN, Wi-Fi công nghiệp) thường xuyên đối mặt với các vấn đề:
1. **Chập chờn đường truyền và mất kết nối ngắt quãng**: Thiết bị cảm biến phải lưu đệm cục bộ (local buffer) và gửi dồn dập (burst transmission) khi kết nối phục hồi.
2. **Độ trễ truyền dẫn không đồng nhất (Network Jitter)**: Các gói tin di chuyển theo các tuyến mạng khác nhau, dẫn đến hiện tượng dữ liệu đến nơi tiếp nhận không theo đúng thứ tự thời gian phát sinh (**Out-of-order Data**).
3. **Dữ liệu đến muộn (Late Data)**: Các bản ghi xuất hiện tại hệ thống xử lý trung tâm sau khi khung thời gian phân tích dự kiến đã trôi qua.

Nếu áp dụng mô hình xử lý theo thời gian tiếp nhận (**Processing-Time**), hệ thống sẽ gán dữ liệu vào sai thời điểm thực tế xảy ra sự kiện, làm méo mó nghiêm trọng các chỉ số thống kê trung bình, tổng hợp và phân tích xu hướng. Ngược lại, nếu xử lý theo thời gian phát sinh sự kiện (**Event-Time**), hệ thống buộc phải duy trì trạng thái tích lũy (stateful accumulation) và thiết lập cơ chế ngưỡng cắt thời gian (**Watermark**) để quyết định khi nào đóng một khung thời gian (**Window**) và phát ra kết quả.

Dự án **Cloud Stream IoT (Đề tài 16)** được thiết kế và triển khai như một hệ thống thực nghiệm có kiểm soát. Hạ tầng được cấu hình chủ đích với các giới hạn tài nguyên tính toán (CPU, RAM, buffer) nhằm tái hiện chính xác các hiện tượng suy giảm hiệu năng, áp lực ngược (**Backpressure**), dồn ứ thông điệp (**Consumer Lag**) và đánh giá năng lực chịu lỗi khi xảy ra sự cố phần cứng hoặc tiến trình.

---

## 2. Phát Biểu Vấn Đề (Problem Statement)

Hệ thống xử lý phân tán theo luồng phải giải quyết sự đánh đổi (trade-off) cơ bản giữa ba yếu tố then chốt:

$$\text{Trade-off} = f(\text{Độ Trễ / Latency}, \text{Độ Chính Xác / Completeness}, \text{Chi Phí Tài Nguyên / State Memory})$$

- Nếu thiết lập **Watermark quá ngắn**: Hệ thống phản hồi kết quả phân tích nhanh (độ trễ thấp), nhưng tỷ lệ dữ liệu bị coi là "đến muộn" và bị loại bỏ (**Dropped / Late Data**) sẽ tăng cao, làm suy giảm độ chính xác của số liệu tổng hợp.
- Nếu thiết lập **Watermark quá dài**: Hệ thống dung nạp được nhiều dữ liệu trễ hơn (độ chính xác cao), nhưng bộ nhớ trạng thái (**State Store**) phải phình to để giữ các cửa sổ mở, đồng thời độ trễ hoàn tất của kết quả phân tích bị kéo dài.
- Khi tải đầu vào tăng đột biến (Traffic Spike): Tiến trình xử lý Spark không kịp tiêu thụ dữ liệu dẫn đến **Consumer Lag** tăng lũy tiến, kích hoạt hiện tượng **Backpressure** dồn ngược lại Message Broker.
- Khi xảy ra lỗi crash dịch vụ: Cơ chế **Checkpoint** phải đảm bảo khôi phục trạng thái chính xác mà không gây ra trùng lặp kết quả (Duplication) hoặc mất mát dữ liệu (Data Loss) tại tầng lưu trữ đích.

---

## 3. Câu Hỏi Nghiên Cứu (Research Questions)

Dự án tập trung trả lời câu hỏi cốt lõi được định nghĩa trong đề tài:

> **"Kích thước Window, ngưỡng trễ Watermark, tần suất Checkpoint và cơ chế giao nhận thông điệp ảnh hưởng thế nào đến độ trễ đầu cuối và độ chính xác của kết quả khi dữ liệu đến muộn?"**

Câu hỏi tổng quát này được phân rã thành các câu hỏi nghiên cứu cụ thể:

1. **RQ1 (Ảnh hưởng của Watermark)**: Khi thay đổi ngưỡng Watermark ($30s, 60s, 120s, 300s$) trong điều kiện 20% dữ liệu bị trễ từ 30s đến 120s, tỷ lệ bản ghi được gán đúng cửa sổ thay đổi ra sao? Khối lượng dữ liệu bị định tuyến vào luồng Late-Data log là bao nhiêu?
2. **RQ2 (Độ trễ đầu cuối - End-to-End Latency)**: Độ trễ tính từ thời điểm cảm biến tạo bản ghi ($t_{event}$) đến khi kết quả tổng hợp cửa sổ được ghi bền vững vào cơ sở dữ liệu ($t_{db}$) phân bổ như thế nào qua các giai đoạn tải bình thường và ép tải?
3. **RQ3 (Hành vi Backpressure & Consumer Lag)**: Điểm bão hòa tài nguyên tính toán (Bottleneck) nằm ở thành phần nào khi tốc độ bơm dữ liệu tăng từ 10 msg/s lên 500 msg/s? Spark xử lý thế nào khi thời gian thực thi micro-batch vượt quá trigger interval?
4. **RQ4 (Tính toàn vẹn & Phục hồi sau sự cố)**: Khi container Spark hoặc PostgreSQL bị buộc dừng (kill crash), cơ chế Spark Checkpoint kết hợp cùng PostgreSQL Idempotent Upsert có bảo toàn được ngữ nghĩa giao nhận tương đương Exactly-Once hay không? Thời gian phục hồi (Recovery Time Objective - RTO) có đạt ngưỡng mục tiêu dưới 300 giây hay không?

---

## 4. Mục Tiêu Dự Án (Project Objectives)

1. **Xây dựng Pipeline Stream Processing end-to-end hoàn chỉnh**:
   - Tầng thu nhận (Ingestion): Event Generator sinh dữ liệu giả lập IoT theo schema nghiêm ngặt, có nhãn kiểm chứng dữ liệu trễ.
   - Tầng truyền tải (Broker): Redpanda phân vùng dữ liệu theo `device_id`.
   - Tầng xử lý (Stream Processing): Apache Spark Structured Streaming thực hiện cửa sổ trượt Tumbling Window 1 phút, Watermark 120 giây và lưu checkpoint bền vững.
   - Tầng lưu trữ (Storage Sink): PostgreSQL tiếp nhận kết quả cửa sổ qua kỹ thuật Idempotent Upsert.
2. **Thiết lập hệ thống Giám sát & Đo lường (Observability)**:
   - Thu thập chỉ số tài nguyên phần cứng container qua cAdvisor và Prometheus.
   - Trực quan hóa các chỉ số hiệu năng luồng (Throughput, Consumer Lag, Batch Duration, Checkpoint Duration, Memory) qua Grafana Dashboards.
3. **Thực thi các kịch bản thực nghiệm định lượng**:
   - Đo lường baseline ở tốc độ tiêu chuẩn 10 msg/s.
   - Ép tải đa pha (Multi-stage Stress Test) xác định điểm nghẽn cổ chai.
   - Diễn tập khôi phục sau sự cố (Disaster Recovery Drill) đo đạc tính toàn vẹn dữ liệu.

---

## 5. Phạm Vi Dự Án (Project Scope)

### 5.1. Nằm trong phạm vi (In-Scope)
- Dữ liệu cảm biến IoT môi trường: Đo đạc 2 trường thực nghiệm chính là nhiệt độ ($20^\circ C - 85^\circ C$) và độ ẩm ($30\% - 90\%$).
- Kiến trúc triển khai cục bộ (Local Containerized Environment) trên nền tảng Docker Desktop và Docker Compose v2.
- Xử lý phân tích cửa sổ cố định không chồng lấn (**Tumbling Window**) kích thước 1 phút.
- Ngưỡng cắt dữ liệu trễ (**Watermark**) mặc định 120 giây.
- Kỹ thuật ghi nhận dữ liệu đích: Idempotent Upsert vào bảng PostgreSQL `sensor_window_metrics`.
- Lưu vết kiểm toán dữ liệu trễ qua 4 tầng tệp JSONL (`raw`, `accepted`, `late`, `dead_letter`).
- Kịch bản diễn tập khôi phục và kiểm tra toàn vẹn bằng PowerShell và SQL.

### 5.2. Nằm ngoài phạm vi (Out-of-Scope)
- Triển khai phân tán trên hạ tầng Cloud công cộng (AWS EMR, Google Cloud Dataproc, Azure HDInsight).
- Tích hợp kho dữ liệu dạng cột ClickHouse vào đường ống xử lý chính (thư mục `volumes/clickhouse` chỉ là thành phần dự kiến mở rộng trong tương lai).
- Các thuật toán học máy dự đoán chuỗi thời gian (Time-series Machine Learning / AI Anomaly Detection).
- Mã hóa dữ liệu TLS/SSL nội bộ giữa các container trong môi trường mạng nội bộ `stream-network`.

---

## 6. Yêu Cầu Hệ Thống (Requirements)

### 6.1. Yêu Cầu Chức Năng (Functional Requirements)
- **FR-01 (Sinh sự kiện IoT)**: Generator phải phát ra các bản ghi định dạng JSON hợp chuẩn schema, gán nhãn `is_late_simulated` với tỷ lệ xác suất đạt $20\% \pm 2\%$ trên mẫu lớn.
- **FR-02 (Kiểm thực Schema)**: Mọi bản ghi không đủ trường bắt buộc, sai định dạng ISO-8601 UTC, hoặc có giá trị nhiệt độ/độ ẩm nằm ngoài ngưỡng vật lý phải bị từ chối trước khi đẩy vào Kafka.
- **FR-03 (Phân vùng theo thiết bị)**: Kafka Producer phải sử dụng `device_id` làm partition key để đảm bảo tính tuần tự theo từng cảm biến.
- **FR-04 (Tổng hợp Cửa Sổ)**: Spark phải gom nhóm dữ liệu theo từng khung thời gian 1 phút và theo từng `device_id`, tính toán tổng số bản ghi (`event_count`), nhiệt độ trung bình (`average_temperature`) và độ ẩm trung bình (`average_humidity`).
- **FR-05 (Cắt Lọc Watermark)**: Spark phải cập nhật watermark tự động dựa trên thời gian sự kiện lớn nhất quan sát được trừ đi 120 giây; các sự kiện thuộc cửa sổ đã đóng không được làm sai lệch kết quả cửa sổ đó.
- **FR-06 (Lưu Trữ Bền Vững Idempotent)**: Cập nhật kết quả vào PostgreSQL bằng câu lệnh `ON CONFLICT (window_start, window_end, device_id) DO UPDATE` để đảm bảo khi xử lý lại một batch, kết quả cuối cùng không bị nhân đôi.
- **FR-07 (Khôi Phục Trạng Thái)**: Khi tiến trình Spark khởi động lại, engine phải nạp offset và delta state từ thư mục checkpoint được mount ngoài ổ đĩa vật lý.

### 6.2. Yêu Cầu Phi Chức Năng (Non-Functional Requirements)
- **NFR-01 (Hiệu năng Throughput)**: Pipeline xử lý ổn định ở tải định mức 10 msg/s mà không phát sinh consumer lag tích tụ kéo dài.
- **NFR-02 (Thời gian khôi phục RTO)**: Quá trình diễn tập khôi phục từ backup và checkpoint (`recovery_drill.ps1`) phải hoàn tất trong thời gian dưới 300 giây (5 phút).
- **NFR-03 (Tính toàn vẹn dữ liệu)**: Không xuất hiện bản ghi trùng lặp khóa chính logic (`duplicate_count == 0`), không xuất hiện bản ghi có mốc thời gian ngược (`window_end <= window_start`).
- **NFR-04 (Giới hạn tài nguyên)**: Hệ thống container phải tuân thủ nghiêm ngặt cgroups quota (Redpanda $\le 1.5$ GB RAM, Spark Processor $\le 2$ Cores / $2$ GB RAM).
- **NFR-05 (Bảo mật container)**: Các tiến trình ứng dụng trong container phải chạy dưới quyền tài khoản non-root (`spark` UID 1001, `generator` UID 1002).

---

## 7. Tiêu Chí Thành Công & Sản Phẩm Bàn Giao

### 7.1. Tiêu Chí Thành Công (Success Criteria)
1. Khởi chạy toàn bộ hạ tầng bằng Docker Compose v2 với 8 container sẵn sàng, trạng thái healthcheck báo `healthy`.
2. Bộ kiểm thử hồi quy `tests.test_smoke_pipeline` chạy đạt 100% PASS trên môi trường máy chủ.
3. Kịch bản khôi phục sự cố `ops/recovery_drill.ps1` chạy tự động, đạt tiêu chuẩn nghiệm thu RTO < 300s và sinh snapshot kiểm tra toàn vẹn hợp lệ.
4. Có bộ số liệu thô và biểu đồ đối chiếu định lượng cho các kịch bản: baseline vận hành, ép tải 4 pha xuất hiện backpressure và phân tích điểm nghẽn.
5. Hồ sơ tài liệu kỹ thuật hoàn chỉnh, giải trình minh bạch hiện trạng mã nguồn, không làm giả số liệu đo đạc.

### 7.2. Sản Phẩm Bàn Giao Dự Kiến (Deliverables)
- Bộ mã nguồn nguồn mở hoàn chỉnh trên Git repository.
- Hệ thống 12 tài liệu kỹ thuật chuyên đề (`docs/01-project-overview.md` đến `docs/12-limitations-roadmap.md`).
- Bộ sổ tay vận hành và hướng dẫn từng thành phần (`src/README.md`, `ops/README.md`, `benchmarks/README.md`, `configs/README.md`, `tests/README.md`).
- Kịch bản kiểm tra toàn vẹn SQL và script tự động hóa đo đạc phục hồi.
- Bộ 3 dashboard cấu hình sẵn cho Grafana phục vụ trình diễn và phân tích trực quan.

---

## 8. Phân Công Trách Nhiệm Nhóm (Team RACI Matrix)

Đội ngũ thực hiện dự án gồm 3 thành viên với ranh giới trách nhiệm và KPI lõi được quy định rõ ràng:

| Thành Viên | Vai Trò Chính | Phạm Vi Kỹ Thuật | KPI Lõi | Quản Trị Rủi Ro |
|---|---|---|---|---|
| **Thái**<br/>*(Team Leader)* | Cloud Architect & Infrastructure Lead | Thiết kế kiến trúc tổng thể, Docker Compose IaC, Redpanda Broker, PostgreSQL DDL, Mount volume checkpoint, Script SRE backup/restore/drill. | Hạ tầng triển khai lặp lại 100% bằng script; Quản lý secret an toàn; RTO khôi phục < 300s. | Tràn đĩa volume, rò rỉ secret `.env`, xung đột quyền ghi thư mục checkpoint của non-root user. |
| **Tân Lê**<br/>*(Member)* | Stream Processing & Data Engineer | Event Generator, Schema Validator, Spark Structured Streaming (window, watermark, foreachBatch upsert, Late Data Router, Unit/Integration tests). | Dữ liệu trễ 20% tái lập được chuẩn xác; Không làm mất dữ liệu; Watermark cắt đúng thời gian sự kiện. | Tính toán watermark sai lệch, ép kiểu timestamp mất timezone, rò rỉ connection pool PostgreSQL. |
| **Tân Lưu**<br/>*(Member)* | Observability & Performance Analyst | Bộ giám sát NhutTan (Prometheus + Grafana), Thiết kế kịch bản ép tải Stress Test, Đo lường Consumer Lag & Backpressure, Thu thập số liệu phân tích. | 3 Dashboard Grafana hoạt động chuẩn xác; Có dữ liệu chuỗi thời gian CSV/JSON chứng minh điểm nghẽn. | Nhầm lẫn giữa số liệu giả lập và số liệu runtime thực tế; Hệ thống treo cứng khi ép tải vượt ngưỡng. |

### Ma Trận Trách Nhiệm RACI:

| Hạng mục công việc | Thái (Lead) | Tân Lê | Tân Lưu |
|---|:---:|:---:|:---:|
| Hạ tầng Docker, Networks, Storage Volumes | **Accountable / Responsible** | Consulted | Informed |
| Schema sự kiện & Mã nguồn Generator | Consulted | **Accountable / Responsible** | Informed |
| Core Pipeline Spark & Xử lý Cửa sổ / Watermark | Consulted | **Accountable / Responsible** | Consulted |
| PostgreSQL Schema, Connection Pool & Idempotent Sink | **Accountable** | **Responsible** | Informed |
| Kịch bản Diễn tập Phục hồi (Recovery Drill) & SRE Ops | **Accountable / Responsible** | Informed | Consulted |
| Bộ Dashboard Grafana, Exporter & Phân tích Ép Tải | Informed | Consulted | **Accountable / Responsible** |
| Bộ Kiểm thử Tự động (Smoke, Unit, Integration) | Consulted | **Accountable / Responsible** | Informed |
| Hệ thống Hồ sơ Kỹ thuật Toàn diện | **Accountable** | **Responsible** | **Responsible** |

