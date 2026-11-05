# Infrastructure Recovery Runbook

## Mục tiêu

Khôi phục PostgreSQL và Spark trong tối đa 5 phút mà không xóa dữ liệu
persistent ngoài chủ đích. PostgreSQL backup được lưu trong
`backups/postgres/`; Spark checkpoint được lưu trong
`checkpoints/spark_pipeline/`.

## 1. Backup PostgreSQL

Chạy tại thư mục gốc:

```powershell
.\ops\backup_postgres.ps1
```

Script sẽ:

1. Kiểm tra PostgreSQL sẵn sàng.
2. Chạy `pg_dump` định dạng custom.
3. Copy file backup ra `backups/postgres/`.
4. Tạo file SHA-256 cạnh file backup.

Không commit thư mục backup nếu chứa dữ liệu thật.

## 2. Kiểm tra integrity

```powershell
.\ops\verify_recovery.ps1
```

Kiểm tra bảng `sensor_window_metrics`, số dòng logic, duplicate và window
không hợp lệ.

## 3. Khôi phục PostgreSQL

Restore có thể ghi đè các bảng trong database đích. Luôn tạo backup hiện tại
trước, sau đó chạy lệnh có cờ xác nhận:

```powershell
.\ops\backup_postgres.ps1
.\ops\restore_postgres.ps1 `
  -BackupFile (Get-ChildItem .\backups\postgres\*.dump |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1).FullName `
  -ConfirmRestore
.\ops\verify_recovery.ps1
```

Script kiểm tra checksum nếu file `.sha256` tồn tại và tự xóa file tạm trong
container sau khi restore.

## 4. Khôi phục Spark từ checkpoint

Không xóa thư mục `checkpoints`:

```powershell
docker compose stop stream-processor
docker compose start stream-processor
docker compose logs --tail=100 stream-processor
Get-ChildItem .\checkpoints\spark_pipeline -Recurse
```

Spark phải khởi động lại với:

```text
/opt/workspace/checkpoints/spark_pipeline
```

được ánh xạ từ `.\checkpoints\spark_pipeline`.

## 5. Chạy recovery drill có đo thời gian

Drill này không xóa database và không mô phỏng phá hủy volume. Vì generator
vẫn có thể phát thêm event trong lúc restart, row count/checksum có thể tăng;
drill chỉ yêu cầu row count không giảm, không có duplicate và không có window
không hợp lệ:

```powershell
.\ops\recovery_drill.ps1
```

Drill tạo backup, khởi động lại PostgreSQL/Spark, chạy integrity check và fail
nếu tổng thời gian vượt 5 phút.

Kết quả nghiệm thu local đã đạt: drill hoàn thành trong khoảng `19.5 giây`,
row count không giảm, duplicate bằng `0`, window không hợp lệ bằng `0` và
checkpoint vẫn tồn tại. Khi chạy lại, thời gian có thể khác do tốc độ máy và
khối lượng dữ liệu.

Để lưu bằng chứng của lần chạy:

```powershell
New-Item -ItemType Directory -Force .\recovery-evidence | Out-Null
.\ops\recovery_drill.ps1 *>&1 |
  Tee-Object .\recovery-evidence\recovery-drill-$(Get-Date -Format yyyyMMdd-HHmmss).log
```

Thư mục `recovery-evidence/` đã được đưa vào `.gitignore`; chỉ commit báo cáo
đã làm sạch secret nếu cần nộp bằng chứng.

## 5a. Sự cố broker đã gặp trong môi trường demo

Trong một lần chạy trước, topic `iot-sensor-events` bị `leaderless_partitions`
và log Redpanda không nhất quán, khiến Spark gặp `TimeoutException` và
`DisconnectException`. Topic đã được tạo lại sau khi xác nhận chấp nhận mất
message Kafka cũ. Đây là thao tác sửa chữa broker, không phải recovery dữ liệu
PostgreSQL.

Kiểm tra trước khi kết luận pipeline hỏng:

```powershell
docker exec redpanda rpk cluster health --api-urls localhost:9644
docker exec redpanda rpk topic describe iot-sensor-events
```

Chỉ recreate topic khi đã đánh giá retention và chấp nhận mất dữ liệu Kafka
chưa được xử lý; không đưa thao tác này vào recovery drill thông thường.

## 6. Mô phỏng sự cố container

Chỉ chạy sau khi đã có backup:

```powershell
.\ops\backup_postgres.ps1
docker compose stop postgres
docker compose start postgres
.\ops\verify_recovery.ps1
docker compose restart stream-processor
```

Không dùng `docker compose down --volumes`; lệnh đó xóa dữ liệu persistent và
không phải kịch bản recovery an toàn.

## 7. Tiêu chí nghiệm thu

- [ ] Backup `.dump` được tạo.
- [ ] SHA-256 backup được tạo và kiểm tra.
- [ ] PostgreSQL restore thành công.
- [ ] Không có duplicate logical row.
- [ ] Không có window không hợp lệ.
- [ ] Spark khởi động lại từ checkpoint.
- [ ] Recovery drill hoàn thành dưới 5 phút.
- [ ] Lưu log thời gian và kết quả integrity làm bằng chứng.
