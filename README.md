Set-Content -Path README.md -Value @"
# Đề tài 16: Hệ thống Xử lý Luồng Thời gian Thực

## Khởi động Hạ tầng
1. Sao chép biến môi trường: \`cp .env.example .env\`
2. Khởi chạy toàn bộ dịch vụ: \`docker compose up -d\`

## Cổng kết nối Dịch vụ (Ports)
- **Redpanda (Kafka API):** \`localhost:9092\`
- **Redpanda Console UI:** \`http://localhost:8080\`
- **PostgreSQL:** \`localhost:5432\`
- **ClickHouse HTTP / Native:** \`localhost:8123\` / \`localhost:9000\`
"@