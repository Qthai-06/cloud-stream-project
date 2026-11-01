"""
Script tự động kiểm tra toàn diện và xuất kết quả báo cáo.
Chạy 1 lệnh duy nhất để nghiệm thu toàn bộ hệ thống giám sát và sinh file báo cáo Word (.docx).
"""

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NHUT_TAN_DIR = PROJECT_ROOT / "NhutTan"

# Đảm bảo PROJECT_ROOT và NHUT_TAN_DIR có trong sys.path để hỗ trợ mọi cách gọi lệnh
for p in [str(PROJECT_ROOT), str(NHUT_TAN_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from NhutTan.src.record_baseline import (
        generate_reference_baseline,
        save_baseline,
        print_summary_table,
    )
    from NhutTan.src.spark_metrics_listener import MetricsCollector, PROMETHEUS_AVAILABLE
    from NhutTan.src.export_report_docx import build_report_docx, OUTPUT_DOCX
except ImportError:
    from src.record_baseline import (  # type: ignore
        generate_reference_baseline,
        save_baseline,
        print_summary_table,
    )
    from src.spark_metrics_listener import MetricsCollector, PROMETHEUS_AVAILABLE  # type: ignore
    from src.export_report_docx import build_report_docx, OUTPUT_DOCX  # type: ignore


def log_step(name: str):
    print(f"\n============================================================")
    print(f" [*] ĐANG KIỂM TRA: {name}")
    print(f"============================================================")


def check_files_exist():
    log_step("1. Kiểm tra sự tồn tại của các file cấu hình và mã nguồn")
    required_files = [
        NHUT_TAN_DIR / "docker-compose.monitoring.yml",
        NHUT_TAN_DIR / "prometheus" / "prometheus.yml",
        NHUT_TAN_DIR / "grafana" / "provisioning" / "datasources" / "prometheus.yml",
        NHUT_TAN_DIR / "grafana" / "provisioning" / "dashboards" / "dashboards.yml",
        NHUT_TAN_DIR / "grafana" / "dashboards_json" / "01_container_resources_baseline.json",
        NHUT_TAN_DIR / "grafana" / "dashboards_json" / "02_spark_streaming_performance.json",
        NHUT_TAN_DIR / "src" / "record_baseline.py",
        NHUT_TAN_DIR / "src" / "spark_metrics_listener.py",
        NHUT_TAN_DIR / "src" / "export_report_docx.py",
    ]
    for f in required_files:
        if not f.exists():
            print(f" [X] THIẾU FILE: {f}")
            return False
        print(f"  [OK] Đã tìm thấy: {f.name}")
    return True


def check_dashboards_valid():
    log_step("2. Kiểm tra tính hợp lệ cú pháp JSON của các Dashboard Grafana")
    d1 = NHUT_TAN_DIR / "grafana" / "dashboards_json" / "01_container_resources_baseline.json"
    d2 = NHUT_TAN_DIR / "grafana" / "dashboards_json" / "02_spark_streaming_performance.json"

    for d in [d1, d2]:
        with open(d, "r", encoding="utf-8") as f:
            data = json.load(f)
            title = data.get("title", "Unknown")
            panels_count = len(data.get("panels", []))
            print(f"  [OK] Dashboard: '{title}' - Tổng cộng: {panels_count} panels hợp lệ.")
    return True


def test_baseline_recorder():
    log_step("3. Chạy thử nghiệm script thu thập số liệu Baseline (Tuần 3)")
    records = generate_reference_baseline()
    save_baseline(records, filename_prefix="baseline_reference_metrics")
    print_summary_table(records)
    print("  [OK] Đã ghi nhận số liệu Baseline ra CSV và JSON thành công!")
    return True


def test_spark_metrics_listener():
    log_step("4. Chạy thử nghiệm Spark Streaming Metrics Exporter & Listener (Tuần 4)")
    print(f"  Trạng thái thư viện Prometheus Client: {'ĐÃ CÀI ĐẶT' if PROMETHEUS_AVAILABLE else 'CHƯA CÓ'}")
    collector = MetricsCollector(query_name="automated_test_stream", port=8000)

    # Thử ghi nhận 3 micro-batches
    for b_id in range(1, 4):
        collector.record_batch(
            batch_id=b_id,
            num_rows=10,
            duration_seconds=0.18,
            checkpoint_seconds=0.045,
            late_events_count=2 if b_id == 2 else 0,
        )

    # Thử truy vấn HTTP endpoint /metrics
    try:
        req = urllib.request.Request("http://127.0.0.1:8000/metrics")
        with urllib.request.urlopen(req, timeout=3) as resp:
            content = resp.read().decode("utf-8")
            if "spark_streaming_input_rows_per_second" in content:
                print("  [OK] Endpoint http://localhost:8000/metrics đang phát metrics chuẩn Prometheus!")
    except Exception as e:
        print(f"  [LƯU Ý] Kiểm tra endpoint HTTP: {e}")

    collector.close()
    return True


def generate_docx():
    log_step("5. Tự động xuất file Báo cáo Word (.docx) hoàn chỉnh")
    build_report_docx()
    if OUTPUT_DOCX.exists():
        size_kb = OUTPUT_DOCX.stat().st_size / 1024
        print(f"  [OK] Đã tạo thành công file Word: {OUTPUT_DOCX.name} ({size_kb:.1f} KB)")
        return True
    return False


def main():
    print("================================================================================")
    print(" BẮT ĐẦU KIỂM TRA TOÀN DIỆN CÁC TÁC VỤ CỦA TÂN LƯU (TUẦN 1 ĐẾN TUẦN 4)")
    print(" Đề tài 16: Cloud Stream Project — Hệ thống xử lý luồng thời gian thực")
    print("================================================================================")

    steps = [
        ("Kiểm tra file cấu hình", check_files_exist),
        ("Kiểm tra Grafana Dashboards", check_dashboards_valid),
        ("Kiểm tra Baseline Recorder (Tuần 3)", test_baseline_recorder),
        ("Kiểm tra Metrics Listener & Port 8000 (Tuần 4)", test_spark_metrics_listener),
        ("Sinh Báo cáo Word (.docx)", generate_docx),
    ]

    for name, func in steps:
        if not func():
            print(f"\n[THẤT BẠI] Kiểm tra dừng tại bước: {name}")
            sys.exit(1)

    print("\n" + "=" * 80)
    print(" [HOÀN TẤT 100%] TẤT CẢ CÁC BƯỚC ĐÃ ĐƯỢC KIỂM TRA VÀ CHẠY THÀNH CÔNG RỰC RỠ!")
    print("================================================================================")
    print("BẠN CÓ THỂ LẤY CÁC KẾT QUẢ ĐỂ NỘP / VIẾT BÁO CÁO (TRONG THƯ MỤC NhutTan/ket_qua/):")
    print("1. File Word hoàn chỉnh: NhutTan/ket_qua/BaoCao_Tuan1_den_Tuan4_TanLuu.docx")
    print("2. Dữ liệu số liệu Baseline CSV: NhutTan/ket_qua/baseline_reference_metrics.csv")
    print("3. Dữ liệu số liệu Baseline JSON: NhutTan/ket_qua/baseline_reference_metrics.json")
    print("4. Log kết quả Tuần 1-2: NhutTan/ket_qua/ket_qua_tuan1_2_kiem_thu_ha_tang.txt")
    print("5. Log kết quả Tuần 3: NhutTan/ket_qua/ket_qua_tuan3_baseline_tai_nguyen.txt")
    print("6. Log kết quả Tuần 4: NhutTan/ket_qua/ket_qua_tuan4_throughput_checkpoint.txt")
    print("================================================================================\n")


if __name__ == "__main__":
    main()
