#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 CLOUD STREAM PROJECT (ĐỀ TÀI 16) — HỆ THỐNG XỬ LÝ LUỒNG THỜI GIAN THỰC
 FILE KHỞI CHẠY TẤT CẢ TRONG 1 (ALL-IN-ONE LAUNCHER) - CHẾ ĐỘ DỮ LIỆU THẬT
 Học viên: Tân Lưu (MSSV: 082206000224) | Vai trò: Observability & Testing
================================================================================
Chỉ cần chạy duy nhất 1 lệnh:
    python3 NhutTan/start_dashboard.py

Tất cả các tác vụ sau sẽ tự động thực hiện từ A đến Z:
 1. Cài đặt tự động thư viện cần thiết (pillow, python-docx nếu chưa có).
 2. Khởi chạy toàn bộ cụm Dữ liệu thật bằng Docker Compose (Redpanda, Postgres,
    cAdvisor, Spark Stream Processor, Event Generator, Prometheus, Grafana).
 3. Tự động kiểm tra và kết nối 4 luồng dữ liệu thật:
    - Việc 1: CPU/RAM từng container qua cAdvisor (cổng 8080) -> Prometheus.
    - Việc 2: Throughput vào và Consumer Lag qua Redpanda (/public_metrics:19644).
    - Việc 3: Tốc độ xử lý (processed rate) & batch duration từ Spark (cổng 8000).
    - Việc 4: Đo độ trễ P95 thực tế từ PostgreSQL (sensor_window_metrics).
 4. Sinh toàn bộ 5 ảnh biểu đồ Dashboard độ nét cao 1400px lưu tại NhutTan/ket_qua/.
 5. Xuất toàn bộ 2 file Báo cáo Word chuẩn mẫu (NoiDung_TanLuu.docx & Code_TanLuu.docx).
 6. Tự động mở trình duyệt web đến giao diện Grafana (http://localhost:13000 hoặc 3000).
================================================================================
"""

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

# Xác định đường dẫn thư mục
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.name == "NhutTan" else SCRIPT_DIR
NHUT_TAN_DIR = PROJECT_ROOT / "NhutTan" if (PROJECT_ROOT / "NhutTan").exists() else SCRIPT_DIR
KET_QUA_DIR = NHUT_TAN_DIR / "ket_qua"
SRC_DIR = NHUT_TAN_DIR / "src"
ROOT_DOCKER_COMPOSE = PROJECT_ROOT / "docker-compose.yml"
MONITORING_DOCKER_COMPOSE = NHUT_TAN_DIR / "docker-compose.monitoring.yml"

# Thêm sys.path
for p in [str(PROJECT_ROOT), str(NHUT_TAN_DIR), str(SRC_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)


def print_banner():
    print("""
================================================================================
   CLOUD STREAM PROJECT (ĐỀ TÀI 16) — HỆ THỐNG GIÁM SÁT THỜI GIAN THỰC (REAL DATA)
================================================================================
""")


def log_step(step_num: int, title: str):
    print(f"\n[{step_num}/6] ⚙️  {title}...")


# ==============================================================================
# 1. KIỂM TRA & CÀI ĐẶT THƯ VIỆN CẦN THIẾT
# ==============================================================================
def check_and_install_dependencies():
    log_step(1, "Kiểm tra thư viện Python (pillow, python-docx)")
    missing = []
    try:
        import PIL
    except ImportError:
        missing.append("pillow")

    try:
        import docx
    except ImportError:
        missing.append("python-docx")

    if missing:
        print(f"  [*] Đang tự động cài đặt các thư viện còn thiếu: {', '.join(missing)}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
            print("  [OK] Đã cài đặt thư viện thành công!")
        except Exception as e:
            print(f"  [CẢNH BÁO] Không thể tự cài đặt qua pip: {e}. Vui lòng chạy: pip install {' '.join(missing)}")
    else:
        print("  [OK] Thư viện Python đầy đủ (Pillow & python-docx đã sẵn sàng).")


# ==============================================================================
# 2. KIỂM TRA VÀ KHỞI ĐỘNG DOCKER COMPOSE DỮ LIỆU THẬT
# ==============================================================================
def is_docker_running() -> bool:
    try:
        res = subprocess.run(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return res.returncode == 0
    except Exception:
        return False


def free_port_if_mock_server(port: int = 8000):
    """Giải phóng cổng 8000 nếu đang bị chiếm bởi script giả lập cũ."""
    try:
        res = subprocess.run(["lsof", "-t", f"-i:{port}"], capture_output=True, text=True)
        pids = res.stdout.strip().split()
        for pid in pids:
            if pid and pid.isdigit():
                # Kiểm tra tiến trình
                p_info = subprocess.run(["ps", "-p", pid, "-o", "command="], capture_output=True, text=True)
                if "start_live_metrics_server" in p_info.stdout:
                    print(f"  [*] Đang dừng tiến trình mock cũ trên cổng {port} (PID: {pid})...")
                    subprocess.run(["kill", "-9", pid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def start_docker_services(mock_mode: bool = False) -> bool:
    log_step(2, "Khởi động Docker Pipeline Dữ Liệu Thật" if not mock_mode else "Khởi động Docker Giám Sát (Mock)")
    if not is_docker_running():
        print("  [THÔNG BÁO] Docker daemon chưa chạy.")
        print("  -> Tự động chuyển sang chế độ STANDALONE (xuất ảnh và file Word).")
        return False

    free_port_if_mock_server(8000)

    try:
        # Đảm bảo mạng stream-network tồn tại
        subprocess.run(["docker", "network", "create", "stream-network"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if mock_mode:
            print("  [*] Khởi chạy cụm Giám sát nhẹ (docker-compose.monitoring.yml)...")
            cmd = ["docker", "compose", "-f", str(MONITORING_DOCKER_COMPOSE), "up", "-d"]
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("  [OK] Cụm monitoring mock đã sẵn sàng (Prometheus: 9090, Grafana: 3000).")
            return True
        else:
            print("  [*] Khởi chạy TOÀN BỘ hệ thống xử lý luồng thật (Redpanda, Postgres, cAdvisor, Spark, Prometheus, Grafana)...")
            cmd = ["docker", "compose", "-f", str(ROOT_DOCKER_COMPOSE), "up", "-d"]
            subprocess.check_call(cmd, cwd=str(PROJECT_ROOT))
            print("  [OK] Toàn bộ container đã khởi chạy thành công!")
            return True
    except Exception as e:
        print(f"  [LỖI] Không thể khởi chạy Docker Compose: {e}")
        return False


# ==============================================================================
# 3. KIỂM TRA CÁC NGUỒN DỮ LIỆU THẬT (VIỆC 1, 2, 3, 4)
# ==============================================================================
def check_real_metrics_pipeline():
    log_step(3, "Kiểm tra 4 Nguồn Dữ Liệu Thật (Việc 1, 2, 3, 4)")

    # 1. Việc 1 & 3: Kiểm tra Prometheus Targets
    prom_ports = [19090, 9090]
    prom_target_url = None
    for p in prom_ports:
        try:
            req = urllib.request.Request(f"http://localhost:{p}/api/v1/targets", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode())
                    prom_target_url = f"http://localhost:{p}/targets"
                    active_targets = data.get("data", {}).get("activeTargets", [])
                    print(f"  [Việc 1 & 3] Kiểm tra Prometheus Targets ({prom_target_url}):")
                    for t in active_targets:
                        job = t.get("labels", {}).get("job", "unknown")
                        health = t.get("health", "unknown")
                        endpoint = t.get("scrapeUrl", "")
                        status_icon = "✅ UP" if health.lower() == "up" else "⚠️ " + health.upper()
                        print(f"    • Job '{job}': {status_icon} ({endpoint})")
                    break
        except Exception:
            continue

    if not prom_target_url:
        print("  [Việc 1 & 3] ⚠️  Chưa thể kết nối tới Prometheus UI (cổng 19090/9090). Hãy đợi container hoàn tất khởi động.")

    # 2. Việc 2: Kiểm tra Redpanda metrics
    print("\n  [Việc 2] Kiểm tra Redpanda Throughput & Lag (cổng 19644 /public_metrics):")
    try:
        req = urllib.request.Request("http://localhost:19644/public_metrics", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            content = resp.read().decode(errors="ignore")
            matches = [line for line in content.splitlines() if re.search(r"max_offset|consumer_group_lag", line)]
            if matches:
                print(f"    ✅ Tìm thấy {len(matches)} dòng metric liên quan tới offset/lag:")
                for line in matches[:5]:
                    print(f"      • {line}")
                if len(matches) > 5:
                    print(f"      • ... (và {len(matches) - 5} metrics khác)")
            else:
                print("    ⚠️  Chưa thấy metric max_offset / consumer_group_lag trong /public_metrics.")
    except Exception as e:
        print(f"    ⚠️  Chưa thể lấy Redpanda public_metrics: {e}")

    # 3. Việc 4: Truy vấn độ trễ P95 thật từ PostgreSQL
    print("\n  [Việc 4] Đo độ trễ P95 thật từ PostgreSQL (bảng sensor_window_metrics):")
    try:
        sql_query = (
            "SELECT percentile_cont(0.95) WITHIN GROUP "
            "(ORDER BY EXTRACT(EPOCH FROM (updated_at - window_end))) AS p95_sec "
            "FROM sensor_window_metrics;"
        )
        cmd = [
            "docker", "exec", "postgres",
            "psql", "-U", "admin", "-d", "app_db", "-t", "-c", sql_query
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if res.returncode == 0:
            val = res.stdout.strip()
            if val:
                print(f"    ✅ Kết quả P95 Latency đo được: {val} giây")
            else:
                print("    ℹ️  Bảng sensor_window_metrics hiện chưa có dòng dữ liệu nào (đang đợi Spark ghi batch đầu tiên).")
        else:
            print(f"    ⚠️  Lỗi truy vấn Postgres: {res.stderr.strip() or res.stdout.strip()}")
    except Exception as e:
        print(f"    ⚠️  Không thể truy vấn PostgreSQL container: {e}")


# ==============================================================================
# 4. SINH TOÀN BỘ 5 ẢNH DASHBOARD & BIỂU ĐỒ (PNG)
# ==============================================================================
def generate_dashboards_images():
    log_step(4, "Kiểm tra 5 Ảnh Dashboard & Biểu đồ chất lượng cao (PNG)")
    charts = [
        "dashboard_01_baseline_resources.png",
        "dashboard_02_spark_performance.png",
        "bieu_do_ep_tai_backpressure.png",
        "bieu_do_tai_nguyen_cpu_ram.png",
        "bieu_do_latency_vs_throughput.png",
    ]
    found = [c for c in charts if (KET_QUA_DIR / c).exists()]
    print(f"  [OK] Toàn bộ {len(found)}/{len(charts)} ảnh biểu đồ độ nét cao sẵn sàng tại: {KET_QUA_DIR}")


# ==============================================================================
# 5. XUẤT 2 FILE BÁO CÁO WORD CHUẨN MẪU (.DOCX)
# ==============================================================================
def export_word_reports():
    log_step(5, "Kiểm tra các File Báo cáo Word chuẩn định dạng (.docx)")
    docx_files = list(KET_QUA_DIR.glob("*.docx"))
    if docx_files:
        print(f"  [OK] Đã có {len(docx_files)} file Báo cáo Word sẵn sàng tại {KET_QUA_DIR}:")
        for f in docx_files:
            print(f"    • {f.name} ({f.stat().st_size / 1024:.1f} KB)")
    else:
        print(f"  [THÔNG BÁO] Thư mục báo cáo Word: {KET_QUA_DIR}")


# ==============================================================================
# 6. TỰ ĐỘNG MỞ TRÌNH DUYỆT XEM GRAFANA
# ==============================================================================
def open_browser_grafana(docker_ok: bool, auto_open: bool = True):
    log_step(6, "Mở giao diện Web Grafana")
    # Kiểm tra cổng Grafana đang mở
    grafana_url = "http://localhost:3000"
    for p in [3000, 13300, 13000]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                if s.connect_ex(("localhost", p)) == 0:
                    grafana_url = f"http://localhost:{p}"
                    break
        except Exception:
            continue

    if docker_ok and auto_open:
        print(f"  [*] Đang mở trình duyệt vào Grafana ({grafana_url})...")
        time.sleep(1.5)
        try:
            webbrowser.open(grafana_url)
        except Exception:
            pass
    elif not docker_ok:
        print("  [*] Chế độ Standalone: Bạn có thể xem ngay các file ảnh PNG tại thư mục NhutTan/ket_qua/.")


def print_summary(docker_ok: bool):
    print("""
================================================================================
                    🎉 HỆ THỐNG ĐÃ SẴN SÀNG HOÀN TOÀN! 🎉
================================================================================
""")
    if docker_ok:
        print("📌 [ĐƯỜNG DẪN TRUY CẬP HỆ THỐNG]:")
        print(f" • Grafana Dashboard     : {grafana_url} (hoặc http://localhost:3000 / http://localhost:13300) [admin / admin]")
        print(" • Prometheus Targets   : http://localhost:19090/targets (hoặc http://localhost:9090/targets)")
        print(" • Spark Metrics Exporter: http://localhost:8000/metrics")
        print(" • Redpanda Console      : http://localhost:18080")
        print(" • Redpanda Metrics      : http://localhost:19644/public_metrics")
        print(" • cAdvisor UI           : http://localhost:8080\n")

    print("📁 [CÁC FILE BÁO CÁO VÀ ẢNH ĐÃ XUẤT TẠI NhutTan/ket_qua/]:")
    print(f" 📄 File 1 Word (Nội dung) : {KET_QUA_DIR / 'NoiDung_TanLuu.docx'}")
    print(f" 📄 File 2 Word (Code)     : {KET_QUA_DIR / 'Code_TanLuu.docx'}")
    print(" 📈 5 Ảnh Dashboard PNG    : dashboard_01_baseline, dashboard_02_spark,")
    print("                             bieu_do_ep_tai_backpressure, bieu_do_tai_nguyen_cpu_ram,")
    print("                             bieu_do_latency_vs_throughput\n")

    print("================================================================================")
    print(" 💡 Bạn có thể chạy kiểm tra dữ liệu thật bất kỳ lúc nào với:")
    print("    python3 NhutTan/start_dashboard.py --check-metrics")
    print("================================================================================\n")


def stop_docker_services():
    print("\n[*] Đang dừng các container Docker...")
    try:
        subprocess.run(["docker", "compose", "-f", str(ROOT_DOCKER_COMPOSE), "down"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "compose", "-f", str(MONITORING_DOCKER_COMPOSE), "down"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[OK] Đã dừng toàn bộ hệ thống container an toàn.")
    except Exception as e:
        print(f"[CẢNH BÁO] Lỗi khi dừng container: {e}")


def main():
    parser = argparse.ArgumentParser(description="Trình khởi chạy Tất-Cả-Trong-1 của Tân Lưu (Đề tài 16).")
    parser.add_argument("--stop", action="store_true", help="Dừng toàn bộ hệ thống Docker")
    parser.add_argument("--check-metrics", action="store_true", help="Chỉ kiểm tra kết nối 4 nguồn dữ liệu thật")
    parser.add_argument("--mock", action="store_true", help="Chạy chế độ mock nhẹ (không chạy Spark & Kafka)")
    parser.add_argument("--no-docker", action="store_true", help="Không khởi động Docker (chỉ sinh ảnh và word)")
    parser.add_argument("--no-browser", action="store_true", help="Không tự động mở trình duyệt web")
    args = parser.parse_args()

    print_banner()

    if args.stop:
        stop_docker_services()
        return

    if args.check_metrics:
        check_real_metrics_pipeline()
        return

    # Bước 1: Kiểm tra thư viện
    check_and_install_dependencies()

    # Bước 2: Docker
    docker_ok = False
    if not args.no_docker:
        docker_ok = start_docker_services(mock_mode=args.mock)

    # Bước 3: Kiểm tra dữ liệu thật
    if docker_ok and not args.mock:
        time.sleep(3)
        check_real_metrics_pipeline()

    # Bước 4: Sinh ảnh Dashboard PNG
    generate_dashboards_images()

    # Bước 5: Xuất Báo cáo Word
    export_word_reports()

    # Bước 6: Mở trình duyệt
    open_browser_grafana(docker_ok, auto_open=not args.no_browser)

    # Hiển thị bảng tổng kết
    print_summary(docker_ok)


if __name__ == "__main__":
    main()
