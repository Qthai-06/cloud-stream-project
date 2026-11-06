#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 CLOUD STREAM PROJECT (ĐỀ TÀI 16) — HỆ THỐNG XỬ LÝ LUỒNG THỜI GIAN THỰC
 FILE KHỞI CHẠY TẤT CẢ TRONG 1 (ALL-IN-ONE LAUNCHER)
 Học viên: Tân Lưu (MSSV: 082206000224) | Vai trò: Observability & Testing
================================================================================
Chỉ cần chạy duy nhất 1 lệnh:
    python3 NhutTan/start_dashboard.py

Tất cả các tác vụ sau sẽ tự động thực hiện từ A đến Z:
 1. Cài đặt tự động thư viện cần thiết (pillow, python-docx nếu chưa có).
 2. Khởi chạy Docker Compose Giám sát (Prometheus & Grafana) nếu máy có Docker.
 3. Tự động nạp sẵn 3 Dashboard JSON và DataSource vào Grafana (Auto-Provisioning).
 4. Bật Live Metrics Server ngầm trên cổng 8000 phát metrics chu kỳ 180s cho Prometheus.
 5. Sinh toàn bộ 5 ảnh biểu đồ Dashboard độ nét cao 1400px lưu tại NhutTan/ket_qua/.
 6. Xuất toàn bộ 2 file Báo cáo Word chuẩn mẫu (NoiDung_TanLuu.docx & Code_TanLuu.docx).
 7. Tự động mở trình duyệt web đến giao diện Grafana (http://localhost:3000).
================================================================================
"""

import argparse
import http.server
import os
import socket
import socketserver
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

# Xác định đường dẫn tương đối và tuyệt đối
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.name == "NhutTan" else SCRIPT_DIR
NHUT_TAN_DIR = PROJECT_ROOT / "NhutTan" if (PROJECT_ROOT / "NhutTan").exists() else SCRIPT_DIR
KET_QUA_DIR = NHUT_TAN_DIR / "ket_qua"
SRC_DIR = NHUT_TAN_DIR / "src"
DOCKER_COMPOSE_MONITORING = NHUT_TAN_DIR / "docker-compose.monitoring.yml"

# Thêm vào sys.path để import dễ dàng
for p in [str(PROJECT_ROOT), str(NHUT_TAN_DIR), str(SRC_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ==============================================================================
# HÀM TIỆN ÍCH IN ẤN
# ==============================================================================
def print_banner():
    print("""
================================================================================
        CLOUD STREAM PROJECT (ĐỀ TÀI 16) — OBSERVABILITY & TESTING 
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
            print(f"  [CẢNH BÁO] Không thể tự cài đặt qua pip: {e}. Bạn hãy chạy: pip install {' '.join(missing)}")
    else:
        print("  [OK] Thư viện Python đầy đủ (Pillow & python-docx đã sẵn sàng).")


# ==============================================================================
# 2. KIỂM TRA VÀ KHỞI ĐỘNG DOCKER COMPOSE
# ==============================================================================
def is_docker_running() -> bool:
    try:
        res = subprocess.run(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return res.returncode == 0
    except Exception:
        return False


def start_docker_monitoring() -> bool:
    log_step(2, "Khởi động Docker Giám sát (Prometheus & Grafana)")
    if not is_docker_running():
        print("  [THÔNG BÁO] Máy chưa bật Docker Desktop hoặc chưa cài Docker.")
        print("  -> Hệ thống sẽ tự động chuyển sang chế độ STANDALONE:")
        print("     Vẫn sinh đầy đủ 5 ảnh Dashboard độ nét cao và 2 file báo cáo Word Word!")
        return False

    print("  [*] Docker đang chạy. Đang kiểm tra mạng stream-network...")
    try:
        # Tạo mạng stream-network nếu chưa có
        subprocess.run(["docker", "network", "create", "stream-network"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Khởi động container Prometheus và Grafana
        print("  [*] Đang khởi chạy Prometheus & Grafana qua docker compose...")
        cmd = ["docker", "compose", "-f", str(DOCKER_COMPOSE_MONITORING), "up", "-d"]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("  [OK] Container nhuttan-prometheus (cổng 9090) và nhuttan-grafana (cổng 3000) đã sẵn sàng!")
        return True
    except Exception as e:
        print(f"  [CẢNH BÁO] Không thể khởi động Docker Compose: {e}")
        return False


# ==============================================================================
# 3. LIVE METRICS SERVER TRÊN CỔNG 8000 (CHẠY NGẦM THREAD)
# ==============================================================================
def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def start_live_metrics_server():
    log_step(3, "Bật Live Metrics HTTP Server (Cổng 8000 phát metrics)")
    if is_port_in_use(8000):
        print("  [OK] Cổng 8000 đã có tiến trình phát metrics đang chạy, tiếp tục sử dụng.")
        return

    try:
        from NhutTan.src.start_live_metrics_server import MetricsHandler, PORT
        server = socketserver.TCPServer(("0.0.0.0", PORT), MetricsHandler)
        server.allow_reuse_address = True
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        print(f"  [OK] Live Metrics Server đã chạy ngầm tại: http://localhost:{PORT}/metrics")
    except Exception as e:
        print(f"  [CẢNH BÁO] Không thể mở Live Metrics Server: {e}")


# ==============================================================================
# 4. SINH TOÀN BỘ 5 ẢNH DASHBOARD & BIỂU ĐỒ (PNG)
# ==============================================================================
def generate_dashboards_images():
    log_step(4, "Sinh toàn bộ 5 Ảnh Dashboard & Biểu đồ chất lượng cao (PNG)")
    try:
        from NhutTan.src.generate_all_dashboards import main as render_all
        render_all()
        print(f"  [OK] Toàn bộ 5 ảnh biểu đồ đã được lưu tại: {KET_QUA_DIR}")
    except Exception as e:
        print(f"  [LỖI] Sinh ảnh Dashboard thất bại: {e}")


# ==============================================================================
# 5. XUẤT 2 FILE BÁO CÁO WORD CHUẨN MẪU (.DOCX)
# ==============================================================================
def export_word_reports():
    log_step(5, "Xuất 2 File Báo cáo Word chuẩn định dạng (NoiDung_TanLuu & Code_TanLuu)")
    try:
        from NhutTan.src.build_exact_format_reports import build_noidung_tanluu, build_code_tanluu, OUT_NOIDUNG, OUT_CODE
        build_noidung_tanluu()
        build_code_tanluu()
        print(f"  [OK] Đã xuất xong các file Word tại: {KET_QUA_DIR}")
    except Exception as e:
        print(f"  [LỖI] Xuất báo cáo Word thất bại: {e}")


# ==============================================================================
# 6. TỰ ĐỘNG MỞ TRÌNH DUYỆT XEM GRAFANA
# ==============================================================================
def open_browser_grafana(docker_ok: bool, auto_open: bool = True):
    log_step(6, "Mở giao diện Web Grafana")
    grafana_url = "http://localhost:3000"
    db_baseline_url = f"{grafana_url}/d/tanluu-infra-baseline/6f14c7b2-4219-55de-a126-058a9828de2d"
    
    if docker_ok and auto_open:
        print(f"  [*] Đang mở trình duyệt vào Dashboard 01...")
        time.sleep(1.5)
        try:
            webbrowser.open(db_baseline_url)
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
        print("📌 [ĐƯỜNG DẪN TRUY CẬP GRAFANA WEB LIVE]:")
        print(" • Trang chủ Grafana   : http://localhost:3000 (Tài khoản: admin / admin)")
        print(" • Dashboard 01 (Hạ Tầng) : http://localhost:3000/d/tanluu-infra-baseline/6f14c7b2-4219-55de-a126-058a9828de2d")
        print(" • Dashboard 02 (Spark)   : http://localhost:3000/d/tanluu-spark-performance/c16c4360-6f9a-522b-97c0-57491f1c0b8c")
        print(" • Dashboard 03 (Ép Tải)  : http://localhost:3000/d/cloudstream-tuan5-stresstest/3173cc78-5370-507f-9343-28c59e231848")
        print(" • Prometheus UI          : http://localhost:9090 (Targets: http://localhost:9090/targets)")
        print(" • Live Metrics Endpoint  : http://localhost:8000/metrics\n")
    
    print("📁 [CÁC FILE BÁO CÁO VÀ ẢNH ĐÃ XUẤT TẠI NhutTan/ket_qua/]:")
    print(f" 📄 File 1 Word (Nội dung) : {KET_QUA_DIR / 'NoiDung_TanLuu.docx'}")
    print(f" 📄 File 2 Word (Code)     : {KET_QUA_DIR / 'Code_TanLuu.docx'}")
    print(f" 📈 5 Ảnh Dashboard PNG    : dashboard_01_baseline, dashboard_02_spark,")
    print("                             bieu_do_ep_tai_backpressure, bieu_do_tai_nguyen_cpu_ram,")
    print("                             bieu_do_latency_vs_throughput\n")
    
    print("================================================================================")
    print(" 💡 Máy đang tiếp tục phát metrics thời gian thực cho Grafana...")
    print("    Nhấn [Ctrl + C] bất kỳ lúc nào để dừng chương trình.")
    print("================================================================================\n")


# ==============================================================================
# HÀM DỪNG HỆ THỐNG (FLAG --stop)
# ==============================================================================
def stop_docker_monitoring():
    print("\n[*] Đang dừng các container Giám sát Docker...")
    try:
        subprocess.run(["docker", "compose", "-f", str(DOCKER_COMPOSE_MONITORING), "down"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[OK] Đã dừng toàn bộ container nhuttan-prometheus và nhuttan-grafana an toàn.")
    except Exception as e:
        print(f"[CẢNH BÁO] Không thể dừng container: {e}")


# ==============================================================================
# ENTRYPOINT CHÍNH
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Trình khởi chạy Tất-Cả-Trong-1 của Tân Lưu (Đề tài 16).")
    parser.add_argument("--stop", action="store_true", help="Dừng cụm container Docker monitoring")
    parser.add_argument("--no-docker", action="store_true", help="Không khởi động Docker (chỉ sinh ảnh và word)")
    parser.add_argument("--no-browser", action="store_true", help="Không tự động mở trình duyệt web")
    args = parser.parse_args()

    print_banner()

    if args.stop:
        stop_docker_monitoring()
        return

    # Bước 1: Kiểm tra thư viện
    check_and_install_dependencies()

    # Bước 2: Docker
    docker_ok = False
    if not args.no_docker:
        docker_ok = start_docker_monitoring()

    # Bước 3: Metrics Server cổng 8000
    start_live_metrics_server()

    # Bước 4: Sinh ảnh Dashboard PNG
    generate_dashboards_images()

    # Bước 5: Xuất Báo cáo Word
    export_word_reports()

    # Bước 6: Mở trình duyệt
    open_browser_grafana(docker_ok, auto_open=not args.no_browser)

    # Hiển thị bảng tổng kết
    print_summary(docker_ok)

    # Giữ tiến trình chạy để tiếp tục phục vụ metrics
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Đã nhận lệnh thoát (Ctrl+C). Tạm biệt!")


if __name__ == "__main__":
    main()

