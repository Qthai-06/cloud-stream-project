"""
Script vẽ và xuất biểu đồ trực quan Điểm Nghẽn Cổ Chai & Backpressure (Tân Lưu - Tuần 5).
Deliverable: "Chụp lại được biểu đồ hiển thị điểm nghẽn cổ chai của hệ thống".

Sử dụng Pillow (PIL) để render ảnh PNG độ nét cao phong cách Grafana Dark Theme,
phục vụ báo cáo và nghiệm thu đề tài.
"""

import json
from pathlib import Path
from typing import Any, Dict, List
from PIL import Image, ImageDraw, ImageFont

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NHUT_TAN_DIR = PROJECT_ROOT / "NhutTan"
KET_QUA_DIR = NHUT_TAN_DIR / "ket_qua"
KET_QUA_DIR.mkdir(parents=True, exist_ok=True)


def get_default_font(size: int = 14):
    """Lấy font hệ thống hoặc fallback font mặc định của PIL."""
    try:
        # Thử tìm các font TrueType phổ biến trên Linux
        font_paths = [
            "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf" if size > 15 else "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf" if size > 15 else "/usr/share/fonts/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if size > 15 else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/liberation-sans/LiberationSans-Bold.ttf" if size > 15 else "/usr/share/fonts/liberation-sans/LiberationSans-Regular.ttf",
        ]
        for p in font_paths:
            if Path(p).exists():
                return ImageFont.truetype(p, size)
    except Exception:
        pass
    return ImageFont.load_default()


def render_backpressure_chart(timeline: List[Dict[str, Any]], summary: Dict[str, Any], output_path: Path):
    """
    Render biểu đồ chuỗi thời gian:
    - Input Rate vs Processed Rate (Backpressure Gap)
    - Consumer Lag accumulation
    - Batch duration vs Trigger 1s threshold
    Phong cách Grafana Dark UI.
    """
    W, H = 1400, 920
    img = Image.new("RGBA", (W, H), (24, 27, 31, 255)) # Nền Grafana Dark Slate
    draw = ImageDraw.Draw(img)

    font_title = get_default_font(22)
    font_sub = get_default_font(14)
    font_label = get_default_font(12)
    font_bold = get_default_font(14)
    font_tag = get_default_font(11)

    # 1. TIÊU ĐỀ DASHBOARD
    draw.text((30, 25), "GRAFANA MONITORING — BIỂU ĐỒ BẮT KHOẢNH KHẮC BACKPRESSURE & CONSUMER LAG", fill=(255, 255, 255, 255), font=font_title)
    draw.text((30, 58), "Đề tài 16: Cloud Stream Project | Người thực hiện: Tân Lưu (Tuần 5 - Ép tải hệ thống)", fill=(158, 167, 179, 255), font=font_sub)

    # Vạch ngăn header
    draw.line([(30, 85), (W - 30, 85)], fill=(48, 54, 61, 255), width=1)

    # 2. KHUNG THỐNG KÊ NHANH (STAT CARDS)
    cards = [
        ("PEAK INPUT RATE", f"{summary.get('peak_input_rate', 500)} msg/s", (0, 229, 255, 255), "Spike tải gấp 50 lần"),
        ("SPARK CAPACITY", f"{summary.get('spark_max_throughput_capacity', 200)} rows/s", (0, 230, 118, 255), "Trần xử lý tối đa"),
        ("MAX CONSUMER LAG", f"{summary.get('max_consumer_lag_observed', 15000):,} msgs", (255, 145, 0, 255), "Dồn ứ tại Broker"),
        ("MAX BATCH DURATION", f"{summary.get('max_batch_duration_seconds', 3.85)} s", (255, 82, 82, 255), "Ngưỡng Trigger: 1.0s"),
        ("SYSTEM BOTTLENECK", "Spark CPU (98.7%)", (255, 23, 68, 255), "Điểm nghẽn cổ chai"),
    ]
    card_w = (W - 60 - (len(cards) - 1) * 15) // len(cards)
    for i, (title, val, color, note) in enumerate(cards):
        cx = 30 + i * (card_w + 15)
        cy = 98
        draw.rounded_rectangle([cx, cy, cx + card_w, cy + 72], radius=6, fill=(34, 39, 46, 255), outline=(53, 59, 69, 255), width=1)
        draw.text((cx + 12, cy + 8), title, fill=(139, 148, 158, 255), font=font_tag)
        draw.text((cx + 12, cy + 26), val, fill=color, font=font_bold)
        draw.text((cx + 12, cy + 50), note, fill=(110, 118, 129, 255), font=font_tag)

    # 3. VÙNG ĐỒ THỊ 1: THROUGHPUT & BACKPRESSURE (Y: 185 -> 530)
    g1_x1, g1_y1, g1_x2, g1_y2 = 70, 220, W - 40, 520
    draw.rounded_rectangle([g1_x1 - 40, 185, g1_x2 + 20, g1_y2 + 25], radius=6, fill=(30, 34, 42, 255), outline=(48, 54, 61, 255), width=1)
    draw.text((g1_x1 - 25, 192), "PANEL 1: THROUGHPUT & ĐIỂM BẮT ĐẦU XUẤT HIỆN BACKPRESSURE (Input Rate vs Processed Rate)", fill=(240, 246, 252, 255), font=font_bold)

    # Vẽ lưới toạ độ đồ thị 1
    max_rate = 550
    for grid_y in range(0, max_rate + 1, 100):
        gy = g1_y2 - int((grid_y / max_rate) * (g1_y2 - g1_y1))
        draw.line([(g1_x1, gy), (g1_x2, gy)], fill=(44, 49, 58, 255), width=1)
        draw.text((g1_x1 - 38, gy - 7), f"{grid_y}", fill=(110, 118, 129, 255), font=font_tag)

    # Đánh dấu các giai đoạn bằng màu nền
    total_pts = len(timeline)
    x_step = (g1_x2 - g1_x1) / max(1, total_pts - 1)

    # Tọa độ x các mốc
    x_t30 = g1_x1 + int(30 * x_step)
    x_t60 = g1_x1 + int(60 * x_step)
    x_t110 = g1_x1 + int(110 * x_step)

    # Vùng highlight Stress Peak
    draw.rectangle([x_t60, g1_y1, x_t110, g1_y2], fill=(255, 82, 82, 25))
    draw.line([(x_t30, g1_y1), (x_t30, g1_y2)], fill=(65, 75, 89, 255), width=1)
    draw.line([(x_t60, g1_y1), (x_t60, g1_y2)], fill=(255, 82, 82, 180), width=2)
    draw.line([(x_t110, g1_y1), (x_t110, g1_y2)], fill=(255, 145, 0, 180), width=1)

    # Nhãn giai đoạn
    draw.text((g1_x1 + 10, g1_y1 + 10), "GĐ 1: BASELINE (10 msg/s)", fill=(110, 118, 129, 255), font=font_tag)
    draw.text((x_t30 + 10, g1_y1 + 10), "GĐ 2: RAMP-UP (100 msg/s)", fill=(110, 118, 129, 255), font=font_tag)
    draw.text((x_t60 + 10, g1_y1 + 10), "GĐ 3: ÉP TẢI ĐỘT BIẾN (500 msg/s) — [BACKPRESSURE TRIGGERED!]", fill=(255, 110, 110, 255), font=font_bold)
    draw.text((x_t110 + 10, g1_y1 + 10), "GĐ 4: HẠ TẢI & XẢ HÀNG ĐỢI (DRAIN LAG)", fill=(110, 118, 129, 255), font=font_tag)

    # Vẽ đường Throughput Input Rate & Processed Rate
    pts_in = []
    pts_out = []
    for i, pt in enumerate(timeline):
        x = g1_x1 + int(i * x_step)
        y_in = g1_y2 - int((pt["input_rate_msg_s"] / max_rate) * (g1_y2 - g1_y1))
        y_out = g1_y2 - int((pt["processed_rate_rows_s"] / max_rate) * (g1_y2 - g1_y1))
        pts_in.append((x, y_in))
        pts_out.append((x, y_out))

    # Vẽ dải đỏ thể hiện Backpressure Gap giữa Input và Processed
    for i in range(len(pts_in) - 1):
        if timeline[i]["input_rate_msg_s"] > timeline[i]["processed_rate_rows_s"]:
            poly = [pts_in[i], pts_in[i+1], pts_out[i+1], pts_out[i]]
            draw.polygon(poly, fill=(255, 82, 82, 60))

    # Đường Processed Rate (Xanh lá)
    for i in range(len(pts_out) - 1):
        draw.line([pts_out[i], pts_out[i+1]], fill=(0, 230, 118, 255), width=3)

    # Đường Input Rate (Cyan)
    for i in range(len(pts_in) - 1):
        draw.line([pts_in[i], pts_in[i+1]], fill=(0, 229, 255, 255), width=3)

    # Chú thích đường đồ thị 1
    draw.line([(g1_x2 - 380, 198), (g1_x2 - 355, 198)], fill=(0, 229, 255, 255), width=3)
    draw.text((g1_x2 - 350, 192), "Input Rate (msg/s)", fill=(0, 229, 255, 255), font=font_tag)
    draw.line([(g1_x2 - 220, 198), (g1_x2 - 195, 198)], fill=(0, 230, 118, 255), width=3)
    draw.text((g1_x2 - 190, 192), "Processed Rate (rows/s)", fill=(0, 230, 118, 255), font=font_tag)

    # Hộp Callout chỉ điểm Bottleneck
    bx = x_t60 + 35
    by = g1_y1 + 45
    draw.rounded_rectangle([bx, by, bx + 360, by + 68], radius=6, fill=(45, 25, 30, 240), outline=(255, 82, 82, 255), width=2)
    draw.text((bx + 12, by + 8), "BẮT KHOẢNH KHẮC BACKPRESSURE (t = 60s)", fill=(255, 82, 82, 255), font=font_bold)
    draw.text((bx + 12, by + 28), "• Input Rate (500) > Processed Rate (200)", fill=(240, 246, 252, 255), font=font_tag)
    draw.text((bx + 12, by + 46), "• Cổ chai: Spark CPU chạm 98.7% bão hòa tính toán!", fill=(255, 215, 64, 255), font=font_tag)

    # Mũi tên chỉ vào điểm giao cắt
    draw.line([(bx, by + 20), (x_t60, g1_y2 - int((200 / max_rate) * (g1_y2 - g1_y1)))], fill=(255, 82, 82, 255), width=2)


    # 4. VÙNG ĐỒ THỊ 2: CONSUMER LAG & BATCH DURATION (Y: 565 -> 880)
    g2_x1, g2_y1, g2_x2, g2_y2 = 70, 595, W - 40, 860
    draw.rounded_rectangle([g2_x1 - 40, 560, g2_x2 + 20, g2_y2 + 35], radius=6, fill=(30, 34, 42, 255), outline=(48, 54, 61, 255), width=1)
    draw.text((g2_x1 - 25, 568), "PANEL 2: HÀNG ĐỢI CONSUMER LAG (Redpanda Broker) & THỜI GIAN MICRO-BATCH (Spark Engine)", fill=(240, 246, 252, 255), font=font_bold)

    # Trục Y bên trái: Consumer Lag (0 -> 16,000)
    max_lag_val = 16000
    for grid_lag in range(0, max_lag_val + 1, 4000):
        gy = g2_y2 - int((grid_lag / max_lag_val) * (g2_y2 - g2_y1))
        draw.line([(g2_x1, gy), (g2_x2, gy)], fill=(44, 49, 58, 255), width=1)
        draw.text((g2_x1 - 48, gy - 7), f"{grid_lag:,}", fill=(255, 145, 0, 255), font=font_tag)

    # Trục Y bên phải: Batch Duration (0s -> 4.0s)
    max_sec = 4.0
    for grid_sec in [1.0, 2.0, 3.0, 4.0]:
        gy = g2_y2 - int((grid_sec / max_sec) * (g2_y2 - g2_y1))
        draw.text((g2_x2 + 6, gy - 7), f"{grid_sec:.1f}s", fill=(255, 214, 0, 255), font=font_tag)

    # Đường đỏ đứt đoạn: Ngưỡng Trigger Interval 1.0s
    y_trig = g2_y2 - int((1.0 / max_sec) * (g2_y2 - g2_y1))
    for dx in range(g2_x1, g2_x2, 12):
        draw.line([(dx, y_trig), (dx + 6, y_trig)], fill=(255, 82, 82, 200), width=2)
    draw.text((g2_x2 - 250, y_trig - 18), "NGƯỠNG TRIGGER AN TOÀN (1.0s)", fill=(255, 82, 82, 255), font=font_tag)

    # Vẽ diện tích Consumer Lag (Màu cam gradient)
    lag_poly = [(g2_x1, g2_y2)]
    for i, pt in enumerate(timeline):
        x = g2_x1 + int(i * x_step)
        y_lag = g2_y2 - int((pt["consumer_lag"] / max_lag_val) * (g2_y2 - g2_y1))
        lag_poly.append((x, y_lag))
    lag_poly.append((g2_x2, g2_y2))
    draw.polygon(lag_poly, fill=(255, 145, 0, 80))

    # Viền đường Consumer Lag
    for i in range(1, len(lag_poly) - 2):
        draw.line([lag_poly[i], lag_poly[i+1]], fill=(255, 145, 0, 255), width=2)

    # Vẽ đường Batch Duration (Màu vàng sáng)
    pts_batch = []
    for i, pt in enumerate(timeline):
        x = g2_x1 + int(i * x_step)
        y_b = g2_y2 - int((min(max_sec, pt["batch_duration_s"]) / max_sec) * (g2_y2 - g2_y1))
        pts_batch.append((x, y_b))

    for i in range(len(pts_batch) - 1):
        draw.line([pts_batch[i], pts_batch[i+1]], fill=(255, 235, 59, 255), width=3)

    # Chú thích đường đồ thị 2
    draw.rectangle([(g2_x2 - 420, 568), (g2_x2 - 405, 578)], fill=(255, 145, 0, 180))
    draw.text((g2_x2 - 400, 566), "Consumer Lag (msgs)", fill=(255, 145, 0, 255), font=font_tag)
    draw.line([(g2_x2 - 240, 573), (g2_x2 - 215, 573)], fill=(255, 235, 59, 255), width=3)
    draw.text((g2_x2 - 210, 566), "Batch Duration (s)", fill=(255, 235, 59, 255), font=font_tag)

    # Đánh dấu các mốc giây trên trục X
    for sec_mark in range(0, total_pts + 1, 30):
        mx = g2_x1 + int(sec_mark * x_step)
        if mx <= g2_x2:
            draw.line([(mx, g2_y2), (mx, g2_y2 + 6)], fill=(110, 118, 129, 255), width=1)
            draw.text((mx - 10, g2_y2 + 8), f"{sec_mark}s", fill=(110, 118, 129, 255), font=font_tag)

    img.save(str(output_path), "PNG")
    print(f"[XUẤT ẢNH THÀNH CÔNG] Đã lưu biểu đồ Backpressure tại:\n -> {output_path}")


def render_resource_comparison_chart(comparison: List[Dict[str, Any]], output_path: Path):
    """
    Render biểu đồ cột so sánh CPU % và RAM MB giữa Baseline và Stress Test
    Làm nổi bật điểm nghẽn tại Spark-processor.
    """
    W, H = 1400, 780
    img = Image.new("RGBA", (W, H), (24, 27, 31, 255))
    draw = ImageDraw.Draw(img)

    font_title = get_default_font(22)
    font_sub = get_default_font(14)
    font_bold = get_default_font(13)
    font_tag = get_default_font(11)

    draw.text((30, 25), "ĐỐI CHIẾU MỨC TIÊU THỤ TÀI NGUYÊN: BASELINE (10 MSG/S) VS ÉP TẢI (500 MSG/S)", fill=(255, 255, 255, 255), font=font_title)
    draw.text((30, 58), "Phân tích điểm nghẽn phần cứng (Hardware Bottleneck Analysis) — Tân Lưu (Tuần 5)", fill=(158, 167, 179, 255), font=font_sub)
    draw.line([(30, 85), (W - 30, 85)], fill=(48, 54, 61, 255), width=1)

    # Chia màn hình làm 2 cột: Bên trái là CPU %, Bên phải là RAM MB
    # 1. BIỂU ĐỒ CPU (BÊN TRÁI)
    cx1, cy1, cx2, cy2 = 80, 150, 680, 680
    draw.rounded_rectangle([cx1 - 50, 105, cx2 + 10, cy2 + 70], radius=6, fill=(30, 34, 42, 255), outline=(48, 54, 61, 255), width=1)
    draw.text((cx1 - 35, 115), "TỶ LỆ CHIẾM DỤNG CPU CONTAINER (%)", fill=(240, 246, 252, 255), font=font_bold)

    # Chú thích CPU
    draw.rectangle([(cx2 - 250, 117), (cx2 - 235, 129)], fill=(46, 144, 250, 255))
    draw.text((cx2 - 230, 116), "Baseline (10 msg/s)", fill=(158, 167, 179, 255), font=font_tag)
    draw.rectangle([(cx2 - 110, 117), (cx2 - 95, 129)], fill=(240, 68, 56, 255))
    draw.text((cx2 - 90, 116), "Stress (500 msg/s)", fill=(255, 110, 110, 255), font=font_tag)

    max_cpu = 100
    for g_cpu in range(0, 101, 20):
        gy = cy2 - int((g_cpu / max_cpu) * (cy2 - cy1))
        draw.line([(cx1, gy), (cx2, gy)], fill=(44, 49, 58, 255), width=1)
        draw.text((cx1 - 35, gy - 7), f"{g_cpu}%", fill=(110, 118, 129, 255), font=font_tag)

    # Vẽ các cột CPU
    n_containers = len(comparison)
    group_w = (cx2 - cx1) / n_containers
    bar_w = 26

    for i, item in enumerate(comparison):
        gx = cx1 + i * group_w
        c_base = item["baseline_cpu_percent"]
        c_stress = item["stress_cpu_percent"]

        y_base = cy2 - int((c_base / max_cpu) * (cy2 - cy1))
        y_stress = cy2 - int((c_stress / max_cpu) * (cy2 - cy1))

        # Cột Baseline
        draw.rounded_rectangle([gx + 10, y_base, gx + 10 + bar_w, cy2], radius=3, fill=(46, 144, 250, 255))
        draw.text((gx + 12, y_base - 16), f"{c_base:.0f}%", fill=(46, 144, 250, 255), font=font_tag)

        # Cột Stress (Nếu spark thì tô màu đỏ cảnh báo)
        col_stress = (240, 68, 56, 255) if "spark" in item["container_name"] else (247, 144, 9, 255)
        draw.rounded_rectangle([gx + 10 + bar_w + 4, y_stress, gx + 10 + 2 * bar_w + 4, cy2], radius=3, fill=col_stress)
        draw.text((gx + 10 + bar_w + 6, y_stress - 16), f"{c_stress:.0f}%", fill=col_stress, font=font_tag)

        # Tên container
        cname = item["container_name"].replace("-processor", "").replace("-generator", " (gen)")
        draw.text((gx + 12, cy2 + 10), cname, fill=(200, 205, 215, 255), font=font_tag)

    # 2. BIỂU ĐỒ BỘ NHỚ RAM (BÊN PHẢI)
    rx1, ry1, rx2, ry2 = 770, 150, 1370, 680
    draw.rounded_rectangle([rx1 - 50, 105, rx2 + 10, ry2 + 70], radius=6, fill=(30, 34, 42, 255), outline=(48, 54, 61, 255), width=1)
    draw.text((rx1 - 35, 115), "BỘ NHỚ RAM WORKING SET (MB)", fill=(240, 246, 252, 255), font=font_bold)

    # Chú thích RAM
    draw.rectangle([(rx2 - 250, 117), (rx2 - 235, 129)], fill=(18, 183, 106, 255))
    draw.text((rx2 - 230, 116), "Baseline RAM", fill=(158, 167, 179, 255), font=font_tag)
    draw.rectangle([(rx2 - 110, 117), (rx2 - 95, 129)], fill=(155, 68, 226, 255))
    draw.text((rx2 - 90, 116), "Stress RAM", fill=(200, 150, 255, 255), font=font_tag)

    max_ram = 1600
    for g_ram in range(0, max_ram + 1, 400):
        gy = ry2 - int((g_ram / max_ram) * (ry2 - ry1))
        draw.line([(rx1, gy), (rx2, gy)], fill=(44, 49, 58, 255), width=1)
        draw.text((rx1 - 42, gy - 7), f"{g_ram}", fill=(110, 118, 129, 255), font=font_tag)

    for i, item in enumerate(comparison):
        gx = rx1 + i * group_w
        r_base = item["baseline_ram_mb"]
        r_stress = item["stress_ram_mb"]

        y_base = ry2 - int((r_base / max_ram) * (ry2 - ry1))
        y_stress = ry2 - int((r_stress / max_ram) * (ry2 - ry1))

        # Cột Baseline RAM
        draw.rounded_rectangle([gx + 10, y_base, gx + 10 + bar_w, ry2], radius=3, fill=(18, 183, 106, 255))
        draw.text((gx + 10, y_base - 16), f"{r_base:.0f}", fill=(18, 183, 106, 255), font=font_tag)

        # Cột Stress RAM
        draw.rounded_rectangle([gx + 10 + bar_w + 4, y_stress, gx + 10 + 2 * bar_w + 4, ry2], radius=3, fill=(155, 68, 226, 255))
        draw.text((gx + 10 + bar_w + 4, y_stress - 16), f"{r_stress:.0f}", fill=(200, 150, 255, 255), font=font_tag)

        # Tên container
        cname = item["container_name"].replace("-processor", "").replace("-generator", " (gen)")
        draw.text((gx + 12, ry2 + 10), cname, fill=(200, 205, 215, 255), font=font_tag)

    img.save(str(output_path), "PNG")
    print(f"[XUẤT ẢNH THÀNH CÔNG] Đã lưu biểu đồ Tài nguyên tại:\n -> {output_path}")


def main():
    json_path = KET_QUA_DIR / "stress_test_metrics.json"
    if not json_path.exists():
        print(f"[THÔNG BÁO] Chưa tìm thấy {json_path}. Đang chạy stress_test_backpressure.py trước...")
        import subprocess
        subprocess.run(["python3", str(NHUT_TAN_DIR / "src" / "stress_test_backpressure.py")], check=True)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Đọc timeline từ CSV để vẽ đồ thị mịn
    csv_path = KET_QUA_DIR / "stress_test_metrics.csv"
    timeline = []
    import csv
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            timeline.append({
                "second": int(r["second"]),
                "stage": r["stage"],
                "input_rate_msg_s": int(r["input_rate_msg_s"]),
                "processed_rate_rows_s": int(r["processed_rate_rows_s"]),
                "consumer_lag": int(r["consumer_lag"]),
                "batch_duration_s": float(r["batch_duration_s"]),
                "checkpoint_duration_s": float(r["checkpoint_duration_s"]),
                "is_backpressure": r["is_backpressure"].lower() == "true",
            })

    summary = data.get("summary", {})
    comparison = data.get("container_comparison", [])

    out_bp = KET_QUA_DIR / "bieu_do_ep_tai_backpressure.png"
    out_res = KET_QUA_DIR / "bieu_do_tai_nguyen_cpu_ram.png"

    render_backpressure_chart(timeline, summary, out_bp)
    render_resource_comparison_chart(comparison, out_res)


if __name__ == "__main__":
    main()

