#!/usr/bin/env bash
# Script khởi chạy 1-click cho macOS / Linux (Tân Lưu)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Sử dụng python3 hoặc python
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    echo "[LỖI] Máy bạn chưa cài đặt Python. Vui lòng cài Python 3.8+!"
    exit 1
fi

"$PYTHON_CMD" NhutTan/start_dashboard.py "$@"

