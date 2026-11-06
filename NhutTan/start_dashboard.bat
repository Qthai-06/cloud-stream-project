@echo off
REM Script khoi chay 1-click tren Windows (Tan Luu)
cd /d "%~dp0\.."

python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [LOI] May ban chua cai dat Python. Vui long cai Python 3.8+ tai https://www.python.org/!
    pause
    exit /b 1
)

python NhutTan\start_dashboard.py %*
pause

