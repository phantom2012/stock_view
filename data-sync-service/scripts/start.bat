@echo off
chcp 65001 >nul
echo ========================================
echo   data-sync-service 数据同步服务
echo ========================================
echo.

cd /d "%~dp0.."

REM 检查虚拟环境
if exist "venv\Scripts\python.exe" (
    set PYTHON=venv\Scripts\python.exe
) else (
    set PYTHON=python
)

echo 启动 data-sync-service...
%PYTHON% main.py

pause
