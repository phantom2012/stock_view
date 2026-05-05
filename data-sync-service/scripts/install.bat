@echo off
chcp 65001 >nul
echo ========================================
echo   data-sync-service 安装依赖
echo ========================================
echo.

cd /d "%~dp0.."

REM 创建虚拟环境
echo 创建虚拟环境...
python -m venv venv

REM 激活虚拟环境并安装依赖
echo 安装依赖...
call venv\Scripts\activate.bat
pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

echo.
echo 安装完成！
echo 使用 scripts\start.bat 启动服务

pause
