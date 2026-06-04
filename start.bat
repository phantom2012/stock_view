@echo off
chcp 65001 >nul

if "%1"=="" goto :usage

if "%1"=="1" goto :run
if "%1"=="2" goto :run
if "%1"=="3" goto :run

echo [ERROR] Invalid option: %1
goto :usage

:run
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
goto :eof

:usage
echo ========================================
echo   Stock Analysis System - Launcher
echo ========================================
echo.
echo Usage: start.bat [option] [args...]
echo   1  - Frontend (dashboard -^> npm run dev)
echo   2  - Backend  (backend -^> python main.py [args...])
echo   3  - Data sync service (data-sync-service -^> python main.py [args...])
echo.
echo Examples:
echo   start.bat 1
echo   start.bat 2 --reload
echo   start.bat 3 --sync-only
goto :eof
