# 手动解析参数，避免 PowerShell 对 --xxx 参数名的特殊解析
$mode = if ($args.Count -ge 1) { $args[0] } else { "" }
$extraArgs = if ($args.Count -gt 1) { $args[1..($args.Count - 1)] } else { @() }

# 切换到脚本所在目录（项目根目录），支持从任意路径调用
$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $rootDir

switch ($mode) {
    "1" {
        Write-Host "[INFO] Starting frontend service..."
        Set-Location "dashboard"
        if (-not (Test-Path "node_modules")) {
            Write-Host "[INFO] Installing dependencies..."
            & npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[ERROR] npm install failed"
                exit 1
            }
        }
        Write-Host "[INFO] Running: npm run dev"
        & npm run dev
    }
    "2" {
        Write-Host "[INFO] Starting backend service (127.0.0.1:8000)..."
        Set-Location "backend"
        Write-Host "[INFO] Running: python main.py $($extraArgs -join ' ')"
        & python main.py $extraArgs
    }
    "3" {
        Write-Host "[INFO] Starting data sync service..."
        Set-Location "data-sync-service"
        Write-Host "[INFO] Running: python main.py $($extraArgs -join ' ')"
        & python main.py $extraArgs
    }
    default {
        Write-Host "========================================"
        Write-Host "  Stock Analysis System - Launcher"
        Write-Host "========================================"
        Write-Host ""
        Write-Host "Usage: .\start.ps1 [option] [args...]"
        Write-Host "  1 - Frontend (dashboard -> npm run dev)"
        Write-Host "  2 - Backend  (backend -> python main.py [args...])"
        Write-Host "  3 - Data sync service (data-sync-service -> python main.py [args...])"
        Write-Host ""
        Write-Host "Examples:"
        Write-Host "  .\start.ps1 1"
        Write-Host "  .\start.ps1 2 --reload"
        Write-Host "  .\start.ps1 3 --sync-only"
    }
}
