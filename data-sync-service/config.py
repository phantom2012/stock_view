"""
数据同步服务配置
"""
import os
from typing import Dict, Any

# ==================== 数据库配置 ====================
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///F:/gupiao/_sqlite_stock_data/stock.db')

# ==================== 外部接口配置 ====================

# 掘金 API 配置
GOLD_MINER_CONFIG = {
    'token': os.getenv('GM_TOKEN', ''),
    'host': os.getenv('GM_HOST', 'localhost'),
    'port': int(os.getenv('GM_PORT', '8000')),
}

# Tushare API 配置
TUSHARE_CONFIG = {
    'token': os.getenv('TUSHARE_TOKEN', ''),
}

# ==================== 同步器配置 ====================
# 股票信息同步配置
STOCK_INFO_CONFIG: Dict[str, Any] = {
    'cron_hour': 16,                  # 定时同步时间（小时）
    'cron_minute': 0,                 # 定时同步时间（分钟）
    'start_delay_minutes': 0,          # 启动后延迟执行时间（分钟），None或0表示不延迟
}

# 资金流向同步配置
MONEY_FLOW_CONFIG: Dict[str, Any] = {
    'interval_minutes': 15,           # 定时同步间隔（分钟）
    'default_days': 30,              # 默认同步天数
    'batch_size': 100,               # 每批处理的股票数量
    'start_delay_minutes': 0,         # 启动后延迟执行时间（分钟），None或0表示不延迟
}


# 日线数据同步配置
DAILY_DATA_CONFIG: Dict[str, Any] = {
    'interval_minutes': 5,           # 定时同步间隔（分钟）
    'default_days': 90,              # 默认同步天数（可配置）
    'start_delay_minutes': 0,         # 启动后延迟执行时间（分钟），None或0表示不延迟
}

# 竞价数据同步配置
AUCTION_DATA_CONFIG: Dict[str, Any] = {
    'cron_hour': 9,                  # 定时同步时间（小时）
    'cron_minute': 35,               # 定时同步时间（分钟）
    'start_delay_minutes': 0,         # 启动后延迟执行时间（分钟），None或0表示不延迟
}

# 分钟数据同步配置
MINUTE_DATA_CONFIG: Dict[str, Any] = {
    'cron_hour': 16,                 # 定时同步时间（小时）
    'cron_minute': 0,                # 定时同步时间（分钟）
    'start_delay_minutes': 0,         # 启动后延迟执行时间（分钟），None或0表示不延迟
}

# 数据清理配置
CLEAR_DATA_CONFIG: Dict[str, Any] = {
    'interval_seconds': 120,          # 扫描间隔（秒）
}

# ==================== 通知表扫描配置 ====================
NOTIFY_SCANNER_CONFIG: Dict[str, Any] = {
    'interval_seconds': 5,           # 扫描间隔（秒）
    'max_wait_seconds': 8,           # backend 最大等待时间（秒）
}

# ==================== Backend 通知配置 ====================
BACKEND_CONFIG: Dict[str, Any] = {
    'base_url': os.getenv('BACKEND_URL', 'http://localhost:8000'),
    'sync_complete_endpoint': '/api/data/sync-complete',
    'timeout_seconds': 10,           # 请求超时时间（秒）
}

# ==================== 日志配置 ====================
LOG_CONFIG: Dict[str, Any] = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': os.getenv('LOG_FILE', 'data-sync-service.log'),
}
