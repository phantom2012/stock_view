"""
数据同步服务配置
"""
import logging
import os
from typing import Dict, Any
# 禁用apscheduler执行器info日志
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

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
    'cron_hour': 15,                  # 定时同步时间（小时）
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

# 转强得分配置
# key: turn_start_date 距离 trade_date 的天数上限
# value: 对应的得分
TURN_START_SCORE_MAP: Dict[int, float] = {
    1: 50,
    2: 40,
    3: 30,
    4: 20,
    5: 10,
}

# 转强周期衰减配置
# 控制转强周期提前结束的两个补充条件
TURN_STRONG_CYCLE_CONFIG: Dict[str, Any] = {
    # 累计净流入衰退比例：当某日累计净流入 ≤ 周期内最大累计净流入 × 该比例时，当前转强周期结束
    'cumulative_decay_ratio': 0.6,
    # 日内流出触发比例：当某日净流出 > 周期内最大单日净流入 × 该比例时，当前转强周期结束
    'daily_outflow_ratio': 0.7,
}


# 日线数据同步配置
DAILY_DATA_CONFIG: Dict[str, Any] = {
    'interval_minutes': 5,           # 定时同步间隔（分钟）
    'default_days': 90,              # 默认同步天数（可配置）
    'start_delay_minutes': 5,         # 启动后延迟执行时间（分钟），None或0表示不延迟
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
