"""
验证 data-sync-service 的导入是否正常
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

print("=" * 60)
print("验证 data-sync-service 导入...")
print("=" * 60)

# 验证 shared 包
try:
    from shared.db import get_session, DataSyncNotify
    print("[OK] shared.db 导入成功")
except Exception as e:
    print(f"[FAIL] shared.db 导入失败: {e}")

try:
    from shared.stock_code_convert import to_goldminer_symbol, to_tushare_ts_code
    print("[OK] shared.stock_code_convert 导入成功")
except Exception as e:
    print(f"[FAIL] shared.stock_code_convert 导入失败: {e}")

try:
    from shared.trade_date_util import TradeDateUtil
    print("[OK] shared.trade_date_util 导入成功")
except Exception as e:
    print(f"[FAIL] shared.trade_date_util 导入失败: {e}")

# 验证 config
try:
    from config import DATABASE_URL, MONEY_FLOW_CONFIG
    print(f"[OK] config 导入成功, DATABASE_URL={DATABASE_URL}")
except Exception as e:
    print(f"[FAIL] config 导入失败: {e}")

# 验证 external_data
try:
    from external_data import get_query_handler
    print("[OK] external_data 导入成功")
except Exception as e:
    print(f"[FAIL] external_data 导入失败: {e}")

# 验证 syncers
try:
    from syncers import MoneyFlowSyncer, StockInfoSyncer, DailyDataSyncer, AuctionDataSyncer, ClearDataSyncer
    print("[OK] syncers 导入成功")
except Exception as e:
    print(f"[FAIL] syncers 导入失败: {e}")

# 验证 scheduler
try:
    from scheduler import DataSyncScheduler
    print("[OK] scheduler 导入成功")
except Exception as e:
    print(f"[FAIL] scheduler 导入失败: {e}")

print("=" * 60)
print("验证完成")
print("=" * 60)
