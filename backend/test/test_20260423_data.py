"""测试获取2026-04-23的股票数据"""
import sys
from pathlib import Path
from datetime import datetime

backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from stock_cache import get_stock_cache

# 获取缓存实例
stock_cache = get_stock_cache()

# 测试股票代码
test_symbol = "SHSE.600666"
trade_date = datetime(2026, 4, 23)
recent_days = 50

print(f'测试股票: {test_symbol}')
print(f'交易日期: {trade_date.strftime("%Y-%m-%d")}')
print(f'获取最近 {recent_days} 个交易日数据\n')

# 获取历史数据
try:
    history_data = stock_cache.get_history_data(test_symbol, recent_days)
    if history_data is not None and len(history_data) > 0:
        print(f'成功获取 {len(history_data)} 条数据')
        print('\n最近5条数据:')
        print(history_data.tail(5).to_string())
    else:
        print('未获取到数据')
except Exception as e:
    print(f'获取数据失败: {e}')
    import traceback
    traceback.print_exc()
