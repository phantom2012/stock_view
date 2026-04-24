import sys
sys.path.insert(0, r'f:\gupiao\stock_view\backend')

from stock_cache import get_stock_cache
from datetime import datetime

print("初始化StockCache...")
cache = get_stock_cache()

# 测试获取竞价数据
print("\n测试获取股票竞价数据...")
symbol = "SHSE.600487"
trade_date = datetime(2026, 4, 22)

data = cache.get_auction_data(symbol, trade_date)
print(f"竞价数据: {data}")
print(f"早盘竞价金额: {data['auction_amount']}")
print(f"开盘成交额: {data['open_amount']}")