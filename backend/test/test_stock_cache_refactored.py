import sys
sys.path.insert(0, r'f:\gupiao\stock_view\backend')

from stock_cache import get_stock_cache
from datetime import datetime

print("初始化StockCache...")
cache = get_stock_cache()

# 测试获取历史数据
print("\n测试获取股票历史数据...")
symbol = "SHSE.600487"
days = 10

data = cache.get_history_data(symbol, days=days)
if data is not None and not data.empty:
    print(f"获取到 {len(data)} 条历史数据")
    print(data)
else:
    print("历史数据查询失败")

# 验证数据是否保存到数据库
print("\n再次查询，应该从数据库读取...")
data2 = cache.get_history_data(symbol, days=days)
if data2 is not None and not data2.empty:
    print(f"从数据库获取到 {len(data2)} 条历史数据")