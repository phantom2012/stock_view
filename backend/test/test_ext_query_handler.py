import sys
sys.path.insert(0, r'f:\gupiao\stock_view\backend')

from external_data.ext_data_query_handle import get_query_handler, QUERY_API_TYPE

print(f"当前使用的API类型: {QUERY_API_TYPE}")

# 获取查询处理器
handler = get_query_handler()

# 测试竞价数据查询
print("\n测试竞价数据查询（使用Tushare接口）...")
symbol = "SHSE.600487"
trade_date = "2026-04-22"

auction_data = handler.get_auction_data(symbol, trade_date)
if auction_data is not None and not auction_data.empty:
    print(f"获取到 {len(auction_data)} 条竞价数据")
    print(auction_data)
else:
    print("竞价数据查询失败")

# 测试日线数据查询（使用掘金接口）
print("\n测试日线数据查询（使用掘金接口）...")
symbol = "SHSE.600487"
start_date = "2026-04-10"
end_date = "2026-04-22"

daily_data = handler.get_daily_data(symbol, start_date, end_date)
if daily_data is not None and not daily_data.empty:
    print(f"获取到 {len(daily_data)} 条日线数据")
    print(daily_data.head())
else:
    print("日线数据查询失败")