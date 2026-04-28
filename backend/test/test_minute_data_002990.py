"""
测试外部数据查询接口的分钟数据获取
测试股票代码：002990
测试日期：2026-04-22
测试时间段：14:55:00 - 15:00:00
"""
import sys
sys.path.insert(0, r'f:\gupiao\stock_view\backend')

from external_data.ext_data_query_handle import get_query_handler, QUERY_API_TYPE

print("=" * 60)
print("测试外部数据查询接口 - 分钟数据获取")
print("=" * 60)
print(f"当前使用的API类型: {QUERY_API_TYPE}")
print()

# 获取查询处理器
handler = get_query_handler()

# 测试参数
symbol = "SZSE.002990"  # 002990是深交所股票
trade_date = "2026-04-22"
start_time = "14:56:00"
end_time = "15:00:00"

print(f"股票代码: {symbol}")
print(f"交易日期: {trade_date}")
print(f"时间段: {start_time} - {end_time}")
print()

# 调用get_minute_data接口
print("开始调用 get_minute_data 接口...")
print("-" * 60)

try:
    minute_data = handler.get_minute_data(symbol, trade_date, start_time, end_time)
    
    print("-" * 60)
    print("\n返回结果：")
    
    if minute_data is not None and not minute_data.empty:
        print(f"\n✅ 成功获取到 {len(minute_data)} 条分钟数据")
        print("\n完整数据：")
        print(minute_data.to_string())
        
        print("\n" + "=" * 60)
        print("数据统计信息：")
        print("=" * 60)
        print(f"数据条数: {len(minute_data)}")
        print(f"时间范围: {minute_data['eob'].min()} ~ {minute_data['eob'].max()}")
        print(f"开盘价范围: {minute_data['open'].min():.2f} ~ {minute_data['open'].max():.2f}")
        print(f"收盘价范围: {minute_data['close'].min():.2f} ~ {minute_data['close'].max():.2f}")
        print(f"最高价范围: {minute_data['high'].min():.2f} ~ {minute_data['high'].max():.2f}")
        print(f"最低价范围: {minute_data['low'].min():.2f} ~ {minute_data['low'].max():.2f}")
        print(f"成交量总和: {minute_data['volume'].sum():.0f}")
        print(f"成交额总和: {minute_data['amount'].sum():.2f}")
        
        print("\n" + "=" * 60)
        print("首尾数据对比：")
        print("=" * 60)
        print("第一条数据：")
        print(minute_data.iloc[0])
        print("\n最后一条数据：")
        print(minute_data.iloc[-1])
        
    else:
        print("\n❌ 未获取到数据")
        print("可能原因：")
        print("  1. 该日期不是交易日")
        print("  2. 该股票在该日期无交易数据")
        print("  3. API接口调用失败")
        
except Exception as e:
    print(f"\n❌ 调用接口时发生异常: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
