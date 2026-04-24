import sys
sys.path.append('..')

# 测试重构后的instruments获取功能
from external_data.ext_data_query_handle import get_query_handler

print("="*60)
print("测试通过ExternalDataQueryHandler获取股票基本信息")
print("="*60)

query_handler = get_query_handler()

# 测试get_instruments方法
print("\n调用 get_instruments...")
instruments = query_handler.get_instruments()

if instruments is not None and not instruments.empty:
    print(f"\n✓ 成功获取 {len(instruments)} 条股票数据")
    print(f"\n列名: {list(instruments.columns)}")
    
    # 显示前5条数据
    print("\n前5条数据:")
    for idx, row in instruments.head().iterrows():
        symbol = row.get('symbol', 'N/A')
        name = row.get('sec_name', '未知')
        print(f"  {symbol}: {name}")
    
    # 检查是否有002834
    if 'sec_id' in instruments.columns:
        matched = instruments[instruments['sec_id'] == '002834']
        if not matched.empty:
            print(f"\n✓ 找到002834: {matched.iloc[0].get('sec_name')}")
        else:
            print("\n✗ 未找到002834（这是正常的，掘金API数据缺失）")
else:
    print("\n✗ 获取失败或返回空数据")
