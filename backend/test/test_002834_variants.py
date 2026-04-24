import sys
sys.path.append('..')

from gm.api import get_symbol_infos, set_token
import pandas as pd

# 设置token
GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"
set_token(GOLD_MINER_API_TOKEN)

print('测试不同的股票代码格式:\n')

# 尝试多种格式
test_symbols = [
    'SZSE.002834',
    '002834',
    'SZSE.002834.SZ',
]

for symbol in test_symbols:
    print(f'测试: {symbol}')
    try:
        result = get_symbol_infos(symbols=symbol, sec_type1=1010)
        if result and len(result) > 0:
            print(f'  ✓ 找到: {result[0].get("sec_name", "未知")}')
        else:
            print(f'  ✗ 未找到')
    except Exception as e:
        print(f'  ✗ 错误: {e}')
    print()

# 搜索所有包含2834的股票
print('\n' + '='*60)
print('搜索所有包含"2834"的股票:')
try:
    # 获取所有股票
    all_stocks = get_symbol_infos(sec_type1=1010)
    if all_stocks:
        print(f'总共有 {len(all_stocks)} 只股票')
        # 过滤包含2834的
        matched = [s for s in all_stocks if '2834' in str(s.get('sec_id', ''))]
        if matched:
            print(f'找到 {len(matched)} 只包含2834的股票:')
            for s in matched:
                print(f"  - {s.get('symbol')}: {s.get('sec_name')}")
        else:
            print('未找到包含2834的股票')
            
            # 显示前10个以0028开头的股票
            print('\n显示前10个以0028开头的股票:')
            matched_0028 = [s for s in all_stocks if str(s.get('sec_id', '')).startswith('0028')]
            for s in matched_0028[:10]:
                print(f"  - {s.get('symbol')}: {s.get('sec_name')}")
    else:
        print('未获取到股票数据')
except Exception as e:
    print(f'搜索失败: {e}')
    import traceback
    traceback.print_exc()
