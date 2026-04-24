import sys
sys.path.append('..')

from gm.api import get_symbol_infos, set_token
import pandas as pd

# 设置token
GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"
set_token(GOLD_MINER_API_TOKEN)
print('✓ Token设置成功\n')

# 测试获取002834的基本信息
symbol = 'SZSE.002834'
print(f'尝试获取股票: {symbol}\n')

try:
    # get_symbol_infos需要sec_type1参数
    # sec_type1: 1010=股票, 1020=基金, 1030=期货, 1040=期权等
    result = get_symbol_infos(symbols=symbol, sec_type1=1010)
    
    print(f'返回类型: {type(result)}')
    
    if result is not None:
        if isinstance(result, pd.DataFrame):
            print(f'\nDataFrame列名: {list(result.columns)}')
            print(f'\n数据行数: {len(result)}')
            print('\n完整信息:')
            for col in result.columns:
                print(f'  {col}: {result.iloc[0].get(col)}')
        else:
            print(f'\n返回内容: {result}')
    else:
        print('返回空结果')
        
except Exception as e:
    print(f'调用失败: {e}')
    import traceback
    traceback.print_exc()

# 也测试一下批量获取
print('\n' + '='*60)
print('测试批量获取多个股票:')
try:
    symbols_list = ['SZSE.002834', 'SHSE.600666', 'SZSE.000001']
    result = get_symbol_infos(symbols=','.join(symbols_list), sec_type1=1010)
    
    if result is not None and isinstance(result, pd.DataFrame):
        print(f'\n获取到 {len(result)} 只股票的信息')
        if 'sec_name' in result.columns:
            print('\n股票名称:')
            for idx, row in result.iterrows():
                print(f"  {row.get('symbol', 'N/A')}: {row.get('sec_name', '未知')}")
    else:
        print(f'返回结果: {result}')
except Exception as e:
    print(f'批量获取失败: {e}')
