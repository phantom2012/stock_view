import sys
sys.path.append('..')

from stock_cache import get_stock_cache
import pandas as pd

# 获取缓存实例
cache = get_stock_cache()

# 加载instruments
instruments = cache._load_instruments_cache()

if instruments is not None and not instruments.empty:
    print(f'instruments总数: {len(instruments)}')
    print(f'列名: {list(instruments.columns)}')
    
    # 搜索包含2834的股票
    print('\n搜索包含"2834"的股票:')
    mask = instruments['sec_id'].astype(str).str.contains('2834', na=False)
    results = instruments[mask]
    
    if not results.empty:
        print(f'找到 {len(results)} 条记录:')
        print(results[['symbol', 'sec_id', 'sec_name', 'exchange']].head(10))
    else:
        print('未找到')
        
    # 检查SZSE前缀的股票
    print('\n搜索SZSE.00开头的股票数量:')
    szse_00 = instruments[instruments['symbol'].astype(str).str.startswith('SZSE.00', na=False)]
    print(f'数量: {len(szse_00)}')
    if len(szse_00) > 0:
        print('前10个:')
        print(szse_00[['symbol', 'sec_name']].head(10))
