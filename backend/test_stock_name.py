#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试股票名称获取
"""
import sys
sys.path.insert(0, 'f:/gupiao/stock_view/backend')

from stock_cache import get_stock_cache

cache = get_stock_cache()
cache.set_api_token("2e664976b46df6a0903672349c30226ac68e7bf3")

# 测试几个显示为"未知"的股票
test_symbols = ["SHSE.605081", "SZSE.000004", "SHSE.603843"]

print("=" * 80)
print("测试单个股票名称获取:")
print("=" * 80)
for symbol in test_symbols:
    name = cache.get_stock_name(symbol)
    print(f"{symbol} -> {name}")

print("\n" + "=" * 80)
print("测试批量获取股票名称:")
print("=" * 80)
names_map = cache.fetch_stock_names_bulk(test_symbols)
for code, name in names_map.items():
    print(f"{code} -> {name}")

print("\n" + "=" * 80)
print("检查 instruments 缓存结构:")
print("=" * 80)
if cache._instruments_cache is not None and not cache._instruments_cache.empty:
    print(f"缓存列名: {cache._instruments_cache.columns.tolist()}")
    print(f"\n前3行数据:")
    print(cache._instruments_cache.head(3))
    
    # 尝试查找 605081
    print("\n" + "=" * 80)
    print("查找 605081:")
    print("=" * 80)
    
    # 方法1: 通过 symbol 字段
    match1 = cache._instruments_cache[
        cache._instruments_cache['symbol'].str.endswith('.605081', na=False)
    ]
    print(f"方法1 (symbol like '%.605081'): {len(match1)} 条")
    if not match1.empty:
        print(match1[['symbol', 'sec_name', 'exchange', 'sec_id']])
    
    # 方法2: 通过 exchange + sec_id
    match2 = cache._instruments_cache[
        (cache._instruments_cache['exchange'] == 'SHSE') & 
        (cache._instruments_cache['sec_id'] == '605081')
    ]
    print(f"\n方法2 (exchange='SHSE' AND sec_id='605081'): {len(match2)} 条")
    if not match2.empty:
        print(match2[['symbol', 'sec_name', 'exchange', 'sec_id']])
    
    # 查看所有 SHSE 的 60 开头股票数量
    sh_60 = cache._instruments_cache[
        (cache._instruments_cache['exchange'] == 'SHSE') & 
        (cache._instruments_cache['sec_id'].str.startswith('60', na=False))
    ]
    print(f"\nSHSE 60开头的股票总数: {len(sh_60)}")
