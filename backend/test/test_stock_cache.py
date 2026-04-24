#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试 StockDataCache 数据保存功能
"""

import sys
import os

# 添加上级目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stock_cache import get_stock_cache
from datetime import datetime
import pandas as pd

# 获取缓存实例
cache = get_stock_cache()

# 测试股票代码
symbol = 'SHSE.600487'

print(f"=== 测试股票: {symbol} ===")

# 1. 测试获取股票名称
print("\n1. 测试获取股票名称:")
name = cache.get_stock_name(symbol)
print(f"股票名称: {name}")

# 2. 测试获取历史数据
print("\n2. 测试获取历史数据:")
try:
    history_data = cache.get_history_data(symbol, days=5)
    if history_data is not None and not history_data.empty:
        print(f"获取到 {len(history_data)} 条历史数据")
        print(history_data)
    else:
        print("未获取到历史数据")
except Exception as e:
    print(f"获取历史数据失败: {e}")

# 3. 测试获取分钟数据
print("\n3. 测试获取分钟数据:")
try:
    trade_date = datetime(2026, 4, 17)
    minute_data = cache.get_minute_data(symbol, trade_date, '09:30:00', '10:00:00')
    if minute_data is not None and not minute_data.empty:
        print(f"获取到 {len(minute_data)} 条分钟数据")
        print(minute_data.head())
    else:
        print("未获取到分钟数据")
except Exception as e:
    print(f"获取分钟数据失败: {e}")

# 4. 测试获取tick数据
print("\n4. 测试获取tick数据:")
try:
    trade_date = datetime(2026, 4, 17)
    tick_data = cache.get_tick_data(symbol, trade_date, '09:25:00', '09:31:00')
    if tick_data is not None and not tick_data.empty:
        print(f"获取到 {len(tick_data)} 条tick数据")
        print(tick_data.head())
    else:
        print("未获取到tick数据")
except Exception as e:
    print(f"获取tick数据失败: {e}")

# 5. 测试获取竞价数据
print("\n5. 测试获取竞价数据:")
try:
    trade_date = datetime(2026, 4, 17)
    auction_data = cache.get_auction_data(symbol, trade_date)
    print(f"竞价数据: {auction_data}")
except Exception as e:
    print(f"获取竞价数据失败: {e}")

print("\n=== 测试完成 ===")