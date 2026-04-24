#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试股票缓存和次日涨幅计算
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from gm.api import *

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stock_data.stock_cache import StockDataCache

# 初始化API
set_token('2e664976b46df6a0903672349c30226ac68e7bf3')

def test_next_day_gain():
    """测试次日涨幅计算"""
    print("=== 测试次日涨幅计算 ===")
    
    # 测试股票和日期
    symbol = "SZSE.002980"  # 华盛昌
    trade_date = datetime(2026, 4, 15)
    
    print(f"测试股票: {symbol}")
    print(f"交易日期: {trade_date.strftime('%Y-%m-%d')}")
    
    # 计算下一个交易日
    next_trading_day = trade_date + timedelta(days=1)
    while next_trading_day.weekday() >= 5:  # 跳过周末
        next_trading_day += timedelta(days=1)
    print(f"下一个交易日: {next_trading_day.strftime('%Y-%m-%d')}")
    
    # 获取缓存实例
    cache = StockDataCache()
    
    # 获取当日数据
    print("\n1. 获取当日数据:")
    today_data = cache.fetch_daily_data(symbol, trade_date, 5)
    if today_data is not None and not today_data.empty:
        print(f"当日数据行数: {len(today_data)}")
        print(f"最后一行数据:")
        print(today_data.tail(1))
        
        # 获取当日收盘价
        today_close = today_data.iloc[-1]['close']
        print(f"\n当日收盘价: {today_close}")
    else:
        print("当日数据获取失败")
        return
    
    # 获取次日数据
    print("\n2. 获取次日数据:")
    next_data = cache.fetch_daily_data(symbol, next_trading_day, 1)
    if next_data is not None and not next_data.empty:
        print(f"次日数据行数: {len(next_data)}")
        print(f"次日数据:")
        print(next_data)
        
        # 获取次日收盘价
        next_close = next_data.iloc[-1]['close']
        print(f"\n次日收盘价: {next_close}")
    else:
        print("次日数据获取失败")
        return
    
    # 计算次日涨幅
    print("\n3. 计算次日涨幅:")
    if today_close != 0:
        next_day_gain = (next_close - today_close) / today_close * 100
        print(f"计算公式: (次日收盘价 - 当日收盘价) / 当日收盘价 * 100")
        print(f"计算过程: ({next_close} - {today_close}) / {today_close} * 100 = {next_day_gain:.2f}%")
        print(f"次日涨幅: {next_day_gain:.2f}%")
    else:
        print("当日收盘价为0，无法计算涨幅")
    
    # 测试缓存机制
    print("\n4. 测试缓存机制:")
    # 再次获取数据，应该从缓存读取
    cached_today_data = cache.fetch_daily_data(symbol, trade_date, 5)
    cached_next_data = cache.fetch_daily_data(symbol, next_trading_day, 1)
    print(f"缓存当日数据: {cached_today_data is not None}")
    print(f"缓存次日数据: {cached_next_data is not None}")

if __name__ == "__main__":
    test_next_day_gain()
