#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试后端API调用，模拟用户浏览个股详情的情况
"""

import sys
import os
import requests

# 后端API地址
API_BASE = "http://localhost:8000"

# 测试股票代码
test_stocks = [
    "600487",  # 亨通光电
    "000001",  # 平安银行
    "600036"   # 招商银行
]

def test_get_stock_info(code):
    """测试获取股票信息接口"""
    url = f"{API_BASE}/get-stock-info"
    params = {"code": code}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"股票信息 ({code}):")
            print(f"  名称: {data.get('stock_name')}")
            print(f"  今日涨幅: {data.get('today_gain')}")
            print(f"  交易日期: {data.get('trade_date')}")
            return True
        else:
            print(f"获取股票信息失败 ({code}): {response.status_code}")
            return False
    except Exception as e:
        print(f"请求失败 ({code}): {e}")
        return False

def test_get_stock_history(code, days=5):
    """测试获取股票历史数据接口"""
    url = f"{API_BASE}/get-stock-history"
    params = {"code": code, "days": days}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"股票历史数据 ({code}):")
            print(f"  数据条数: {len(data)}")
            if data:
                print(f"  最近日期: {data[0].get('date')}")
                print(f"  开盘价: {data[0].get('open')}")
                print(f"  开盘成交额: {data[0].get('open_amount')}")
            return True
        else:
            print(f"获取股票历史数据失败 ({code}): {response.status_code}")
            return False
    except Exception as e:
        print(f"请求失败 ({code}): {e}")
        return False

def test_health_check():
    """测试健康检查接口"""
    url = f"{API_BASE}/"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print("健康检查:")
            print(f"  状态: {data.get('status')}")
            print(f"  最后运行时间: {data.get('last_run')}")
            return True
        else:
            print(f"健康检查失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"健康检查请求失败: {e}")
        return False

if __name__ == "__main__":
    print("=== 测试后端API ===")
    print(f"后端地址: {API_BASE}")
    print()
    
    # 测试健康检查
    print("1. 健康检查:")
    test_health_check()
    print()
    
    # 测试获取股票信息
    print("2. 测试股票信息:")
    for stock in test_stocks:
        test_get_stock_info(stock)
        print()
    
    # 测试获取股票历史数据
    print("3. 测试股票历史数据:")
    for stock in test_stocks:
        test_get_stock_history(stock)
        print()
    
    print("=== 测试完成 ===")