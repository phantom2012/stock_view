#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
后端股票过滤器模块
提供个股分析（StockAnalyzer）、波形形态分析（StockWaveAnalyzer）、
资金流向分析（StockMoneyAnalyzer）和批量筛选引擎（StockFilterEngine）
"""

from .stock_analyzer import StockAnalyzer
from .stock_wave_analyzer import StockWaveAnalyzer
from .stock_money_analyzer import StockMoneyAnalyzer
from .stock_filter_engine import StockFilterEngine

# 全局单例
_stock_filter_engine_instance = None


def get_stock_filter_engine() -> StockFilterEngine:
    """
    获取StockFilterEngine单例

    Returns:
        StockFilterEngine实例
    """
    global _stock_filter_engine_instance
    if _stock_filter_engine_instance is None:
        _stock_filter_engine_instance = StockFilterEngine()
    return _stock_filter_engine_instance
