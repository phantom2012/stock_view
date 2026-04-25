#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
个股信息数据结构和缓存池
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from gm.api import *


class DailyStockData:
    """
    个股单日数据结构类
    """
    
    def __init__(self):
        """初始化单日数据"""
        # 日K线数据
        self.daily_data: Optional[pd.DataFrame] = None
        
        # 竞价数据
        self.auction_data: Optional[Dict[str, Any]] = None
        
        # 涨幅数据
        self.today_gain: Optional[float] = None
        self.next_day_gain: Optional[float] = None
        
        # 性能数据
        self.max_gain: float = 0.0
        self.max_daily_gain: float = 0.0
        
        # 升浪形态得分
        self.rising_wave_score: int = 0


class StockData:
    """
    个股数据结构类
    """
    
    def __init__(self, symbol: str):
        """
        初始化个股数据
        
        Args:
            symbol: 股票代码，如 'SZSE.002636'
        """
        self.symbol = symbol
        self.code = symbol.split('.')[-1] if '.' in symbol else symbol
        
        # 基本信息
        self.name: str = '未知'
        
        # 分钟级K线数据（非按日期缓存）
        self.minute_data: Optional[pd.DataFrame] = None
        
        # 按日期存储的单日数据
        self.daily_data_map: Dict[str, DailyStockData] = {}
        
        # 数据更新时间
        self.update_time: Optional[datetime] = None
        
        # 数据缓存标记
        self._minute_cached: bool = False
    
    def set_name(self, name: str):
        """设置股票名称"""
        self.name = name
    
    def _get_or_create_daily_data(self, date_key: str) -> DailyStockData:
        """获取或创建单日数据实例"""
        if date_key not in self.daily_data_map:
            self.daily_data_map[date_key] = DailyStockData()
        return self.daily_data_map[date_key]
    
    def set_daily_data(self, data: pd.DataFrame, date_key: str):
        """设置日K线数据"""
        daily_data = self._get_or_create_daily_data(date_key)
        daily_data.daily_data = data
        self.update_time = datetime.now()
    
    def get_daily_data(self, date_key: str) -> Optional[pd.DataFrame]:
        """获取日K线数据"""
        daily_data = self.daily_data_map.get(date_key)
        return daily_data.daily_data if daily_data else None
    
    def set_minute_data(self, data: pd.DataFrame):
        """设置分钟级K线数据"""
        self.minute_data = data
        self._minute_cached = True
        self.update_time = datetime.now()
    
    def set_auction_data(self, auction_data: Dict[str, Any], date_key: str):
        """设置竞价数据"""
        daily_data = self._get_or_create_daily_data(date_key)
        daily_data.auction_data = auction_data
        self.update_time = datetime.now()
    
    def get_auction_data(self, date_key: str) -> Optional[Dict[str, Any]]:
        """获取竞价数据"""
        daily_data = self.daily_data_map.get(date_key)
        return daily_data.auction_data if daily_data else None
    
    def set_gains(self, today_gain: Optional[float], next_day_gain: Optional[float], date_key: str):
        """设置涨幅数据"""
        daily_data = self._get_or_create_daily_data(date_key)
        daily_data.today_gain = today_gain
        daily_data.next_day_gain = next_day_gain
    
    def get_gains(self, date_key: str) -> Optional[Dict[str, Optional[float]]]:
        """获取涨幅数据"""
        daily_data = self.daily_data_map.get(date_key)
        if daily_data:
            return {
                'today_gain': daily_data.today_gain,
                'next_day_gain': daily_data.next_day_gain
            }
        return None
    
    def set_performance_data(self, max_gain: float, max_daily_gain: float, date_key: str):
        """设置性能数据"""
        daily_data = self._get_or_create_daily_data(date_key)
        daily_data.max_gain = max_gain
        daily_data.max_daily_gain = max_daily_gain
    
    def set_rising_wave_score(self, score: int, date_key: str):
        """设置升浪形态得分"""
        daily_data = self._get_or_create_daily_data(date_key)
        daily_data.rising_wave_score = score
    
    def is_daily_cached(self, date_key: str) -> bool:
        """检查日K线数据是否已缓存"""
        daily_data = self.daily_data_map.get(date_key)
        return daily_data is not None and daily_data.daily_data is not None
    
    def is_minute_cached(self) -> bool:
        """检查分钟级数据是否已缓存"""
        return self._minute_cached
    
    def is_auction_cached(self, date_key: str) -> bool:
        """检查竞价数据是否已缓存"""
        daily_data = self.daily_data_map.get(date_key)
        return daily_data is not None and daily_data.auction_data is not None
    
    def get_latest_close(self, date_key: str) -> Optional[float]:
        """获取最新收盘价"""
        daily_data = self.daily_data_map.get(date_key)
        if daily_data and daily_data.daily_data is not None and not daily_data.daily_data.empty:
            return daily_data.daily_data.iloc[-1]['close']
        return None
    
    def get_latest_date(self, date_key: str) -> Optional[str]:
        """获取最新数据日期"""
        daily_data = self.daily_data_map.get(date_key)
        if daily_data and daily_data.daily_data is not None and not daily_data.daily_data.empty:
            # 尝试多个可能的日期列名
            for col in ['eob', 'time', 'date', 'datetime']:
                if col in daily_data.daily_data.columns:
                    return str(daily_data.daily_data.iloc[-1][col])
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'code': self.code,
            'name': self.name,
            'update_time': self.update_time.strftime('%Y-%m-%d %H:%M:%S') if self.update_time else None
        }
    
    def clear_cache(self):
        """清除缓存"""
        self.daily_data_map.clear()
        self.minute_data = None
        self._minute_cached = False


class StockDataCache:
    """
    个股信息缓存池
    使用单例模式，确保全局只有一个缓存池实例
    """
    
    _instance = None
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化缓存池"""
        if self._initialized:
            return
        self._stock_map: Dict[str, StockData] = {}
        self._api_token_set: bool = False
        self._instruments_cache: Optional[pd.DataFrame] = None
        self._instruments_loaded: bool = False
        self._initialized = True
    
    def _load_instruments_cache(self):
        """
        加载并缓存 instruments 数据（仅加载一次）
        """
        if self._instruments_loaded:
            return
        
        print("加载 instruments 缓存...")
        try:
            # 获取所有股票列表（只获取A股）
            instruments = get_instruments(exchanges=['SHSE', 'SZSE'], sec_types=[1], df=True)
            if instruments is not None and not instruments.empty:
                self._instruments_cache = instruments
                self._instruments_loaded = True
                print(f"instruments 缓存加载完成，共 {len(self._instruments_cache)} 条数据")
        except Exception as e:
            print(f"加载 instruments 缓存失败: {e}")
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        从缓存中获取股票信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票信息字典
        """
        if not self._instruments_loaded:
            self._load_instruments_cache()
        
        if self._instruments_cache is None or self._instruments_cache.empty:
            return None
        
        # 从缓存中查找
        # symbol 格式可能是 'SZSE.002636' 或 '002636'
        symbol_code = symbol.split('.')[-1] if '.' in symbol else symbol
        
        # 在缓存中查找匹配的记录
        for _, row in self._instruments_cache.iterrows():
            if str(row.get('symbol', '')) == symbol:
                return row.to_dict()
            # 也尝试匹配 sec_code
            if str(row.get('sec_code', '')) == symbol_code:
                return row.to_dict()
        
        return None
    
    def get_symbol_infos_bulk(self, symbols: list) -> Dict[str, Dict[str, Any]]:
        """
        批量从缓存获取股票信息
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            股票代码到信息的映射
        """
        if not self._instruments_loaded:
            self._load_instruments_cache()
        
        result = {}
        if self._instruments_cache is None or self._instruments_cache.empty:
            return result
        
        # 转换为集合提高查找效率
        symbol_set = set(symbols)
        
        for _, row in self._instruments_cache.iterrows():
            sym = str(row.get('symbol', ''))
            if sym in symbol_set:
                result[sym] = row.to_dict()
        
        return result
    
    def set_api_token(self, api_key: str):
        """
        设置API Token
        
        Args:
            api_key: API密钥
        """
        if not self._api_token_set:
            set_token(api_key)
            self._api_token_set = True
    
    def get_stock(self, symbol: str) -> Optional[StockData]:
        """
        获取个股数据对象
        
        Args:
            symbol: 股票代码
            
        Returns:
            StockData对象，如果不存在返回None
        """
        return self._stock_map.get(symbol)
    
    def get_or_create_stock(self, symbol: str) -> StockData:
        """
        获取或创建个股数据对象
        
        Args:
            symbol: 股票代码
            
        Returns:
            StockData对象
        """
        if symbol not in self._stock_map:
            self._stock_map[symbol] = StockData(symbol)
        return self._stock_map[symbol]
    
    def has_daily_data(self, symbol: str) -> bool:
        """
        检查是否有日K线缓存
        
        Args:
            symbol: 股票代码
            
        Returns:
            bool
        """
        stock = self._stock_map.get(symbol)
        return stock.is_daily_cached() if stock else False
    
    def has_minute_data(self, symbol: str) -> bool:
        """
        检查是否有分钟级数据缓存
        
        Args:
            symbol: 股票代码
            
        Returns:
            bool
        """
        stock = self._stock_map.get(symbol)
        return stock.is_minute_cached() if stock else False
    
    def is_auction_cached(self, symbol: str, trade_date: datetime) -> bool:
        """
        检查竞价数据是否已缓存
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            
        Returns:
            bool: 是否已缓存
        """
        stock = self._stock_map.get(symbol)
        date_key = trade_date.strftime('%Y-%m-%d')
        return stock.is_auction_cached(date_key) if stock else False
    
    def fetch_daily_data(self, symbol: str, trade_date: datetime, recent_days: int = 10) -> Optional[pd.DataFrame]:
        """
        获取日K线数据（带缓存）
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_days: 近N个交易日
            
        Returns:
            DataFrame
        """
        stock = self.get_or_create_stock(symbol)
        date_key = trade_date.strftime('%Y-%m-%d')
        
        # 检查缓存
        if stock.is_daily_cached(date_key):
            return stock.get_daily_data(date_key)
        
        # 计算日期范围
        start_date = trade_date - timedelta(days=recent_days * 2)
        
        # 调用API获取数据
        data = history(
            symbol=symbol,
            frequency='1d',
            start_time=start_date.strftime('%Y-%m-%d'),
            end_time=trade_date.strftime('%Y-%m-%d'),
            fields='symbol,open,close,high,low,volume,eob',
            adjust=ADJUST_PREV,
            df=True
        )
        
        if data is not None and not data.empty:
            stock.set_daily_data(data, date_key)
        
        return data
    
    def fetch_minute_data(self, symbol: str, trade_date: datetime, 
                         start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """
        获取分钟级K线数据（带缓存）
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            DataFrame
        """
        stock = self.get_or_create_stock(symbol)
        
        # 构建缓存key
        cache_key = f"{trade_date.strftime('%Y-%m-%d')}_{start_time}_{end_time}"
        
        # 检查缓存
        if stock.is_minute_cached() and stock.minute_data is not None:
            # 检查缓存的数据日期是否匹配
            if hasattr(stock.minute_data, 'iloc') and len(stock.minute_data) > 0:
                first_eob = str(stock.minute_data.iloc[0].get('eob', ''))
                if trade_date.strftime('%Y-%m-%d') in first_eob:
                    return stock.minute_data
        
        # 调用API获取数据
        date_str = trade_date.strftime('%Y-%m-%d')
        data = history(
            symbol=symbol,
            frequency='1m',
            start_time=f"{date_str} {start_time}",
            end_time=f"{date_str} {end_time}",
            fields='symbol,open,close,high,low,volume,eob',
            adjust=ADJUST_PREV,
            df=True
        )
        
        if data is not None and not data.empty:
            stock.set_minute_data(data)
        
        return data
    
    def fetch_auction_data(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        获取竞价数据（带缓存）
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            
        Returns:
            竞价数据字典
        """
        stock = self.get_or_create_stock(symbol)
        date_key = trade_date.strftime('%Y-%m-%d')
        
        # 检查缓存
        if stock.is_auction_cached(date_key):
            return stock.get_auction_data(date_key)
        
        # 获取日K线数据
        daily_data = self.fetch_daily_data(symbol, trade_date)
        
        # 获取14:57-15:00的分钟数据
        auction_start = "14:56:00"
        auction_end = "15:00:00"
        minute_data = self.fetch_minute_data(symbol, trade_date, auction_start, auction_end)
        
        auction_data = None
        if minute_data is not None and not minute_data.empty:
            auction_data = {
                'symbol': symbol,
                'auction_start_price': minute_data.iloc[0]['open'],
                'auction_end_price': minute_data.iloc[-1]['close'],
                'volume': minute_data.iloc[-1]['volume']
            }
        elif daily_data is not None and not daily_data.empty:
            # 如果分钟数据获取失败，使用日K线的收盘价作为参考
            auction_data = {
                'symbol': symbol,
                'auction_start_price': daily_data.iloc[-1]['close'],
                'auction_end_price': daily_data.iloc[-1]['close'],
                'volume': 0
            }
        
        if auction_data:
            stock.set_auction_data(auction_data, date_key)
        
        return auction_data
    
    def fetch_stock_name(self, symbol: str) -> str:
        """
        获取股票名称（带缓存）

        Args:
            symbol: 股票代码

        Returns:
            股票名称
        """
        stock = self.get_or_create_stock(symbol)

        if stock.name != '未知':
            return stock.name

        # 从缓存的 instruments 中获取
        info = self.get_symbol_info(symbol)
        if info:
            stock.name = info.get('sec_name', '未知')

        return stock.name

    def fetch_stock_names_bulk(self, symbols: list) -> Dict[str, str]:
        """
        批量获取股票名称（高效版本）

        Args:
            symbols: 股票代码列表

        Returns:
            股票代码到名称的映射
        """
        stock_names = {}

        if not symbols:
            return stock_names

        # 从缓存的 instruments 中批量获取
        infos = self.get_symbol_infos_bulk(symbols)
        for symbol, info in infos.items():
            code = symbol.split('.')[-1] if '.' in symbol else symbol
            stock_names[code] = info.get('sec_name', '未知')

        # 缓存到 StockData 对象中
        for symbol in symbols:
            stock = self.get_or_create_stock(symbol)
            code = symbol.split('.')[-1] if '.' in symbol else symbol
            if stock.name == '未知' and code in stock_names:
                stock.name = stock_names[code]

        return stock_names
    
    def set_stock_name(self, symbol: str, name: str):
        """
        设置股票名称
        
        Args:
            symbol: 股票代码
            name: 股票名称
        """
        stock = self.get_or_create_stock(symbol)
        stock.set_name(name)
    
    def get_all_stocks(self) -> Dict[str, StockData]:
        """
        获取所有缓存的个股数据
        
        Returns:
            Dict[str, StockData]
        """
        return self._stock_map
    
    def clear_all_cache(self):
        """清除所有缓存"""
        for stock in self._stock_map.values():
            stock.clear_cache()
        print("所有缓存已清除")
    
    def remove_stock(self, symbol: str):
        """
        移除个股缓存
        
        Args:
            symbol: 股票代码
        """
        if symbol in self._stock_map:
            del self._stock_map[symbol]
            print(f"已移除 {symbol} 的缓存")
    
    def get_cache_count(self) -> int:
        """
        获取缓存的股票数量
        
        Returns:
            int
        """
        return len(self._stock_map)
    
    def print_cache_info(self):
        """打印缓存信息"""
        print(f"\n=== 缓存池信息 ===")
        print(f"缓存股票数量: {len(self._stock_map)}")
        for symbol, stock in self._stock_map.items():
            print(f"  {symbol} ({stock.name}):")
            print(f"    - 日K线缓存: {stock.is_daily_cached()}")
            print(f"    - 分钟级缓存: {stock.is_minute_cached()}")
            print(f"    - 竞价数据缓存: {stock.is_auction_cached()}")
            print(f"    - 升浪得分: {stock.rising_wave_score}")


# 全局缓存池实例
_global_cache = None

def get_stock_cache() -> StockDataCache:
    """
    获取全局缓存池实例
    
    Returns:
        StockDataCache单例
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = StockDataCache()
    return _global_cache