#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
个股过滤器类
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
import pandas as pd
from gm.api import *

# 导入缓存池
from stock_data import get_stock_cache


class StockFilter:
    """
    个股过滤器类
    提供各种股票筛选条件的接口
    """
    
    def __init__(self):
        """
        初始化过滤器
        """
        self.cache = get_stock_cache()
    
    def check_performance(self, symbol: str, trade_date: datetime, 
                        recent_interval_days: int = 10, 
                        recent_interval_max_gain: float = 15, 
                        day_max_gain_days: int = 6,
                        day_max_gain: float = 8) -> Tuple[bool, float, float]:
        """
        检查股票近N个交易日的表现
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_interval_days: 近N个交易日
            recent_interval_max_gain: 区间最大涨幅阈值
            day_max_gain_days: 最近N日日内最大涨幅
            day_max_gain: 日内最大涨幅阈值
            
        Returns:
            Tuple[是否符合条件, 最大涨幅, 单日最大涨幅]
        """
        try:
            # 从缓存获取日K线数据
            data = self.cache.fetch_daily_data(symbol, trade_date, recent_interval_days)
            
            if data is not None and len(data) >= 2:  # 至少需要2天数据才能计算
                # 取最后recent_interval_days条数据
                data = data.tail(recent_interval_days)
                
                # 计算最小和最大收盘价
                min_close = data['close'].min()
                max_close = data['close'].max()
                
                # 计算最大收盘价与最小收盘价的涨幅
                max_gain = 0
                if min_close != 0:
                    max_gain = (max_close - min_close) / min_close * 100
                
                # 计算日内最大涨幅（使用分时最高价相对于昨日收盘价）
                max_daily_gain = 0.0
                if len(data) >= 2:
                    for i in range(1, min(day_max_gain_days + 1, len(data))):
                        current_row = data.iloc[-i]
                        prev_row = data.iloc[-i - 1]
                        
                        current_date_str = current_row['eob'].split(' ')[0] if isinstance(current_row['eob'], str) else str(current_row['eob'])[:10]
                        current_high = current_row['high']
                        prev_close = prev_row['close']
                        
                        if prev_close != 0:
                            day_gain = (current_high - prev_close) / prev_close * 100
                            if day_gain > max_daily_gain:
                                max_daily_gain = day_gain
                
                # 保留小数点后2位
                max_gain = round(max_gain, 2)
                max_daily_gain = round(max_daily_gain, 2)
                
                # 检查条件
                if max_gain > recent_interval_max_gain and max_daily_gain > day_max_gain:
                    return True, max_gain, max_daily_gain
                return False, max_gain, max_daily_gain
            return False, 0, 0
        except Exception as e:
            print(f"Error checking performance for {symbol}: {e}")
            return False, 0, 0
    
    def calculate_rising_wave_score(self, symbol: str, trade_date: datetime,
                                   recent_interval_days: int = 10) -> int:
        """
        计算升浪形态得分

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_interval_days: 近N个交易日

        Returns:
            升浪形态得分（0-100）
        """
        try:
            # 从缓存获取日K线数据
            data = self.cache.fetch_daily_data(symbol, trade_date, recent_interval_days)

            if data is not None and len(data) >= recent_interval_days:
                # 取最后 recent_interval_days 条数据
                data = data.tail(recent_interval_days)
                # 重置索引以避免 iloc 索引问题
                data = data.reset_index(drop=True)

                # 找到最低收盘价的索引
                min_close_loc = data['close'].idxmin()
                start_idx = min_close_loc

                if start_idx >= len(data) - 1:
                    # 最低价就是最后一天，无法计算
                    return 0

                # 从最低点的下一个交易日开始
                current_high = data.iloc[start_idx]['close']
                max_days_to_break = 0
                current_streak = 1  # 开始计数为1，因为当天也算一天

                for i in range(start_idx + 1, len(data)):
                    current_close = data.iloc[i]['close']
                    if current_close >= current_high:
                        # 突破前高，记录当前突破所需天数
                        current_high = current_close
                        if current_streak > max_days_to_break:
                            max_days_to_break = current_streak
                        # 重置计数，为下一次突破做准备
                        current_streak = 1  # 当天突破，记为1天
                    else:
                        # 未突破，记录天数
                        current_streak += 1

                # 检查最后一段未突破的天数（+1表示最好情况）
                if current_streak > max_days_to_break:
                    max_days_to_break = current_streak + 1

                # 如果超过4天未突破，不符合要求
                if max_days_to_break > 4:
                    return 0

                # 根据突破情况打分
                if max_days_to_break == 1:
                    # 1天就突破前高（每天都突破）
                    return 100
                elif max_days_to_break == 2:
                    # 2天突破
                    return 80
                elif max_days_to_break == 3:
                    # 3天突破
                    return 50
                elif max_days_to_break == 4:
                    # 4天突破
                    return 20
                else:
                    return 0
            return 0
        except Exception as e:
            print(f"Error calculating rising wave score for {symbol}: {e}")
            return 0
    
    def check_auction_condition(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        检查竞价条件（竞价结束价 >= 竞价开始价）
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            
        Returns:
            竞价数据字典，如果不符合条件返回None
        """
        # 从缓存获取竞价数据
        auction_data = self.cache.fetch_auction_data(symbol, trade_date)
        
        if auction_data and auction_data.get('auction_end_price', 0) >= auction_data.get('auction_start_price', 0):
            return auction_data
        
        return None
    
    def check_stock_gains(self, symbol: str, trade_date: datetime) -> Tuple[Optional[float], Optional[float]]:
        """
        获取股票的当日涨幅和次日涨幅
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            
        Returns:
            Tuple[当日涨幅, 次日涨幅]
        """
        try:
            # 从缓存获取日K线数据
            data = self.cache.fetch_daily_data(symbol, trade_date, 5)
            
            today_gain = None
            prev_close = None
            
            if data is not None and not data.empty and len(data) >= 2:
                # 当日收盘价
                today_close = data.iloc[-1]['close']
                # 昨日收盘价（前一行）
                prev_close = data.iloc[-2]['close']
                # 计算当日收盘价相对昨日收盘价的涨跌幅
                if prev_close != 0:
                    today_gain = (today_close - prev_close) / prev_close * 100
            
            # 获取次日数据
            next_day_gain = None
            # 检查是否是最后一个交易日（今天）
            if trade_date.date() < datetime.now().date():
                # 计算下一个交易日
                next_trading_day = trade_date + timedelta(days=1)
                while next_trading_day.weekday() >= 5:  # 跳过周末
                    next_trading_day += timedelta(days=1)
                
                # 获取当日收盘价
                today_close = None
                if data is not None and not data.empty:
                    today_close = data.iloc[-1]['close']
                
                if today_close is not None and today_close != 0:
                    # 从缓存获取次日数据
                    next_data = self.cache.fetch_daily_data(symbol, next_trading_day, 1)
                    if next_data is not None and not next_data.empty:
                        next_close = next_data.iloc[-1]['close']
                        # 计算次日收盘价相对当日收盘价的涨幅
                        next_day_gain = (next_close - today_close) / today_close * 100
            
            # 保留小数点后2位
            if today_gain is not None:
                today_gain = round(today_gain, 2)
            if next_day_gain is not None:
                next_day_gain = round(next_day_gain, 2)
            
            return today_gain, next_day_gain
        except Exception as e:
            print(f"Error getting stock gains for {symbol}: {e}")
            return None, None
    
    def check_stock_type(self, symbol: str) -> bool:
        """
        检查股票类型（只保留10cm涨跌幅的主板股票，排除科创板、创业板、北交所、ST股）
        
        Args:
            symbol: 股票代码
            
        Returns:
            bool: 是否符合条件
        """
        stock_code = symbol.split('.')[-1] if '.' in symbol else symbol
        
        # 1. 排除科创板 (688)
        if stock_code.startswith('688'):
            return False
        
        # 2. 排除创业板 (300, 301)
        if stock_code.startswith('300') or stock_code.startswith('301'):
            return False
        
        # 3. 排除北交所 (8, 4, 92开头)
        if stock_code.startswith('8') or stock_code.startswith('4') or stock_code.startswith('92'):
            return False
        
        # 4. 排除 ST 股票 (通过名称判断)
        try:
            stock_name = self.cache.get_stock_name(symbol)
            if 'ST' in stock_name or '*ST' in stock_name:
                return False
        except:
            pass
        
        # 5. 只保留主板股票 (60, 00, 002, 003等)
        return True
    
    def calculate_higher_score(self, auction_data: Dict[str, Any], 
                             rising_wave_score: int = 0) -> float:
        """
        计算超预期得分
        
        Args:
            auction_data: 竞价数据
            rising_wave_score: 升浪形态得分
            
        Returns:
            超预期得分
        """
        auction_start_price = auction_data.get('auction_start_price', 0)
        auction_end_price = auction_data.get('auction_end_price', 0)
        
        if auction_start_price != 0:
            base_score = round((auction_end_price - auction_start_price) / auction_start_price * 10000, 2)
            higher_score = base_score + rising_wave_score
            return higher_score
        
        return 0
    
    def filter_stocks(self, symbols: list, trade_date: datetime, 
                     weipan_exceed: int = 0, 
                     zaopan_exceed: int = 0, 
                     rising_wave: int = 0, 
                     config: Optional[Dict[str, Any]] = None) -> list:
        """
        综合筛选股票
        
        Args:
            symbols: 股票代码列表
            trade_date: 交易日期
            weipan_exceed: 尾盘超预期
            zaopan_exceed: 早盘超预期
            rising_wave: 上升形态
            config: 配置参数
            
        Returns:
            筛选结果列表
        """
        if config is None:
            config = {
                'recent_interval_days': 10,
                'recent_interval_max_gain': 15,
                'day_max_gain_days': 6,
                'day_max_gain': 8,
                'debug_stock': None  # 用于调试的股票代码
            }
        
        results = []
        
        for i, symbol in enumerate(symbols):
            if i % 30 == 0 and i > 0:
                print(f"已处理 {i}/{len(symbols)} 只股票")
            
            # 检查是否需要调试该股票
            debug_mode = False
            debug_stock = config.get('debug_stock')
            if debug_stock and (symbol == debug_stock or symbol.endswith('.' + debug_stock)):
                debug_mode = True
                print(f"\n=== 开始调试股票: {symbol} ===")
            
            # 检查股票类型
            if not self.check_stock_type(symbol):
                if debug_mode:
                    print(f"  [筛选] 股票类型不符合条件（创业板/科创板），被筛掉")
                continue
            
            # 检查性能条件
            performance_ok, max_gain, max_daily_gain = self.check_performance(
                symbol, trade_date,
                config['recent_interval_days'],
                config['recent_interval_max_gain'],
                config['day_max_gain_days'],
                config['day_max_gain']
            )
            
            if not performance_ok:
                if debug_mode:
                    print(f"  [筛选] 性能条件不符合，被筛掉")
                    print(f"    - 区间最大涨幅: {max_gain}% (阈值: {config['recent_interval_max_gain']}%)")
                    print(f"    - 日内最大涨幅: {max_daily_gain}% (阈值: {config['day_max_gain']}%)")
                continue
            
            if debug_mode:
                print(f"  [筛选] 性能条件符合")
                print(f"    - 区间最大涨幅: {max_gain}% (阈值: {config['recent_interval_max_gain']}%)")
                print(f"    - 日内最大涨幅: {max_daily_gain}% (阈值: {config['day_max_gain']}%)")
            
            # 检查竞价条件
            auction_data = self.check_auction_condition(symbol, trade_date)
            
            if not auction_data:
                if debug_mode:
                    print(f"  [筛选] 竞价条件不符合，被筛掉")
                continue
            
            if debug_mode:
                print(f"  [筛选] 竞价条件符合")
                print(f"    - 竞价开始价: {auction_data['auction_start_price']}")
                print(f"    - 竞价结束价: {auction_data['auction_end_price']}")
            
            # 获取涨幅数据
            today_gain, next_day_gain = self.check_stock_gains(symbol, trade_date)
            
            # 计算升浪形态得分
            rising_wave_score = 0
            if rising_wave == 1:
                rising_wave_score = self.calculate_rising_wave_score(
                    symbol, trade_date, config['day_max_gain_days']
                )
                # 只有得分大于0的股票才符合条件
                if rising_wave_score <= 0:
                    if debug_mode:
                        print(f"  [筛选] 升浪形态得分不符合，被筛掉")
                        print(f"    - 升浪形态得分: {rising_wave_score}")
                    continue

                if debug_mode:
                    print(f"  [筛选] 升浪形态得分符合")
                    print(f"    - 升浪形态得分: {rising_wave_score}")
            
            # 计算超预期得分
            higher_score = self.calculate_higher_score(auction_data, rising_wave_score)
            
            # 构建结果
            result = {
                'symbol': symbol,
                'stock_name': '',
                'auction_start_price': auction_data['auction_start_price'],
                'auction_end_price': auction_data['auction_end_price'],
                'price_diff': round(auction_data['auction_end_price'] - auction_data['auction_start_price'], 2),
                'max_gain': max_gain,
                'max_daily_gain': max_daily_gain,
                'today_gain': today_gain,
                'next_day_gain': next_day_gain,
                'trade_date': trade_date.strftime('%Y-%m-%d'),
                'higher_score': higher_score,
                'rising_wave_score': rising_wave_score,
                'weipan_exceed': weipan_exceed,
                'zaopan_exceed': zaopan_exceed,
                'rising_wave': rising_wave
            }
            
            if debug_mode:
                print(f"  [筛选] 股票通过所有筛选条件")
                print(f"    - 超预期得分: {higher_score}")
                print(f"    - 当日涨幅: {today_gain}%")
                print(f"    - 次日涨幅: {next_day_gain}%")
                print(f"=== 调试结束: {symbol} ===")
            
            results.append(result)
        
        return results
    
    def get_stock_names(self, symbols: list) -> Dict[str, str]:
        """
        获取股票名称

        Args:
            symbols: 股票代码列表

        Returns:
            股票代码到名称的映射
        """
        # 使用批量获取方法提高效率
        return self.cache.fetch_stock_names_bulk(symbols)