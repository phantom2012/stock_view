#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
后端股票过滤器模块
移植自 goldminer/stock_filter/stock_filter.py
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd

# 导入后端缓存
from stock_cache import get_stock_cache


class StockFilter:
    """
    个股过滤器类
    提供各种股票筛选条件的接口
    """
    
    def __init__(self):
        """初始化过滤器"""
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
            # 从缓存获取日K线数据（优先从数据库读取）
            data = self.cache.get_history_data(symbol, days=recent_interval_days + 5, trade_date=trade_date, force_refresh=False)
            
            if data is not None and len(data) >= 2:
                # 取最后recent_interval_days条数据
                data = data.tail(recent_interval_days)
                
                # 计算最小和最大收盘价
                min_close = data['close'].min()
                max_close = data['close'].max()
                
                # 计算最大收盘价与最小收盘价的涨幅
                max_gain = 0
                if min_close != 0:
                    max_gain = (max_close - min_close) / min_close * 100
                
                # 计算日内最大涨幅
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
            print(f"[StockFilter] Error checking performance for {symbol}: {e}")
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
            data = self.cache.get_history_data(symbol, recent_interval_days + 5, force_refresh=False)
            
            if data is not None and len(data) >= recent_interval_days:
                data = data.tail(recent_interval_days).reset_index(drop=True)
                
                # 找到最低收盘价的索引
                min_close_loc = data['close'].idxmin()
                start_idx = min_close_loc
                
                if start_idx >= len(data) - 1:
                    return 0
                
                # 从最低点的下一个交易日开始
                current_high = data.iloc[start_idx]['close']
                max_days_to_break = 0
                current_streak = 1
                
                for i in range(start_idx + 1, len(data)):
                    current_close = data.iloc[i]['close']
                    if current_close >= current_high:
                        current_high = current_close
                        if current_streak > max_days_to_break:
                            max_days_to_break = current_streak
                        current_streak = 1
                    else:
                        current_streak += 1
                
                if current_streak > max_days_to_break:
                    max_days_to_break = current_streak
                
                if max_days_to_break > 4:
                    return 0
                
                # 根据突破情况打分
                score_map = {1: 100, 2: 80, 3: 50, 4: 20}
                return score_map.get(max_days_to_break, 0)
            
            return 0
        except Exception as e:
            print(f"[StockFilter] Error calculating rising wave score for {symbol}: {e}")
            return 0
    
    def check_auction_condition(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        检查竞价条件（获取14:57-15:00的尾盘数据）
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
            
        Returns:
            竞价数据字典，如果不符合条件返回None
        """
        try:
            # 通过缓存池获取14:57-15:00的分钟数据
            minute_data = self.cache.get_minute_data(symbol, trade_date, "14:57:00", "15:00:00")
            
            if minute_data is not None and len(minute_data) > 0:
                auction_start = float(minute_data.iloc[0]['open'])
                auction_end = float(minute_data.iloc[-1]['close'])
                
                # 竞价结束价 >= 竞价开始价才符合条件
                if auction_end >= auction_start:
                    return {
                        'auction_start_price': auction_start,
                        'auction_end_price': auction_end,
                        'volume': float(minute_data.iloc[-1]['volume'])
                    }
            
            return None
        except Exception as e:
            print(f"[StockFilter] Error checking auction for {symbol}: {e}")
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
            # 从缓存获取日K线数据（优先从数据库读取）
            data = self.cache.get_history_data(symbol, days=10, trade_date=trade_date, force_refresh=False)
            
            today_gain = 0.0
            next_day_gain = 0.0
            
            if data is not None and not data.empty and len(data) >= 2:
                # 当日收盘价
                today_close = data.iloc[-1]['close']
                # 昨日收盘价（前一行）
                prev_close = data.iloc[-2]['close']
                # 计算当日收盘价相对昨日收盘价的涨跌幅
                if prev_close != 0:
                    today_gain = round((today_close - prev_close) / prev_close * 100, 2)
            
            # 获取次日数据
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
                    # 从缓存获取次日数据，多取几天确保有数据（优先从数据库读取）
                    next_data = self.cache.get_history_data(symbol, days=5, trade_date=next_trading_day, force_refresh=False)
                    if next_data is not None and not next_data.empty:
                        # 查找 next_trading_day 对应的收盘价
                        next_trading_day_str = next_trading_day.strftime('%Y-%m-%d')
                        next_close = None
                        for i, row in next_data.iterrows():
                            row_date = str(row['eob'])[:10]
                            if row_date == next_trading_day_str:
                                next_close = row['close']
                                break
                        # 如果没找到，使用最后一天的数据
                        if next_close is None and not next_data.empty:
                            next_close = next_data.iloc[-1]['close']
                        # 计算次日收盘价相对当日收盘价的涨幅
                        if next_close is not None and next_close != 0:
                            next_day_gain = round((next_close - today_close) / today_close * 100, 2)
            
            return today_gain, next_day_gain
        except Exception as e:
            print(f"[StockFilter] Error getting gains for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return 0.0, 0.0
    
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
        
        # 4. 验证股票名称并排除 ST 股票
        try:
            stock_name = self.cache.get_stock_name(symbol)
            
            # 如果缓存中名称未知，尝试强制刷新 instruments 缓存
            if stock_name == '未知':
                self.cache._load_instruments_cache()
                stock_name = self.cache.get_stock_name(symbol)
            
            # 如果还是未知，尝试通过 API 单独查询
            if stock_name == '未知':
                try:
                    from gm.api import get_instruments
                    inst = get_instruments(symbols=[symbol], df=True)
                    if inst is not None and not inst.empty:
                        stock_name = inst.iloc[0].get('sec_name', '未知')
                        print(f"[StockFilter] API 单独查询 {symbol} 名称: {stock_name}")
                    else:
                        print(f"[StockFilter] 股票 {symbol} 在 API 中也找不到，视为无效股票，已剔除")
                        return False
                except Exception as api_err:
                    print(f"[StockFilter] API 单独查询 {symbol} 失败: {api_err}，已剔除")
                    return False
            
            # 最终检查：如果名称仍然是未知，打印警告但允许通过
            if stock_name == '未知':
                print(f"[StockFilter] 警告: 股票 {symbol} 无法获取有效名称，但允许通过筛选")
                # return False  # 注释掉，允许名称未知的股票通过
            
            # 检查是否是 ST 或 *ST
            if 'ST' in stock_name or '*ST' in stock_name:
                # print(f"[DEBUG] 股票 {symbol} ({stock_name}) 是 ST 股，已剔除")
                return False
            
            # 检查是否是退市股票
            try:
                from gm.api import get_instruments
                import pandas as pd
                inst = get_instruments(symbols=[symbol], df=True)
                if inst is not None and not inst.empty:
                    delisted_date = inst.iloc[0].get('delisted_date')
                    # 正确的退市判断：只有当 delisted_date 是过去的时间才算退市
                    # 注意：正常股票 API 会返回 2038-01-01 作为占位符，不应视为退市
                    if delisted_date is not None and not pd.isna(delisted_date):
                        if isinstance(delisted_date, pd.Timestamp):
                            # 如果退市日期早于今天，才是真的退市
                            if delisted_date < datetime.now():
                                # print(f"[DEBUG] 股票 {symbol} ({stock_name}) 已退市（{delisted_date}），已剔除")
                                return False
                        elif isinstance(delisted_date, str) and delisted_date.strip():
                            # 处理字符串格式的日期
                            try:
                                delisted_dt = datetime.strptime(delisted_date[:10], '%Y-%m-%d')
                                if delisted_dt < datetime.now():
                                    # print(f"[DEBUG] 股票 {symbol} ({stock_name}) 已退市（{delisted_date}），已剔除")
                                    return False
                            except:
                                pass
            except Exception:
                pass
                
        except Exception as e:
            print(f"[StockFilter] 检查股票类型时出错 {symbol}: {e}，已剔除")
            return False
        
        # 5. 只保留主板股票 (60, 00, 002, 003等)
        # print(f"[DEBUG] 股票 {symbol} ({stock_name}) 通过类型检查")
        return True
    
    def calculate_higher_score(self, auction_data: Dict[str, Any], 
                             rising_wave_score: int = 0) -> float:
        """
        计算超预期得分
        
        Args:
            auction_data: 竞价数据
            rising_wave_score: 升浪形态得分
            
        Returns:
            超预期得分（保留2位小数）
        """
        auction_start_price = auction_data.get('auction_start_price', 0)
        auction_end_price = auction_data.get('auction_end_price', 0)
        
        if auction_start_price != 0:
            base_score = (auction_end_price - auction_start_price) / auction_start_price * 10000
            higher_score = base_score + rising_wave_score
            return round(higher_score, 2)
        
        return 0
    
    def filter_stocks(self, symbols: List[str], trade_date: datetime, 
                     weipan_exceed: int = 0, 
                     zaopan_exceed: int = 0, 
                     rising_wave: int = 0, 
                     config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
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
                'recent_interval_days': 40,
                'recent_interval_max_gain': 60,
                'day_max_gain_days': 6,
                'day_max_gain': 8,
            }
        
        results = []
        
        for i, symbol in enumerate(symbols):
            # 检查股票类型
            if not self.check_stock_type(symbol):
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
                continue
            
            # 检查竞价条件
            auction_data = self.check_auction_condition(symbol, trade_date)
            
            if not auction_data:
                continue
            
            # 获取涨幅数据
            today_gain, next_day_gain = self.check_stock_gains(symbol, trade_date)
            
            # 计算升浪形态得分
            rising_wave_score = 0
            if rising_wave == 1:
                rising_wave_score = self.calculate_rising_wave_score(
                    symbol, trade_date, config['day_max_gain_days']
                )
                if rising_wave_score <= 0:
                    continue
            
            # 计算超预期得分
            higher_score = self.calculate_higher_score(auction_data, rising_wave_score)
            
            # 获取股票名称
            stock_name = self.cache.get_stock_name(symbol)
            stock_code = symbol.split('.')[-1] if '.' in symbol else symbol
            
            # 如果名称仍为未知，尝试通过批量接口获取
            if stock_name == '未知':
                try:
                    names_map = self.cache.fetch_stock_names_bulk([symbol])
                    stock_name = names_map.get(stock_code, '未知')
                except:
                    pass
            
            # 构建结果
            result = {
                'symbol': symbol,  # 添加symbol字段
                'code': stock_code,
                'stock_name': stock_name,
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
            
            results.append(result)
        
        return results
