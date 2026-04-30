#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
后端股票过滤器模块
移植自 goldminer/stock_filter/stock_filter.py
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd

# 导入掘金 API
from gm.api import get_instruments

# 导入数据模型
from models import StockResult, StockPerformance
from models.filter_params import FilterParams

# 导入后端缓存
from stock_cache import get_stock_cache
# 导入交易日工具
from baostock_data.trade_date_util import TradeDateUtil


class StockFilter:
    """
    个股过滤器类
    提供各种股票筛选条件的接口
    """

    def __init__(self):
        """初始化过滤器"""
        self.cache = get_stock_cache()
        self.trade_date_util = TradeDateUtil()

    def _calculate_price_ratio(self, data: pd.DataFrame) -> float:
        """计算股价相对于近期最高价的比例"""
        high_prices = data['high'].dropna()
        if len(high_prices) == 0:
            return 0.0
        recent_high = high_prices.max()
        current_price = data.iloc[-1]['close']
        if recent_high > 0:
            return (current_price / recent_high) * 100
        return 0.0

    def _calculate_period_gain(self, data: pd.DataFrame) -> float:
        """计算区间最大涨幅（首尾收盘价）"""
        first_close = data.iloc[0]['close']
        last_close = data.iloc[-1]['close']
        if first_close > 0:
            return ((last_close - first_close) / first_close) * 100
        return 0.0

    def _calculate_max_day_rise(self, data: pd.DataFrame, recent_days: int) -> float:
        """计算日内最大涨幅"""
        max_day_rise = 0.0
        if len(data) >= 2:
            for i in range(1, min(recent_days + 1, len(data))):
                current_row = data.iloc[-i]
                prev_row = data.iloc[-i - 1]
                current_high = current_row['high']
                prev_close = prev_row['close']
                if prev_close > 0:
                    day_gain = ((current_high - prev_close) / prev_close) * 100
                    if day_gain > max_day_rise:
                        max_day_rise = day_gain
        return max_day_rise

    def check_performance(self, symbol: str, trade_date: datetime,
                        params: FilterParams) -> StockPerformance:
        """
        检查股票近N个交易日的表现

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            params: 筛选参数对象

        Returns:
            StockPerformance对象
        """
        # 打印所有入参数值
        print(f"[StockFilter.check_performance] symbol={symbol}, trade_date={trade_date}")

        try:
            # 从缓存获取日K线数据（优先从数据库读取）
            # 多获取1天数据，用于计算当日开盘价相对于前一日收盘价的涨跌
            data = self.cache.get_history_data(symbol, days=params.interval_days + 1, trade_date=trade_date, force_refresh=False)

            if data is not None and len(data) >= 2:
                # 取最后interval_days条数据用于计算
                data = data.tail(params.interval_days)

                # 1. 计算股价相对于近期最高价的比例
                price_ratio = self._calculate_price_ratio(data)

                # 如果设置了股价比例阈值，检查是否满足
                if params.prev_high_price_rate > 0 and price_ratio < params.prev_high_price_rate:
                    return StockPerformance(is_pass=False, interval_max_rise=0, max_day_rise=0, prev_high_price_rate=round(price_ratio, 2))

                # 2. 计算区间最大涨幅（首尾收盘价）
                interval_max_rise_value = self._calculate_period_gain(data)

                # 3. 计算日内最大涨幅
                max_day_rise = self._calculate_max_day_rise(data, params.recent_days)

                # 保留小数点后2位
                interval_max_rise_value = round(interval_max_rise_value, 2)
                max_day_rise = round(max_day_rise, 2)
                price_ratio = round(price_ratio, 2)

                # 检查条件：区间涨幅和日内涨幅都需要大于阈值
                is_pass = abs(interval_max_rise_value) >= params.interval_max_rise and max_day_rise >= params.recent_max_day_rise
                return StockPerformance(is_pass=is_pass, interval_max_rise=interval_max_rise_value, max_day_rise=max_day_rise, prev_high_price_rate=price_ratio)

            return StockPerformance(is_pass=False, interval_max_rise=0, max_day_rise=0, prev_high_price_rate=0)
        except Exception as e:
            print(f"[StockFilter] Error checking performance for {symbol}: {e}")
            return StockPerformance(is_pass=False, interval_max_rise=0, max_day_rise=0, prev_high_price_rate=0)

    def calculate_rising_wave_score(self, symbol: str, trade_date: datetime,
                                   recent_days: int = 10) -> int:
        """
        计算升浪形态得分

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_days: 最近最大涨幅天数

        Returns:
            升浪形态得分（0-100）
        """
        try:
            data = self.cache.get_history_data(symbol, recent_days + 5, force_refresh=False)

            if data is not None and len(data) >= recent_days:
                data = data.tail(recent_days).reset_index(drop=True)

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

    def check_tail_auction_condition(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        检查尾盘竞价条件（获取14:57-15:00的尾盘数据，要求价格上涨）

        Args:
            symbol: 股票代码
            trade_date: 交易日期

        Returns:
            竞价数据字典，如果不符合条件返回None
        """
        try:
            # 调用新接口获取尾盘竞价数据
            tail_data = self.cache.get_tail_auction_data(symbol, trade_date)

            if tail_data:
                auction_start = tail_data['open_price']
                auction_end = tail_data['close_price']

                # 竞价结束价 >= 竞价开始价才符合条件
                if auction_end >= auction_start:
                    return tail_data

            return None
        except Exception as e:
            print(f"[StockFilter] Error checking tail auction for {symbol}: {e}")
            return None

    def get_stock_day_gain(self, symbol: str, trade_date: datetime) -> Optional[float]:
        """
        获取股票指定日期的涨幅

        Args:
            symbol: 股票代码
            trade_date: 交易日期

        Returns:
            涨幅百分比，如果无法计算则返回 None
        """
        try:
            # 从缓存获取该日的日K线数据（优先从数据库读取）
            data = self.cache.get_stock_day_data(symbol, trade_date, force_refresh=False)

            if data is not None and not data.empty:
                # 获取第一行数据
                row = data.iloc[0]

                # 使用当日收盘价和前一日收盘价计算涨幅
                today_close = row['close']
                prev_close = row['pre_close']

                # 计算涨幅
                if prev_close is not None and prev_close != 0:
                    gain = round((today_close - prev_close) / prev_close * 100, 2)
                    return gain

            return None
        except Exception as e:
            print(f"[StockFilter] Error getting day gain for {symbol} on {trade_date.strftime('%Y-%m-%d')}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def check_is_main_board(self, symbol: str) -> bool:
        """
        检查是否是主板股票
        主板股票定义：排除科创板、创业板、北交所后的股票

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否是主板股票
        """
        stock_code = symbol.split('.')[-1] if '.' in symbol else symbol

        # 排除科创板 (688)
        if stock_code.startswith('688'):
            return False

        # 排除创业板 (300, 301)
        if stock_code.startswith('300') or stock_code.startswith('301'):
            return False

        # 排除北交所 (8, 4, 92开头)
        if stock_code.startswith('8') or stock_code.startswith('4') or stock_code.startswith('92'):
            return False

        # 剩下的就是主板股票（包括60、00、002、003等）
        return True

    def _check_delisted(self, symbol: str) -> bool:
        """
        检查股票是否已退市

        Args:
            symbol: 股票代码

        Returns:
            bool: True=未退市(有效), False=已退市
        """
        try:
            inst = get_instruments(symbols=[symbol], df=True)
            if inst is None or inst.empty:
                print(f"[StockFilter] 股票 {symbol} 在 API 中找不到，视为无效股票，已剔除")
                return False

            delisted_date = inst.iloc[0].get('delisted_date')
            if delisted_date is not None and not pd.isna(delisted_date):
                if isinstance(delisted_date, pd.Timestamp):
                    if delisted_date < datetime.now():
                        print(f"[StockFilter] 股票 {symbol} 已退市，日期: {delisted_date}，已剔除")
                        return False
                elif isinstance(delisted_date, str) and delisted_date.strip():
                    try:
                        delisted_dt = datetime.strptime(delisted_date[:10], '%Y-%m-%d')
                        if delisted_dt < datetime.now():
                            print(f"[StockFilter] 股票 {symbol} 已退市，日期: {delisted_date}，已剔除")
                            return False
                    except:
                        pass
            return True
        except Exception:
            return True  # API查询失败时默认通过

    def _fetch_stock_name(self, symbol: str) -> str:
        """
        获取股票名称，依次尝试：缓存 -> 刷新缓存 -> API单独查询

        Args:
            symbol: 股票代码

        Returns:
            str: 股票名称，获取失败返回'未知'
        """
        stock_name = self.cache.get_stock_name(symbol)

        if stock_name == '未知':
            self.cache._load_instruments_cache()
            stock_name = self.cache.get_stock_name(symbol)

        if stock_name == '未知':
            try:
                inst = get_instruments(symbols=[symbol], df=True)
                if inst is not None and not inst.empty:
                    stock_name = inst.iloc[0].get('sec_name', '未知')
                    print(f"[StockFilter] API 单独查询 {symbol} 名称: {stock_name}")
                else:
                    print(f"[StockFilter] 股票 {symbol} 在 API 中也找不到，视为无效股票")
                    return '未知'
            except Exception as api_err:
                print(f"[StockFilter] API 单独查询 {symbol} 失败: {api_err}")
                return '未知'

        return stock_name

    def check_is_10cm(self, symbol: str) -> bool:
        """
        检查是否为10cm涨跌幅股票（主板非ST股票）
        过滤条件：
        1. 必须是主板股票（60或00开头）
        2. 排除ST、*ST股票
        3. 排除退市股票

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否符合10cm条件
        """
        # 1. 首先检查是否是主板股票
        if not self.check_is_main_board(symbol):
            return False

        # 2. 检查是否退市
        if not self._check_delisted(symbol):
            return False

        # 3. 获取股票名称并检查 ST
        try:
            stock_name = self._fetch_stock_name(symbol)

            # 检查是否是 ST 或 *ST 股票
            if stock_name == '未知':
                print(f"[StockFilter] 警告: 股票 {symbol} 无法获取有效名称，但允许通过筛选")
            elif 'ST' in stock_name or '*ST' in stock_name:
                print(f"[StockFilter] 股票 {symbol} 是 ST 股票，已剔除")
                return False

        except Exception as e:
            print(f"[StockFilter] 检查10cm股票时出错 {symbol}: {e}，已剔除")
            return False

        return True

    def calculate_exp_score(self, auction_data: Dict[str, Any],
                           rising_wave_score: int = 0) -> float:
        """
        计算预期得分

        Args:
            auction_data: 竞价数据
            rising_wave_score: 升浪形态得分

        Returns:
            预期得分（保留2位小数）
        """
        begin_price = auction_data.get('begin_price', auction_data.get('open_price', 0))
        end_price = auction_data.get('end_price', auction_data.get('close_price', 0))

        if begin_price != 0:
            base_score = (end_price - begin_price) / begin_price * 10000
            exp_score = base_score + rising_wave_score
            return round(exp_score, 2)

        return 0

    def filter_stocks(self, symbols: List[str], trade_date: datetime,
                     params: FilterParams) -> List[StockResult]:
        """
        综合筛选股票

        Args:
            symbols: 股票代码列表
            trade_date: 交易日期
            params: 筛选参数对象

        Returns:
            筛选结果列表
        """

        results = []

        for i, symbol in enumerate(symbols):
            # 检查是否为10cm股票
            if not self.check_is_10cm(symbol):
                continue

            # 检查性能条件
            performance = self.check_performance(symbol, trade_date, params)

            if not performance.is_pass:
                continue

            # 只有在勾选了尾盘超预期时才检查尾盘竞价条件
            # 注意：早盘超预期的逻辑不是用这个接口
            auction_data = None
            if params.weipan_exceed > 0:
                auction_data = self.check_tail_auction_condition(symbol, trade_date)
                if not auction_data:
                    continue

            # 获取当日涨幅
            today_gain = self.get_stock_day_gain(symbol, trade_date)

            # 获取次日涨幅（只有当次日不是今天时才获取）
            next_day_rise = None
            if trade_date.date() < datetime.now().date():
                next_trade_date_str = self.trade_date_util.get_next_trade_date(trade_date)
                if next_trade_date_str:
                    next_trading_day = datetime.strptime(next_trade_date_str, '%Y-%m-%d')
                    next_day_rise = self.get_stock_day_gain(symbol, next_trading_day)

            # 计算升浪形态得分
            rising_wave_score = 0
            if params.rising_wave == 1:
                rising_wave_score = self.calculate_rising_wave_score(
                    symbol, trade_date, params.recent_days
                )
                if rising_wave_score <= 0:
                    continue

            # exp_score 取 rising_wave_score 的值
            exp_score = rising_wave_score

            # 计算新增字段：昨均价、昨收盘价、昨涨幅
            pre_avg_price = 0
            pre_close_price = 0
            pre_price_gain = 0

            # 获取上一个交易日的数据
            try:
                prev_trade_data = self.cache.get_previous_trade_data(symbol, trade_date)
                if prev_trade_data:
                    pre_close_price = prev_trade_data.get('pre_close_price', 0)
                    pre_avg_price = prev_trade_data.get('pre_avg_price', 0)
                    pre_price_gain = prev_trade_data.get('pre_price_gain', 0)
            except Exception as e:
                logger.error(f"Error getting previous trade data for {symbol}: {e}")

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

            # 获取竞价数据以获取volume_ratio
            volume_ratio = 0
            try:
                auction_data_full = self.cache.get_auction_data(symbol, trade_date)
                volume_ratio = auction_data_full.get('volume_ratio', 0)
            except Exception as e:
                print(f"[StockFilter] Error getting auction data for {symbol}: {e}")

            # 获取当日开盘价和收盘价
            open_price = 0.0
            close_price = 0.0
            next_close_price = 0.0
            try:
                # 获取当日日K线数据
                day_data = self.cache.get_stock_day_data(symbol, trade_date, force_refresh=False)
                if day_data is not None and not day_data.empty:
                    row = day_data.iloc[0]
                    open_price = row.get('open', 0.0)
                    close_price = row.get('close', 0.0)

                # 获取次日收盘价
                if trade_date.date() < datetime.now().date():
                    next_trade_date_str = self.trade_date_util.get_next_trade_date(trade_date)
                    if next_trade_date_str:
                        next_trading_day = datetime.strptime(next_trade_date_str, '%Y-%m-%d')
                        next_day_data = self.cache.get_stock_day_data(symbol, next_trading_day, force_refresh=False)
                        if next_day_data is not None and not next_day_data.empty:
                            next_close_price = next_day_data.iloc[0].get('close', 0.0)
            except Exception as e:
                print(f"[StockFilter] Error getting day data for {symbol}: {e}")

            # 构建结果
            result = StockResult.create(
                symbol=symbol,
                code=stock_code,
                stock_name=stock_name,
                auction_data=auction_data,
                open_volume_ratio=volume_ratio,
                interval_max_rise=performance.interval_max_rise,
                max_day_rise=performance.max_day_rise,
                today_gain=today_gain if today_gain is not None else 0.0,
                next_day_rise=next_day_rise if next_day_rise is not None else 0.0,
                trade_date=trade_date.strftime('%Y-%m-%d'),
                exp_score=exp_score,
                rising_wave_score=rising_wave_score,
                weipan_exceed=params.weipan_exceed,
                zaopan_exceed=params.zaopan_exceed,
                rising_wave=params.rising_wave,
                pre_avg_price=pre_avg_price,
                pre_close_price=pre_close_price,
                pre_price_gain=pre_price_gain,
                open_price=open_price,
                close_price=close_price,
                next_close_price=next_close_price
            )

            results.append(result)

        return results


# 全局单例
_stock_filter_instance = None

def get_stock_filter() -> StockFilter:
    """
    获取StockFilter单例

    Returns:
        StockFilter实例
    """
    global _stock_filter_instance
    if _stock_filter_instance is None:
        _stock_filter_instance = StockFilter()
    return _stock_filter_instance
