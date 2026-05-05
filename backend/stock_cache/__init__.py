#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
后端个股数据缓存模块（SQLite版本）
移植自 goldminer/stock_data/stock_cache.py
使用 SQLite 数据库替代内存缓存
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from shared.trade_date_util import TradeDateUtil
from common.stock_code_convert import to_pure_code
from models import StockDaily, StockAuction, StockInfo, StockMinute, StockTick, get_session, get_session_ro


# 创建 TradeDateUtil 实例
trade_date_util = TradeDateUtil()


class StockDataCache:
    """
    个股信息缓存池（SQLite版本，用于后端API）
    使用 SQLite 数据库替代内存缓存
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

        self._initialized = True

    @staticmethod
    def to_float(value, default=0):
        """安全转换为浮点数"""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def to_int(value, default=0):
        """安全转换为整数"""
        try:
            if value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def get_stock_name(self, symbol: str) -> str:
        """
        获取股票名称（仅从数据库读取）
        """
        pure_code = to_pure_code(symbol)

        # 从数据库查找
        try:
            with get_session_ro() as db:
                row = db.query(StockInfo).filter(StockInfo.code == pure_code).first()
                if row and row.name:
                    return row.name
        except Exception as e:
            print(f"[StockCache] 从数据库获取股票名称失败: {e}")

        return '未知'

    def fetch_stock_names_bulk(self, symbols: List[str]) -> Dict[str, str]:
        """
        批量获取股票名称（仅从数据库读取）
        """
        result = {}

        # 将symbols转换为纯数字code
        pure_codes = [to_pure_code(s) for s in symbols]

        # 从数据库批量查找
        try:
            with get_session_ro() as db:
                rows = db.query(StockInfo).filter(StockInfo.code.in_(pure_codes)).all()
                for row in rows:
                    result[row.code] = row.name
        except Exception as e:
            print(f"[StockCache] 从数据库批量获取股票名称失败: {e}")

        # 返回时使用原始symbol作为key
        return_result = {}
        for i, symbol in enumerate(symbols):
            pure_code = pure_codes[i]
            if pure_code in result:
                return_result[symbol] = result[pure_code]
            else:
                return_result[symbol] = '未知'

        return return_result

    def get_stock_day_data(self, symbol: str, trade_date: datetime, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        获取股票指定日期的日线数据（单条记录，仅从数据库读取）

        Args:
            symbol: 股票代码，如 'SHSE.600105'
            trade_date: 交易日期
            force_refresh: 是否强制刷新（仅保留参数兼容性，实际只从数据库读取）

        Returns:
            包含单条记录的 DataFrame，如果未找到则返回 None
        """
        date_key = trade_date.strftime('%Y-%m-%d')
        pure_code = to_pure_code(symbol)

        # 从数据库读取指定日期的数据
        cached_data = self._read_single_day_from_db(pure_code, date_key)
        if cached_data is not None and not cached_data.empty:
            return cached_data

        print(f"[StockCache] 未获取到 {symbol} 在 {date_key} 的数据")
        return None

    def get_history_data(self, symbol: str, days: int = 5, trade_date: Optional[datetime] = None, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        获取股票最近N日的历史数据（仅从数据库读取）
        """
        if trade_date is None:
            latest_trade_date = trade_date_util.get_latest_trade_date()
            if latest_trade_date:
                end_date = datetime.strptime(latest_trade_date, '%Y-%m-%d')
            else:
                end_date = datetime.now()
        else:
            end_date = trade_date

        date_key = end_date.strftime('%Y-%m-%d')

        # 将symbol转换为纯数字code，用于数据库操作
        pure_code = to_pure_code(symbol)

        # 从数据库读取
        cached_data = self._read_daily_from_db(pure_code, days, date_key)
        if cached_data is not None and not cached_data.empty:
            # 按日期正序排列
            cached_data = cached_data.sort_values('trade_date')
            return cached_data

        print(f"[StockCache] 未获取到 {symbol} 的历史数据")
        return None

    def get_previous_trade_data(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        获取指定交易日的上一个交易日的数据

        Args:
            symbol: 股票代码（如：SHSE.600105）
            trade_date: 指定的交易日

        Returns:
            包含上一个交易日数据的字典，包含以下字段：
            - pre_close_price: 上一个交易日的收盘价
            - pre_avg_price: 上一个交易日的均价
            - pre_price_gain: 昨涨幅（上一个交易日相比上上一个交易日的涨幅）
            - trade_date: 上一个交易日的日期
            如果获取失败返回None
        """
        try:
            # 获取上一个交易日
            prev_trade_date_str = trade_date_util.get_previous_trade_date(trade_date)
            if not prev_trade_date_str:
                print(f"[StockCache] 无法获取 {trade_date.strftime('%Y-%m-%d')} 的上一个交易日")
                return None

            prev_trade_date = datetime.strptime(prev_trade_date_str, '%Y-%m-%d')

            # 获取上一个交易日的数据
            data = self.get_stock_day_data(symbol, prev_trade_date)
            if data is None or data.empty:
                print(f"[StockCache] 无法获取 {symbol} 在 {prev_trade_date_str} 的数据")
                return None

            # 提取需要的字段
            row = data.iloc[-1]
            pre_close_price = row.get('close', 0)  # 上一个交易日的收盘价
            pre_pre_close_price = row.get('pre_close', 0)  # 上上个交易日的收盘价（pre_close字段）

            # 计算均价：成交额 / 成交量
            volume = row.get('volume', 0)
            amount = row.get('amount', 0)
            pre_avg_price = round(amount / volume, 2) if volume > 0 else pre_close_price

            # 计算昨涨幅：(上一个交易日收盘价 - 上上个交易日收盘价) / 上上个交易日收盘价 * 100
            pre_price_gain = 0
            try:
                if pre_pre_close_price > 0:
                    pre_price_gain = round((pre_close_price - pre_pre_close_price) / pre_pre_close_price * 100, 2)
            except Exception as e:
                print(f"[StockCache] 计算昨涨幅失败: {e}")

            return {
                'pre_close_price': pre_close_price,
                'pre_avg_price': pre_avg_price,
                'pre_price_gain': pre_price_gain,
                'trade_date': prev_trade_date_str
            }

        except Exception as e:
            print(f"[StockCache] 获取上一个交易日数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _process_time_field(self, value) -> str:
        """处理时间字段，转换为字符串格式"""
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value)

    def _read_daily_from_db(self, code: str, days: int, date_key: str) -> Optional[pd.DataFrame]:
        """从数据库读取日线数据"""
        try:
            with get_session_ro() as db:
                # 使用SQLAlchemy ORM查询
                daily_data = db.query(StockDaily).filter(
                    StockDaily.code == code,
                    StockDaily.trade_date <= date_key
                ).order_by(StockDaily.trade_date.desc()).limit(days).all()

                if daily_data:
                    # 转换为DataFrame
                    data = []
                    for item in daily_data:
                        data.append({
                            'trade_date': item.trade_date,
                            'open': item.open,
                            'close': item.close,
                            'high': item.high,
                            'low': item.low,
                            'volume': item.volume,
                            'amount': item.amount,
                            'pre_close': item.pre_close,
                            'eob': item.eob
                        })
                    df = pd.DataFrame(data)
                    return df
        except Exception as e:
            print(f"[StockCache] 从数据库读取日线数据失败: {e}")
        return None

    def _read_single_day_from_db(self, code: str, date_key: str) -> Optional[pd.DataFrame]:
        """从数据库读取指定日期的单条日线数据"""
        try:
            with get_session_ro() as db:
                # 使用SQLAlchemy ORM查询
                daily_data = db.query(StockDaily).filter(
                    StockDaily.code == code,
                    StockDaily.trade_date == date_key
                ).first()

                if daily_data:
                    # 转换为DataFrame
                    data = [{
                        'trade_date': daily_data.trade_date,
                        'open': daily_data.open,
                        'close': daily_data.close,
                        'high': daily_data.high,
                        'low': daily_data.low,
                        'volume': daily_data.volume,
                        'amount': daily_data.amount,
                        'pre_close': daily_data.pre_close,
                        'eob': daily_data.eob
                    }]
                    df = pd.DataFrame(data)
                    return df
        except Exception as e:
            print(f"[StockCache] 从数据库读取单条日线数据失败: {e}")
        return None

    def get_minute_data(self, symbol: str, trade_date: datetime, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """获取指定时间段的分钟数据（仅从数据库读取）"""
        date_key = trade_date.strftime('%Y-%m-%d')

        # 将symbol转换为纯数字code
        pure_code = to_pure_code(symbol)

        # 从数据库读取
        cached_data = self._read_minute_from_db(pure_code, date_key, start_time_str, end_time_str)
        if cached_data is not None and not cached_data.empty:
            return cached_data

        print(f"[StockCache] 未获取到 {symbol} 在 {date_key} {start_time_str}-{end_time_str} 的分钟数据")
        return None

    def _read_minute_from_db(self, code: str, date_key: str, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """从数据库读取分钟数据"""
        try:
            with get_session_ro() as db:
                rows = db.query(StockMinute).filter(
                    StockMinute.code == code,
                    StockMinute.trade_date == date_key,
                    StockMinute.eob >= f"{date_key} {start_time_str}",
                    StockMinute.eob <= f"{date_key} {end_time_str}"
                ).order_by(StockMinute.eob).all()

                if rows:
                    data = [{c.name: getattr(r, c.name) for c in StockMinute.__table__.columns if c.name in ('eob', 'open', 'close', 'high', 'low', 'volume', 'amount')} for r in rows]
                    return pd.DataFrame(data)
        except Exception as e:
            print(f"[StockCache] 从数据库读取分钟数据失败: {e}")
        return None

    def _is_date_in_recent_trade_dates(self, date_key: str, days: int = 6) -> bool:
        """检查日期是否在最近N个交易日内"""
        recent_trade_dates = trade_date_util.get_recent_trade_dates(days)
        if recent_trade_dates:
            if date_key not in recent_trade_dates:
                print(f"[StockCache] 查询超出最近{days}个交易日范围: {date_key}，跳过查询")
                return False
        return True

    def _is_auction_time(self, start_time_str: str, end_time_str: str) -> bool:
        """检查是否是竞价相关的时间范围（9:25-9:31）"""
        try:
            start_time = datetime.strptime(start_time_str, '%H:%M:%S').time()
            end_time = datetime.strptime(end_time_str, '%H:%M:%S').time()
            if start_time <= datetime.strptime('09:25:00', '%H:%M:%S').time() and \
               end_time >= datetime.strptime('09:31:00', '%H:%M:%S').time():
                return True
        except:
            pass
        return False

    def get_tick_data(self, symbol: str, trade_date: datetime, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """
        获取指定时间段的tick数据（仅从数据库读取）
        限制：只允许查询包含今日在内的最近6个交易日
        """
        date_key = trade_date.strftime('%Y-%m-%d')

        # 将symbol转换为纯数字code
        pure_code = to_pure_code(symbol)

        if not self._is_date_in_recent_trade_dates(date_key, 6):
            return None

        # 从数据库读取
        cached_data = self._read_tick_from_db(pure_code, date_key, start_time_str, end_time_str)
        if cached_data is not None and not cached_data.empty:
            return cached_data

        print(f"[StockCache] 未获取到 {symbol} 在 {date_key} {start_time_str}-{end_time_str} 的tick数据")
        return None

    def _read_tick_from_db(self, code: str, date_key: str, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """从数据库读取tick数据"""
        try:
            with get_session_ro() as db:
                rows = db.query(StockTick).filter(
                    StockTick.code == code,
                    StockTick.trade_date == date_key,
                    StockTick.created_at >= f"{date_key} {start_time_str}",
                    StockTick.created_at <= f"{date_key} {end_time_str}"
                ).order_by(StockTick.created_at).all()

                if rows:
                    data = [{c.name: getattr(r, c.name) for c in StockTick.__table__.columns if c.name in ('created_at', 'price', 'volume', 'cum_amount', 'cum_volume')} for r in rows]
                    return pd.DataFrame(data)
        except Exception as e:
            print(f"[StockCache] 从数据库读取tick数据失败: {e}")
        return None

    def get_auction_data(self, symbol: str, trade_date: datetime) -> Dict[str, float]:
        """
        获取个股早盘竞价数据（仅从数据库读取）
        - 早盘竞价金额：9:30前最后一个快照的累计成交额
        - 开盘成交额：9:30第一个快照的累计成交额
        - 开盘量比：竞价成交量与过去5日平均成交量的比值
        """
        date_key = trade_date.strftime('%Y-%m-%d')

        # 将symbol转换为纯数字code
        pure_code = to_pure_code(symbol)

        # 从数据库读取
        cached_data = self._read_auction_from_db(pure_code, date_key)
        if cached_data is not None:
            return cached_data

        print(f"[StockCache] 未获取到 {symbol} 在 {date_key} 的竞价数据")
        return {
            'open_volume': 0,
            'open_amount': 0,
            'volume_ratio': 0
        }

    def _read_auction_from_db(self, code: str, date_key: str) -> Optional[Dict[str, float]]:
        """从数据库读取竞价数据"""
        try:
            with get_session_ro() as db:
                row = db.query(StockAuction).filter(
                    StockAuction.code == code,
                    StockAuction.trade_date == date_key
                ).first()

                if row:
                    return {
                        'open_volume': self.to_float(row.open_volume),
                        'open_amount': self.to_float(row.open_amount),
                        'volume_ratio': self.to_float(row.volume_ratio)
                    }
        except Exception as e:
            print(f"[StockCache] 从数据库读取竞价数据失败: {e}")
        return None

    def get_tail_auction_data(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        获取尾盘竞价数据（14:57-15:00的分钟数据）

        Args:
            symbol: 股票代码（如: SZSE.002990）
            trade_date: 交易日期

        Returns:
            尾盘竞价数据字典，如果无数据返回None
            {
                'auction_start_price': 14:57收盘价,
                'auction_end_price': 15:00收盘价,
                'amount': 尾盘竞价金额
            }
        """
        try:
            # 通过缓存池获取14:56-15:00的分钟数据:获取到的是14:57,14:58，15:00的min数据
            minute_data = self.get_minute_data(symbol, trade_date, "14:56:00", "15:00:00")

            if minute_data is not None and len(minute_data) > 0:
                auction_start = float(minute_data.iloc[0]['close'])
                auction_end = float(minute_data.iloc[-1]['close'])

                # 返回尾盘竞价数据
                return {
                    'auction_start_price': auction_start,
                    'auction_end_price': auction_end,
                    'amount': float(minute_data.iloc[-1]['amount'])
                }

            return None
        except Exception as e:
            print(f"[StockCache] Error getting tail auction data for {symbol}: {e}")
            return None


# 全局缓存池实例
_global_cache = None


def get_stock_cache() -> StockDataCache:
    """获取全局缓存池实例"""
    global _global_cache
    if _global_cache is None:
        _global_cache = StockDataCache()
    return _global_cache
