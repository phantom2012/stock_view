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

from baostock_data.trade_date_util import TradeDateUtil
from stock_sqlite.database import get_db_connection, get_db_cursor
from external_data.ext_data_query_handle import get_query_handler, QUERY_API_TYPE
from common.stock_code_convert import to_pure_code, get_exchange
from models import StockDaily, get_db


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

        self._api_token_set = False
        self._instruments_loaded = False
        self._instruments_cache = None
        self._query_handler = get_query_handler()

        self._initialized = True

    def set_api_token(self, api_key: str):
        """设置API Token（已迁移到ExternalDataQueryHandler）"""
        if not self._api_token_set:
            print(f"[StockCache] API Token已通过ExternalDataQueryHandler初始化")
            self._api_token_set = True

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

    def _load_instruments_cache(self) -> pd.DataFrame:
        """加载 instruments 缓存（通过ExternalDataQueryHandler获取）"""
        if self._instruments_loaded and self._instruments_cache is not None:
            return self._instruments_cache

        print("[StockCache] 加载 instruments 缓存...")
        try:
            # 使用ExternalDataQueryHandler获取股票基本信息
            query_handler = get_query_handler()
            instruments = query_handler.get_instruments()

            if instruments is not None and not instruments.empty:
                self._instruments_cache = instruments
                self._instruments_loaded = True
                print(f"[StockCache] instruments 缓存加载完成，共 {len(instruments)} 条数据")
                return instruments
        except Exception as e:
            print(f"[StockCache] 加载 instruments 缓存失败: {e}")
            import traceback
            traceback.print_exc()
        return None

    def get_stock_name(self, symbol: str) -> str:
        """
        获取股票名称
        """
        pure_code = to_pure_code(symbol)

        # 先从数据库查找
        try:
            with get_db_cursor() as cursor:
                cursor.execute("SELECT name FROM stock_info WHERE code = ?", (pure_code,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
        except Exception as e:
            print(f"[StockCache] 从数据库获取股票名称失败: {e}")

        # 从 API 获取并保存到数据库
        instruments = self._load_instruments_cache()
        if instruments is not None and not instruments.empty:
            match = instruments[instruments['symbol'] == symbol]
            if not match.empty:
                name = match.iloc[0].get('sec_name', '未知')
                self._save_stock_info(pure_code, name)
                return name

        return '未知'

    def _save_stock_info(self, code: str, name: str):
        """保存股票信息到数据库"""
        try:
            pure_code = code.split('.')[-1] if '.' in code else code
            exchange = get_exchange(pure_code)
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            with get_db_cursor() as cursor:
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_info (code, name, exchange, update_time)
                    VALUES (?, ?, ?, ?)
                """, (pure_code, name, exchange, update_time))
        except Exception as e:
            print(f"[StockCache] 保存股票信息失败: {e}")

    def fetch_stock_names_bulk(self, symbols: List[str]) -> Dict[str, str]:
        """
        批量获取股票名称
        """
        result = {}

        # 将symbols转换为纯数字code
        pure_codes = [to_pure_code(s) for s in symbols]

        # 先从数据库批量查找
        try:
            with get_db_cursor() as cursor:
                placeholders = ','.join(['?' for _ in pure_codes])
                cursor.execute(f"SELECT code, name FROM stock_info WHERE code IN ({placeholders})", pure_codes)
                db_results = cursor.fetchall()
                for row in db_results:
                    result[row[0]] = row[1]
        except Exception as e:
            print(f"[StockCache] 从数据库批量获取股票名称失败: {e}")

        # 找出未找到的股票
        not_found = [s for s in symbols if to_pure_code(s) not in result]

        if not_found:
            instruments = self._load_instruments_cache()
            if instruments is not None and not instruments.empty:
                for symbol in not_found:
                    pure_code = to_pure_code(symbol)
                    match = instruments[instruments['symbol'] == symbol]
                    if not match.empty:
                        name = match.iloc[0].get('sec_name', '未知')
                        result[pure_code] = name
                        self._save_stock_info(pure_code, name)
                    else:
                        result[pure_code] = '未知'

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
        获取股票指定日期的日线数据（单条记录）

        Args:
            symbol: 股票代码，如 'SHSE.600105'
            trade_date: 交易日期
            force_refresh: 是否强制从 API 刷新

        Returns:
            包含单条记录的 DataFrame，如果未找到则返回 None
        """
        date_key = trade_date.strftime('%Y-%m-%d')
        pure_code = to_pure_code(symbol)

        if not force_refresh:
            # 从数据库读取指定日期的数据
            cached_data = self._read_single_day_from_db(pure_code, date_key)
            if cached_data is not None and not cached_data.empty:
                return cached_data

        # 从 API 获取（使用 ExternalDataQueryHandler）
        try:
            # 直接查询指定日期的数据
            date_str = trade_date.strftime('%Y-%m-%d')

            data = self._query_handler.get_daily_data(
                symbol=symbol,
                start_date=date_str,
                end_date=date_str
            )

            if data is not None and not data.empty:
                # 保存到数据库
                self._save_daily_to_db(pure_code, data)
                return data
            else:
                print(f"[StockCache] 未获取到 {symbol} 在 {date_str} 的数据")
                return None
        except Exception as e:
            print(f"[StockCache] 获取单日数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_history_data(self, symbol: str, days: int = 5, trade_date: Optional[datetime] = None, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        获取股票最近N日的历史数据（从数据库读取）
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

        if not force_refresh:
            # 从数据库读取
            cached_data = self._read_daily_from_db(pure_code, days, date_key)
            if cached_data is not None and not cached_data.empty:
                # print(f"[StockCache] 从数据库读取到 {len(cached_data)} 条历史数据")
                # 按日期正序排列，与API返回数据保持一致
                cached_data = cached_data.sort_values('trade_date')
                return cached_data

        # 从 API 获取（使用ExternalDataQueryHandler）
        try:
            start_date = end_date - timedelta(days=days + 20)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            data = self._query_handler.get_daily_data(
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str
            )

            if data is not None and not data.empty:
                # print(f"[StockCache] 从 API 获取到 {len(data)} 条历史数据")
                # print(f"[StockCache] 数据列名: {list(data.columns)}")
                data = data.sort_values('eob')
                self._save_daily_to_db(pure_code, data)
                return data.tail(days)
            else:
                print(f"[StockCache] API 返回空数据: {data}")
                return None

        except Exception as e:
            print(f"[StockCache] 获取历史数据失败: {e}")
            import traceback
            traceback.print_exc()
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

    def _read_from_db(self, query: str, params: tuple, columns: list) -> Optional[pd.DataFrame]:
        """从数据库读取数据的通用方法"""
        try:
            with get_db_cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    return df
        except Exception as e:
            print(f"[StockCache] 从数据库读取数据失败: {e}")
        return None

    def _process_time_field(self, value) -> str:
        """处理时间字段，转换为字符串格式"""
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value)

    def _read_daily_from_db(self, code: str, days: int, date_key: str) -> Optional[pd.DataFrame]:
        """从数据库读取日线数据"""
        try:
            db = next(get_db())
            try:
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
            finally:
                db.close()
        except Exception as e:
            print(f"[StockCache] 从数据库读取日线数据失败: {e}")
        return None

    def _read_single_day_from_db(self, code: str, date_key: str) -> Optional[pd.DataFrame]:
        """从数据库读取指定日期的单条日线数据"""
        try:
            db = next(get_db())
            try:
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
            finally:
                db.close()
        except Exception as e:
            print(f"[StockCache] 从数据库读取单条日线数据失败: {e}")
        return None

    def _save_daily_to_db(self, code: str, data: pd.DataFrame):
        """保存日线数据到数据库"""
        if data is None or data.empty:
            return

        try:
            db = next(get_db())
            try:
                for _, row in data.iterrows():
                    # 处理 eob 字段，确保是字符串
                    eob_str = self._process_time_field(row['eob'])
                    trade_date = eob_str[:10] if len(eob_str) >= 10 else ''

                    # 检查是否已存在
                    existing = db.query(StockDaily).filter(
                        StockDaily.code == code,
                        StockDaily.trade_date == trade_date,
                        StockDaily.eob == eob_str
                    ).first()

                    if existing:
                        # 更新现有记录
                        existing.open = row['open']
                        existing.close = row['close']
                        existing.high = row['high']
                        existing.low = row['low']
                        existing.volume = row['volume']
                        existing.amount = row['amount']
                        existing.pre_close = row['pre_close']
                        existing.update_time = datetime.now()
                    else:
                        # 创建新记录
                        new_daily = StockDaily(
                            code=code,
                            trade_date=trade_date,
                            open=row['open'],
                            close=row['close'],
                            high=row['high'],
                            low=row['low'],
                            volume=row['volume'],
                            amount=row['amount'],
                            pre_close=row['pre_close'],
                            eob=eob_str,
                            update_time=datetime.now()
                        )
                        db.add(new_daily)

                db.commit()
            except Exception as e:
                db.rollback()
                print(f"[StockCache] 保存日线数据失败: {e}")
                import traceback
                traceback.print_exc()
            finally:
                db.close()
        except Exception as e:
            print(f"[StockCache] 保存日线数据失败: {e}")
            import traceback
            traceback.print_exc()

    def get_minute_data(self, symbol: str, trade_date: datetime, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """获取指定时间段的分钟数据（带缓存）"""
        date_key = trade_date.strftime('%Y-%m-%d')

        # 将symbol转换为纯数字code
        pure_code = to_pure_code(symbol)

        # 从数据库读取
        cached_data = self._read_minute_from_db(pure_code, date_key, start_time_str, end_time_str)
        if cached_data is not None and not cached_data.empty:
            return cached_data

        # 从 API 获取（使用ExternalDataQueryHandler）
        try:
            data = self._query_handler.get_minute_data(
                symbol=symbol,
                trade_date=date_key,
                start_time=start_time_str,
                end_time=end_time_str
            )

            if data is not None and not data.empty:
                self._save_minute_to_db(pure_code, date_key, data)
                return data
            return None
        except Exception as e:
            print(f"[StockCache] 获取分钟数据失败: {e}")
            return None

    def _read_minute_from_db(self, code: str, date_key: str, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """从数据库读取分钟数据"""
        query = """
            SELECT eob, open, close, high, low, volume, amount
            FROM stock_minute
            WHERE code = ? AND trade_date = ? AND eob >= ? AND eob <= ?
            ORDER BY eob
        """
        columns = ['eob', 'open', 'close', 'high', 'low', 'volume', 'amount']
        params = (code, date_key, f"{date_key} {start_time_str}", f"{date_key} {end_time_str}")
        return self._read_from_db(query, params, columns)

    def _save_minute_to_db(self, code: str, date_key: str, data: pd.DataFrame):
        """保存分钟数据到数据库"""
        if data is None or data.empty:
            return

        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            with get_db_cursor() as cursor:
                for _, row in data.iterrows():
                    # 处理 eob 字段
                    eob_str = self._process_time_field(row['eob'])

                    cursor.execute("""
                        INSERT OR REPLACE INTO stock_minute
                        (code, trade_date, eob, open, close, high, low, volume, amount, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        code,
                        date_key,
                        eob_str,
                        row['open'],
                        row['close'],
                        row['high'],
                        row['low'],
                        int(row['volume']),
                        row['amount'],
                        update_time
                    ))
        except Exception as e:
            print(f"[StockCache] 保存分钟数据失败: {e}")

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
        获取指定时间段的tick数据
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

        # 从 API 获取（使用ExternalDataQueryHandler）
        try:
            data = self._query_handler.get_tick_data(
                symbol=symbol,
                trade_date=date_key,
                start_time=start_time_str,
                end_time=end_time_str
            )

            if data is not None and not data.empty:
                # 检查是否是竞价相关的时间范围（9:25-9:31）
                is_auction_time = self._is_auction_time(start_time_str, end_time_str)

                if is_auction_time:
                    # 只保存9:30前后的两个快照
                    data['created_at'] = pd.to_datetime(data['created_at'])

                    # 9:30前的最后一个快照
                    before_930 = data[data['created_at'] < f'{date_key} 09:30:00']
                    # 9:30后的第一个快照
                    after_930 = data[data['created_at'] >= f'{date_key} 09:30:00']

                    filtered_data = pd.DataFrame()
                    if not before_930.empty:
                        last_before = before_930.tail(1)
                        filtered_data = pd.concat([filtered_data, last_before])
                        print(f"[StockCache] 保存9:30前最后快照: {last_before['created_at'].iloc[0]}")
                    if not after_930.empty:
                        first_after = after_930.head(1)
                        filtered_data = pd.concat([filtered_data, first_after])
                        print(f"[StockCache] 保存9:30后第一快照: {first_after['created_at'].iloc[0]}")

                    if not filtered_data.empty:
                        print(f"[StockCache] 总共保存 {len(filtered_data)} 个快照")
                        self._save_tick_to_db(pure_code, date_key, filtered_data)
                        return filtered_data
                else:
                    # 保存所有tick数据
                    self._save_tick_to_db(pure_code, date_key, data)
                return data
            return None
        except Exception as e:
            print(f"[StockCache] 获取tick数据失败: {e}")
            return None

    def _read_tick_from_db(self, code: str, date_key: str, start_time_str: str, end_time_str: str) -> Optional[pd.DataFrame]:
        """从数据库读取tick数据"""
        query = """
            SELECT created_at, price, volume, cum_amount, cum_volume
            FROM stock_tick
            WHERE code = ? AND trade_date = ? AND created_at >= ? AND created_at <= ?
            ORDER BY created_at
        """
        columns = ['created_at', 'price', 'volume', 'cum_amount', 'cum_volume']
        params = (code, date_key, f"{date_key} {start_time_str}", f"{date_key} {end_time_str}")
        return self._read_from_db(query, params, columns)

    def _save_tick_to_db(self, code: str, date_key: str, data: pd.DataFrame):
        """保存tick数据到数据库"""
        if data is None or data.empty:
            return

        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            with get_db_cursor() as cursor:
                for _, row in data.iterrows():
                    # 处理 created_at 字段
                    created_at_str = self._process_time_field(row.get('created_at', ''))

                    # 处理 volume 字段，确保存在
                    volume = row.get('volume', 0)
                    try:
                        volume_int = int(volume)
                    except (ValueError, TypeError):
                        volume_int = 0

                    # 处理 cum_volume 字段，确保存在
                    cum_volume = row.get('cum_volume', 0)
                    try:
                        cum_volume_int = int(cum_volume)
                    except (ValueError, TypeError):
                        cum_volume_int = 0

                    cursor.execute("""
                        INSERT OR REPLACE INTO stock_tick
                        (code, trade_date, created_at, price, volume, cum_amount, cum_volume, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        code,
                        date_key,
                        created_at_str,
                        row.get('price', 0),
                        volume_int,
                        row.get('cum_amount', 0),
                        cum_volume_int,
                        update_time
                    ))
        except Exception as e:
            print(f"[StockCache] 保存tick数据失败: {e}")
            import traceback
            traceback.print_exc()

    def get_auction_data(self, symbol: str, trade_date: datetime) -> Dict[str, float]:
        """
        获取个股早盘竞价数据
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

        # 从 API 获取（使用ExternalDataQueryHandler）
        try:
            import time
            start_time = time.time()
            auction_data = self._query_handler.get_auction_data(symbol, date_key)
            elapsed_time = time.time() - start_time
            print(f"[StockCache] 获取竞价数据耗时: {elapsed_time:.2f} 秒")

            if auction_data is not None and not auction_data.empty:
                # 检查数据格式
                if 'cum_amount' in auction_data.columns:
                    # 掘金格式的tick数据
                    try:
                        auction_data['created_at'] = pd.to_datetime(auction_data['created_at'])

                        before_930 = auction_data[auction_data['created_at'] < f'{date_key} 09:30:00']
                        at_930 = auction_data[(auction_data['created_at'] >= f'{date_key} 09:30:00') & (auction_data['created_at'] < f'{date_key} 09:30:01')]

                        if not before_930.empty and not at_930.empty:
                            auction_amount = before_930.iloc[-1]['cum_amount']
                            open_amount = at_930.iloc[0]['cum_amount']

                            # 对于掘金数据，使用默认值填充其他字段
                            self._save_auction_to_db(pure_code, date_key, {
                                'open_amount': auction_amount,
                                'amount': auction_amount
                            })

                            return {
                                'open_volume': auction_amount,
                                'open_amount': open_amount,
                                'volume_ratio': 0
                            }
                    except Exception as e:
                        print(f"[StockCache] 处理掘金格式数据失败: {e}")
                elif 'amount' in auction_data.columns:
                    # Tushare格式的竞价数据
                    try:
                        row = auction_data.iloc[0]

                        # 确保所有数字字段都是正确的类型
                        price = self.to_float(row.get('price', 0))
                        amount = self.to_float(row.get('amount', 0))
                        volume = self.to_int(row.get('vol', row.get('volume', 0)))
                        pre_close = self.to_float(row.get('pre_close', 0))
                        turn_over_rate = self.to_float(row.get('turnover_rate', 0))
                        volume_ratio = self.to_float(row.get('volume_ratio', 0))
                        float_share = self.to_float(row.get('float_share', 0))

                        # 保存完整的竞价数据到数据库
                        self._save_auction_to_db(pure_code, date_key, {
                            'price': price,
                            'amount': amount,
                            'volume': volume,
                            'pre_close': pre_close,
                            'turn_over_rate': turn_over_rate,
                            'volume_ratio': volume_ratio,
                            'float_share': float_share
                        })

                        # open_volume和open_amount都等于竞价成交额amount
                        return {
                            'open_volume': volume,
                            'open_amount': amount,
                            'volume_ratio': volume_ratio
                        }
                    except Exception as e:
                        print(f"[StockCache] 处理Tushare格式数据失败: {e}")
        except Exception as e:
            print(f"[StockCache] 获取竞价数据失败: {e}")

        return {
            'open_volume': 0,
            'open_amount': 0,
            'volume_ratio': 0
        }

    def _read_auction_from_db(self, code: str, date_key: str) -> Optional[Dict[str, float]]:
        """从数据库读取竞价数据"""
        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    SELECT open_amount, open_volume, volume_ratio
                    FROM stock_auction
                    WHERE code = ? AND trade_date = ?
                """, (code, date_key))

                row = cursor.fetchone()
                if row:
                    open_amount = row[0]
                    open_volume = row[1]
                    volume_ratio = row[2]
                    # 确保open_amount是数字类型
                    try:
                        if open_amount is None:
                            open_amount = 0
                        elif isinstance(open_amount, bytes):
                            open_amount = 0
                        else:
                            open_amount = float(open_amount)
                    except (ValueError, TypeError):
                        open_amount = 0
                    # 确保open_volume是数字类型
                    try:
                        if open_volume is None:
                            open_volume = 0
                        elif isinstance(open_volume, bytes):
                            open_volume = 0
                        else:
                            open_volume = float(open_volume)
                    except (ValueError, TypeError):
                        open_volume = 0
                    # 确保volume_ratio是数字类型
                    try:
                        if volume_ratio is None:
                            volume_ratio = 0
                        elif isinstance(volume_ratio, bytes):
                            volume_ratio = 0
                        else:
                            volume_ratio = float(volume_ratio)
                    except (ValueError, TypeError):
                        volume_ratio = 0
                    return {
                        'open_volume': open_volume,
                        'open_amount': open_amount,
                        'volume_ratio': volume_ratio
                    }
        except Exception as e:
            print(f"[StockCache] 从数据库读取竞价数据失败: {e}")
        return None

    def _save_auction_to_db(self, code: str, date_key: str, auction_data: dict):
        """保存竞价数据到数据库

        Args:
            code: 股票代码（纯数字）
            date_key: 交易日期（YYYY-MM-DD）
            auction_data: 竞价数据字典，支持字段：
                - price/open_price: 竞价价格
                - amount/open_amount: 竞价成交额
                - volume/open_volume: 竞价成交量
                - pre_close: 前收盘价
                - turn_over_rate: 换手率
                - volume_ratio: 量比
                - float_share: 流通股本
                - tail_57_price: 尾盘14:57竞价价格
                - close_price: 收盘价
                - tail_amount: 尾盘竞价金额
        """
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        code = str(code)
        open_price = self.to_float(auction_data.get('price', auction_data.get('open_price', 0)))
        open_amount = self.to_float(auction_data.get('amount', auction_data.get('open_amount', 0)))
        open_volume = self.to_int(auction_data.get('volume', auction_data.get('open_volume', 0)))
        pre_close_val = self.to_float(auction_data.get('pre_close', 0))
        turn_over_rate = self.to_float(auction_data.get('turn_over_rate', 0))
        vol_ratio = self.to_float(auction_data.get('volume_ratio', 0))
        float_share = self.to_float(auction_data.get('float_share', 0))
        tail_57_price = self.to_float(auction_data.get('tail_57_price', 0))
        close_price = self.to_float(auction_data.get('close_price', 0))
        tail_amount = self.to_float(auction_data.get('tail_amount', 0))
        tail_volume = self.to_int(auction_data.get('tail_volume', 0))

        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_auction
                    (code, trade_date, open_price, open_amount, open_volume, pre_close, turn_over_rate, volume_ratio, float_share,
                     tail_57_price, tail_amount, tail_volume, close_price, avg_5d_price, avg_10d_price, update_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code, date_key, open_price, open_amount, open_volume, pre_close_val, turn_over_rate, vol_ratio, float_share,
                    tail_57_price, tail_amount, tail_volume, close_price, 0, 0, update_time
                ))
        except Exception as e:
            print(f"[StockCache] 保存竞价数据失败: {e}")

    def _update_auction_tail_data(self, code: str, date_key: str, tail_57_price: float, close_price: float, tail_amount: float):
        """
        更新竞价数据表中的尾盘竞价字段

        Args:
            code: 股票代码（纯数字）
            date_key: 交易日期（YYYY-MM-DD）
            tail_57_price: 尾盘14:57竞价价格
            close_price: 收盘价
            tail_amount: 尾盘竞价金额
        """
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        tail_57_price = self.to_float(tail_57_price)
        close_price = self.to_float(close_price)
        tail_amount = self.to_float(tail_amount)
        tail_volume = 0  # 默认值

        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    UPDATE stock_auction
                    SET tail_57_price = ?,
                        tail_amount = ?,
                        tail_volume = ?,
                        close_price = ?,
                        update_time = ?
                    WHERE code = ? AND trade_date = ?
                """, (tail_57_price, tail_amount, tail_volume, close_price, update_time, code, date_key))
        except Exception as e:
            print(f"[StockCache] 更新尾盘竞价数据失败: {e}")

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
