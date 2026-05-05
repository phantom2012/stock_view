"""
分钟数据同步器（优化版）
每日16:00执行，获取股票早盘和尾盘的分钟级别数据并写入 stock_minute 表

数据获取策略（批量优化）：
- 早盘 9:15~9:40：
  - 9:25 和 9:30 的快照数据使用 get_tick_data_batch（掘金接口）- 批量查询
  - 9:30~9:40 的分钟数据使用 get_minute_data_batch - 批量查询
- 尾盘 14:50~15:00：
  - 使用 get_minute_data_batch 获取分钟数据 - 批量查询

性能优化：
- 使用掘金批量接口，每批最多50只股票
- 先批量获取所有股票数据，再统一入库（按时间顺序）
- 减少网络请求次数，提升同步效率
"""
import logging
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Optional

from shared.db import get_session, get_session_ro, StockMinute, FilterResult
from shared.stock_code_convert import to_goldminer_symbol, to_pure_code
from shared.trade_date_util import TradeDateUtil
from external_data import get_query_handler
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
DEFAULT_DAYS = 5  # 最近5个交易日
BATCH_SIZE = 50   # 掘金接口限制：每批最多50只股票


class MinuteDataSyncer(BaseSyncer):
    """
    分钟数据同步器（优化版）
    每日16:00执行，批量获取股票早盘和尾盘的分钟级别数据并写入 stock_minute 表
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始分钟数据同步（批量优化版）=====")
        try:
            # 如果没有传入股票列表，从 filter_results 表读取
            if stock_codes is None:
                stock_codes = self._get_filter_stock_codes()
            
            if not stock_codes:
                logger.warning("filter_results 表中没有股票数据，跳过同步")
                return True, 0, 0, "无股票数据"

            recent_trade_dates = trade_date_util.get_recent_trade_dates(DEFAULT_DAYS)
            if not recent_trade_dates:
                return False, 0, 0, "获取交易日列表失败"

            logger.info(f"获取到 {len(recent_trade_dates)} 个交易日: {recent_trade_dates}")
            logger.info(f"获取到 {len(stock_codes)} 只股票")

            query_handler = get_query_handler()

            # 转换为掘金symbol格式
            symbols = [to_goldminer_symbol(code) for code in stock_codes if code]

            total_success = 0
            total_fail = 0

            for date_str in recent_trade_dates:
                logger.info(f"处理日期: {date_str}")

                # 1. 批量获取早盘分钟数据（9:30~9:40）
                morning_minute_data = query_handler.get_minute_data_batch(
                    symbols=symbols, trade_date=date_str,
                    start_time="09:30:00", end_time="09:40:00",
                    batch_size=BATCH_SIZE
                )

                # 2. 批量获取尾盘分钟数据（14:50~15:00）
                afternoon_minute_data = query_handler.get_minute_data_batch(
                    symbols=symbols, trade_date=date_str,
                    start_time="14:50:00", end_time="15:00:00",
                    batch_size=BATCH_SIZE
                )

                # 3. 批量获取Tick快照数据（9:25, 9:30）
                tick_snapshots = self._get_morning_tick_snapshots_batch(query_handler, symbols, date_str)

                # 4. 统一入库（按时间顺序）
                success_count, fail_count = self._batch_save_to_db(
                    stock_codes, date_str,
                    morning_minute_data, afternoon_minute_data, tick_snapshots
                )

                total_success += success_count
                total_fail += fail_count

            logger.info("===== 分钟数据同步完成 =====")
            logger.info(f"总股票数: {len(stock_codes)}, 成功: {total_success}, 失败: {total_fail}")
            return True, total_success, total_fail, f"处理{total_success}只"

        except Exception as e:
            logger.error(f"分钟数据同步异常: {e}")
            import traceback
            traceback.print_exc()
            return False, 0, 0, str(e)

    def _get_filter_stock_codes(self) -> List[str]:
        """获取 filter_results 表中的股票代码列表"""
        try:
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                return [row[0] for row in rows if row[0]]
        except Exception as e:
            logger.error(f"获取 filter_results 股票代码失败: {e}")
            return []

    def _get_morning_tick_snapshots_batch(self, query_handler, symbols: List[str], date_str: str) -> List[dict]:
        """批量获取多只股票的早盘Tick快照（9:25, 9:30）

        简化逻辑：直接取查询结果的第一张（9:25）和最后一张（9:30）数据
        """
        snapshots = []

        # 使用批量接口获取所有股票的Tick数据
        tick_data = query_handler.get_tick_data_batch(
            symbols=symbols, trade_date=date_str,
            start_time="09:24:59", end_time="09:30:00",
            batch_size=BATCH_SIZE
        )

        if tick_data is None or tick_data.empty:
            return snapshots

        tick_data['created_at'] = pd.to_datetime(tick_data['created_at'])

        # 按股票分组处理
        grouped = tick_data.groupby('symbol')

        for symbol, group in grouped:
            code = to_pure_code(symbol)

            # 筛选非零价格的数据
            valid_data = group[group['price'] > 0]
            if valid_data.empty:
                continue

            # 按时间排序
            valid_data = valid_data.sort_values('created_at')

            # 9:25 快照 - 取第一张（最早的）
            first_row = valid_data.iloc[0]
            snapshots.append({
                'code': code,
                'trade_date': date_str,
                'eob': f"{date_str} 09:25:00",
                'open': first_row['price'],
                'close': first_row['price'],
                'high': first_row['price'],
                'low': first_row['price'],
                'volume': int(first_row.get('cum_volume', 0)),
                'amount': first_row['cum_amount'],
            })

            # 9:30 快照 - 取最后一张（最晚的）
            last_row = valid_data.iloc[-1]
            snapshots.append({
                'code': code,
                'trade_date': date_str,
                'eob': f"{date_str} 09:30:00",
                'open': last_row['price'],
                'close': last_row['price'],
                'high': last_row['price'],
                'low': last_row['price'],
                'volume': int(last_row.get('cum_volume', 0)),
                'amount': last_row['cum_amount'],
            })

        logger.info(f"  从Tick数据中提取了 {len(snapshots)} 个快照")
        return snapshots

    def _batch_save_to_db(self, stock_codes: List[str], date_str: str,
                          morning_data: Optional[pd.DataFrame],
                          afternoon_data: Optional[pd.DataFrame],
                          tick_snapshots: List[dict]) -> Tuple[int, int]:
        """批量保存所有数据到数据库（按时间顺序）"""
        success_count = 0
        fail_count = 0

        try:
            with get_session() as db:
                # 1. 先保存Tick快照数据（9:25, 9:30）- 最早的时间
                for snapshot in tick_snapshots:
                    self._upsert_minute_record_from_snapshot(db, snapshot)

                # 2. 保存早盘分钟数据（9:30~9:40）
                if morning_data is not None and not morning_data.empty:
                    # 按时间排序后插入
                    morning_data_sorted = morning_data.sort_values('eob')
                    for _, row in morning_data_sorted.iterrows():
                        code = to_pure_code(row['symbol'])
                        eob_str = self._process_time_field(row['eob'])
                        self._upsert_minute_record(db, code, date_str, eob_str, row)

                # 3. 保存尾盘分钟数据（14:50~15:00）- 最晚的时间
                if afternoon_data is not None and not afternoon_data.empty:
                    # 按时间排序后插入
                    afternoon_data_sorted = afternoon_data.sort_values('eob')
                    for _, row in afternoon_data_sorted.iterrows():
                        code = to_pure_code(row['symbol'])
                        eob_str = self._process_time_field(row['eob'])
                        self._upsert_minute_record(db, code, date_str, eob_str, row)

                db.commit()
                success_count = len(stock_codes)

        except Exception as e:
            logger.error(f"批量保存数据失败: {e}")
            fail_count = len(stock_codes)

        return success_count, fail_count

    def _upsert_minute_record(self, db, code: str, trade_date: str, eob_str: str, row):
        """更新或插入分钟数据记录"""
        existing = db.query(StockMinute).filter(
            StockMinute.code == code,
            StockMinute.trade_date == trade_date,
            StockMinute.eob == eob_str
        ).first()

        if existing:
            existing.open = row['open']
            existing.close = row['close']
            existing.high = row['high']
            existing.low = row['low']
            existing.volume = int(row['volume'])
            existing.amount = row['amount']
            existing.update_time = datetime.now()
        else:
            db.add(StockMinute(
                code=code, trade_date=trade_date, eob=eob_str,
                open=row['open'], close=row['close'],
                high=row['high'], low=row['low'],
                volume=int(row['volume']), amount=row['amount'],
                update_time=datetime.now()
            ))

    def _upsert_minute_record_from_snapshot(self, db, snapshot: dict):
        """从快照字典更新或插入分钟数据记录"""
        code = snapshot['code']
        trade_date = snapshot['trade_date']
        eob_str = snapshot['eob']

        existing = db.query(StockMinute).filter(
            StockMinute.code == code,
            StockMinute.trade_date == trade_date,
            StockMinute.eob == eob_str
        ).first()

        if existing:
            existing.open = snapshot['open']
            existing.close = snapshot['close']
            existing.high = snapshot['high']
            existing.low = snapshot['low']
            existing.volume = snapshot['volume']
            existing.amount = snapshot['amount']
            existing.update_time = datetime.now()
        else:
            db.add(StockMinute(
                code=code, trade_date=trade_date, eob=eob_str,
                open=snapshot['open'], close=snapshot['close'],
                high=snapshot['high'], low=snapshot['low'],
                volume=snapshot['volume'], amount=snapshot['amount'],
                update_time=datetime.now()
            ))

    @staticmethod
    def _process_time_field(value) -> str:
        """处理时间字段，转换为字符串格式"""
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value)
