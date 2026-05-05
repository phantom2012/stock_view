"""
竞价数据同步器
从 stock_minute 表获取分时数据，计算早盘和尾盘竞价信息，写入 stock_auction 表
"""
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Any

from shared.db import (
    get_session, get_session_ro, StockAuction, StockMinute, StockDaily, FilterResult,
    upsert_by_unique_keys
)
from shared.trade_date_util import TradeDateUtil
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
DEFAULT_DAYS = 30


class AuctionDataSyncer(BaseSyncer):

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始竞价数据同步 =====")
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

            recent_trade_dates.reverse()
            logger.info(f"获取到 {len(recent_trade_dates)} 个交易日")

            success_count = 0
            fail_count = 0

            for stock_idx, code in enumerate(stock_codes, 1):
                if not code:
                    fail_count += 1
                    continue

                try:
                    stock_success = self._sync_stock_auction(code, recent_trade_dates)
                    if stock_success:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(f"  {code}: 同步失败: {e}")
                    fail_count += 1

                log_progress(f"进度: [{stock_idx}/{len(stock_codes)}]", stock_idx, len(stock_codes))

            logger.info("===== 竞价数据同步完成 =====")
            logger.info(f"总股票数: {len(stock_codes)}, 成功: {success_count}, 失败: {fail_count}")
            return True, success_count, fail_count, f"处理{success_count}只"

        except Exception as e:
            logger.error(f"竞价数据同步异常: {e}")
            return False, 0, 0, str(e)

    def _sync_stock_auction(self, code: str, trade_dates: List[str]) -> bool:
        minute_map = self._get_minute_data_map(code, trade_dates)
        if not minute_map:
            return True

        daily_map = self._get_daily_data_map(code, trade_dates)
        auction_map = self._get_auction_data_map(code, trade_dates)

        stock_success = True
        auction_updates = []

        for date_str in trade_dates:
            if date_str not in minute_map:
                continue

            try:
                auction_data = self._calculate_auction_data(
                    code, date_str, trade_dates, minute_map, daily_map, auction_map
                )
                if auction_data:
                    auction_updates.append((date_str, auction_data))
                    auction_map[date_str] = auction_data
            except Exception as e:
                logger.error(f"  {code} {date_str}: 处理失败: {e}")
                stock_success = False

        if auction_updates:
            self._batch_save_auction_to_db(code, auction_updates)

        return stock_success

    def _get_filter_stock_codes(self) -> List[str]:
        try:
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                return [row[0] for row in rows if row[0]]
        except Exception as e:
            logger.error(f"获取 filter_results 股票代码失败: {e}")
            return []

    def _get_minute_data_map(self, code: str, trade_dates: List[str]) -> Dict[str, Dict[str, Any]]:
        try:
            with get_session_ro() as db:
                rows = db.query(StockMinute).filter(
                    StockMinute.code == code,
                    StockMinute.trade_date.in_(trade_dates)
                ).all()
                result = {}
                for row in rows:
                    if row.trade_date not in result:
                        result[row.trade_date] = {}
                    time_part = row.eob.split(' ')[1] if ' ' in row.eob else row.eob
                    result[row.trade_date][time_part] = row
                return result
        except Exception as e:
            logger.error(f"查询 {code} minute数据失败: {e}")
            return {}

    def _get_daily_data_map(self, code: str, trade_dates: List[str]) -> Dict[str, Any]:
        try:
            with get_session_ro() as db:
                rows = db.query(StockDaily).filter(
                    StockDaily.code == code,
                    StockDaily.trade_date.in_(trade_dates)
                ).all()
                return {row.trade_date: row for row in rows}
        except Exception as e:
            logger.error(f"查询 {code} daily数据失败: {e}")
            return {}

    def _get_auction_data_map(self, code: str, trade_dates: List[str]) -> Dict[str, Dict]:
        try:
            with get_session_ro() as db:
                rows = db.query(StockAuction).filter(
                    StockAuction.code == code,
                    StockAuction.trade_date.in_(trade_dates)
                ).all()
                return {row.trade_date: {'open_volume': row.open_volume} for row in rows}
        except Exception as e:
            logger.error(f"查询 {code} auction数据失败: {e}")
            return {}

    def _calculate_auction_data(self, code: str, date_str: str, trade_dates: List[str],
                                minute_map: Dict, daily_map: Dict, auction_map: Dict) -> Optional[Dict]:
        date_minute = minute_map.get(date_str, {})
        morning_data = date_minute.get("09:25:00")
        if not morning_data:
            return None

        result = {
            'open_price': self._to_float(morning_data.close),
            'open_amount': self._to_float(morning_data.amount),
            'open_volume': self._to_int(morning_data.volume),
        }

        tail_57_data = date_minute.get("14:57:00")
        if tail_57_data:
            result['tail_57_price'] = self._to_float(tail_57_data.close)

        tail_00_data = date_minute.get("15:00:00")
        if tail_00_data:
            result['tail_amount'] = self._to_float(tail_00_data.amount)
            result['tail_volume'] = self._to_int(tail_00_data.volume)

        daily_data = daily_map.get(date_str)
        if daily_data:
            result['pre_close'] = self._to_float(daily_data.pre_close)
            result['close_price'] = self._to_float(daily_data.close)

        prev_open_volume = self._find_valid_prev_open_volume(date_str, minute_map, auction_map)
        if prev_open_volume and prev_open_volume > 0:
            result['volume_ratio'] = round(self._to_float(morning_data.volume) / prev_open_volume, 2)

        return result

    def _find_valid_prev_open_volume(self, date_str: str, minute_map: Dict, auction_map: Dict) -> Optional[float]:
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        prev_date_str = trade_date_util.get_previous_trade_date(current_date)

        if not prev_date_str:
            logger.warning(f"未找到 {date_str} 之前的上一个交易日")
            return None

        prev_minute = minute_map.get(prev_date_str, {}).get("09:25:00")
        if prev_minute and prev_minute.volume and prev_minute.volume > 0:
            return float(prev_minute.volume)

        prev_auction = auction_map.get(prev_date_str)
        if prev_auction and prev_auction.get('open_volume') and prev_auction['open_volume'] > 0:
            return float(prev_auction['open_volume'])

        logger.debug(f"{date_str} 的上一个交易日 {prev_date_str} 没有有效开盘量数据")
        return None

    def _batch_save_auction_to_db(self, code: str, updates: List[Tuple[str, dict]]):
        try:
            with get_session() as db:
                for date_key, auction_data in updates:
                    upsert_by_unique_keys(
                        db,
                        StockAuction,
                        unique_keys={'code': code, 'trade_date': date_key},
                        update_data={
                            **auction_data,
                            'update_time': datetime.now()
                        }
                    )
                db.commit()
        except Exception as e:
            logger.error(f"批量保存竞价数据失败: {e}")

    @staticmethod
    def _to_float(value, default=0):
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_int(value, default=0):
        try:
            if value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
