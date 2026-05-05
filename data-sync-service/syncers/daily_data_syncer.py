"""
日线数据同步器
从 backend/stock_cache/__init__.py 中的 get_history_data / get_stock_day_data 迁移
每日16:00执行，从掘金接口获取日线数据并写入 stock_daily 表
"""
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from shared.db import get_session, get_session_ro, StockDaily, FilterConfig, BlockStock
from shared.stock_code_convert import to_goldminer_symbol, to_pure_code
from shared.trade_date_util import TradeDateUtil
from external_data import get_query_handler
from config import DAILY_DATA_CONFIG
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
DEFAULT_DAYS = DAILY_DATA_CONFIG.get('default_days', 90)  # 默认90个交易日，可配置


class DailyDataSyncer(BaseSyncer):
    """
    日线数据同步器
    每日16:00执行，从掘金接口获取日线数据并写入 stock_daily 表
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始日线数据同步 =====")
        try:
            # 如果没有传入股票列表，从板块配置获取
            if stock_codes is None:
                stock_codes = self._get_stock_codes_from_blocks()

            if not stock_codes:
                logger.warning("未获取到板块配置对应的股票数据，跳过同步")
                return True, 0, 0, "无股票数据"

            latest_trade_date = trade_date_util.get_latest_trade_date()
            if not latest_trade_date:
                return False, 0, 0, "获取最新交易日失败"

            # 获取最近需要同步的交易日列表
            expected_dates = trade_date_util.get_recent_trade_dates(days=DEFAULT_DAYS + 10)
            expected_count = len(expected_dates)
            if expected_count == 0:
                logger.warning("未获取到交易日列表")
                return False, 0, 0, "未获取到交易日列表"

            logger.info(f"期望同步最近 {expected_count} 个交易日数据")

            query_handler = get_query_handler()
            total_saved = 0
            failed_count = 0
            skipped_count = 0

            for idx, code in enumerate(stock_codes, 1):
                try:
                    # 先检查数据库中该股票在本次同步时间范围内已有多少条日线数据
                    existing_count = self._get_existing_daily_count(code, expected_dates[0] if expected_dates else None)

                    total_stocks = len(stock_codes)
                    # 如果数据量已满足，跳过查询外部接口
                    if existing_count >= expected_count:
                        log_progress(f"[{idx}/{total_stocks}] {code}: 已有 {existing_count} 条数据，无需同步", idx, total_stocks)
                        skipped_count += 1
                        continue

                    symbol = to_goldminer_symbol(code)
                    log_progress(f"[{idx}/{total_stocks}] {code}: 获取日线数据...(已有 {existing_count} 条，期望 {expected_count} 条)", idx, total_stocks)

                    data = query_handler.get_daily_data(
                        symbol=symbol,
                        start_date=expected_dates[0] if expected_dates else (datetime.now() - timedelta(days=DEFAULT_DAYS + 20)).strftime('%Y-%m-%d'),
                        end_date=latest_trade_date
                    )

                    if data is None or data.empty:
                        logger.warning(f"  {code}: 未获取到日线数据")
                        failed_count += 1
                        continue

                    saved = self._save_daily_to_db(code, data)
                    total_saved += saved
                    if saved > 0:
                        logger.info(f"  {code}: 保存 {saved} 条日线数据")

                except Exception as e:
                    logger.error(f"  {code}: 同步失败: {e}")
                    failed_count += 1
                    continue

            logger.info("===== 日线数据同步完成 =====")
            logger.info(f"总股票数: {len(stock_codes)}, 成功保存: {total_saved}, 跳过: {skipped_count}, 失败: {failed_count}")
            return True, total_saved, failed_count, f"同步{total_saved}条, 跳过{skipped_count}条"

        except Exception as e:
            logger.error(f"日线数据同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    def _get_existing_daily_count(self, code: str, start_date: str = None) -> int:
        """获取数据库中该股票在指定时间范围内已有的日线数据数量"""
        try:
            with get_session_ro() as db:
                query = db.query(StockDaily).filter(StockDaily.code == code)
                # 如果指定了起始日期，只统计该日期及之后的数据
                if start_date:
                    query = query.filter(StockDaily.trade_date >= start_date)
                count = query.count()
                return count
        except Exception as e:
            logger.error(f"查询股票 {code} 日线数据数量失败: {e}")
            return 0

    def _get_stock_codes_from_blocks(self) -> List[str]:
        """
        从 filter_config 获取 type=2 的板块配置，然后从 block_stock 查询对应的股票列表
        与其他同步器的 _get_filter_stock_codes 不同，此方法从板块关联获取股票，而非筛选结果表
        """
        try:
            with get_session_ro() as db:
                # 获取 type=2 的筛选配置
                config = db.query(FilterConfig).filter(
                    FilterConfig.type == 2
                ).first()

                if not config or not config.select_blocks:
                    logger.warning("filter_config 中 type=2 的配置不存在或未设置 select_blocks")
                    return []

                # 解析板块列表（逗号分隔）
                block_codes = [b.strip() for b in config.select_blocks.split(',') if b.strip()]
                if not block_codes:
                    logger.warning("select_blocks 为空")
                    return []

                logger.info(f"从 filter_config 获取到 {len(block_codes)} 个板块: {block_codes}")

                # 从 block_stock 表查询这些板块对应的所有股票（去重）
                rows = db.query(BlockStock.stock_code).filter(
                    BlockStock.block_code.in_(block_codes)
                ).distinct().all()

                stock_codes = [row[0] for row in rows if row[0]]
                logger.info(f"从 {len(block_codes)} 个板块中获取到 {len(stock_codes)} 只股票（已去重）")

                return stock_codes

        except Exception as e:
            logger.error(f"获取板块股票代码失败: {e}")
            import traceback; traceback.print_exc()
            return []

    def _save_daily_to_db(self, code: str, data) -> int:
        """保存日线数据到数据库，跳过已存在的数据"""
        if data is None or data.empty:
            return 0

        saved_count = 0
        try:
            with get_session() as db:
                # 先查询该股票已有的所有 trade_date
                existing_dates = db.query(StockDaily.trade_date).filter(
                    StockDaily.code == code
                ).distinct().all()
                existing_date_set = {row[0] for row in existing_dates}

                for _, row in data.iterrows():
                    eob_str = self._process_time_field(row['eob'])
                    trade_date = eob_str[:10] if len(eob_str) >= 10 else ''

                    # 如果该日期数据已存在，跳过
                    if trade_date in existing_date_set:
                        continue

                    db.add(StockDaily(
                        code=code, trade_date=trade_date,
                        open=row['open'], close=row['close'],
                        high=row['high'], low=row['low'],
                        volume=row['volume'], amount=row['amount'],
                        pre_close=row['pre_close'], eob=eob_str,
                        update_time=datetime.now()
                    ))
                    saved_count += 1
                db.commit()
        except Exception as e:
            logger.error(f"保存日线数据失败: {e}")
            import traceback; traceback.print_exc()
        return saved_count

    @staticmethod
    def _process_time_field(value) -> str:
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value)
