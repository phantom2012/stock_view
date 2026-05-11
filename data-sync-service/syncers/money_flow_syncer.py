"""
资金流向数据同步器
仅负责从外部接口拉取资金流向数据并保存到 stock_money_flow 表
转强字段计算已迁移到 backend/stock_filter/stock_money_analyzer.py
"""
import logging
from datetime import datetime
from typing import List, Set, Tuple, Dict, Any

from shared.db import (
    get_session, get_session_ro, StockMoneyFlow, FilterResult,
    upsert_by_unique_keys
)
from shared.stock_code_convert import to_goldminer_symbol
from shared.trade_date_util import TradeDateUtil
from external_data import get_query_handler
from config import MONEY_FLOW_CONFIG
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
SCAN_DAYS = 30


class MoneyFlowSyncer(BaseSyncer):
    """
    资金流向数据同步器
    1. 从 filter_results 表获取所有股票代码
    2. 检查 stock_money_flow 表中最近30个交易日的数据是否完整
    3. 如有缺失则调用外部接口补充
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始资金流向数据同步 =====")
        try:
            if stock_codes is None:
                stock_codes = self._get_filter_stock_codes()

            if not stock_codes:
                logger.warning("filter_results 表中没有股票数据，跳过同步")
                return True, 0, 0, "无股票数据"

            all_trade_dates = trade_date_util.get_recent_trade_dates(SCAN_DAYS)
            if not all_trade_dates:
                return False, 0, 0, "获取交易日列表失败"

            logger.info(f"最近 {SCAN_DAYS} 个交易日: {all_trade_dates[0]} ~ {all_trade_dates[-1]}")

            query_handler = get_query_handler()
            total_stocks = len(stock_codes)
            total_saved = 0
            failed_stocks = 0

            for idx, code in enumerate(stock_codes, 1):
                try:
                    missing_dates = self._get_missing_trade_dates(code, all_trade_dates)
                    if not missing_dates:
                        continue

                    log_progress(f"[{idx}/{total_stocks}] {code}: 缺失 {len(missing_dates)} 个交易日", idx, total_stocks)
                    start_date = missing_dates[0]
                    end_date = missing_dates[-1]

                    symbol = to_goldminer_symbol(code)
                    df = query_handler.get_money_flow_data(
                        symbol=symbol, start_date=start_date, end_date=end_date
                    )

                    if df is None or df.empty:
                        logger.warning(f"  外部接口未返回 {code} 的数据")
                        failed_stocks += 1
                        continue

                    saved = self._save_money_flow_records(code, df)
                    total_saved += saved

                except Exception as e:
                    logger.error(f"  同步 {code} 失败: {e}")
                    failed_stocks += 1
                    continue

            logger.info("===== 资金流向数据同步完成 =====")
            logger.info(f"总股票数: {total_stocks}, 成功保存: {total_saved}, 失败: {failed_stocks}")

            return True, total_saved, failed_stocks, f"同步{total_saved}条, 失败{failed_stocks}只"

        except Exception as e:
            logger.error(f"资金流向数据同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    def _get_filter_stock_codes(self) -> List[str]:
        """从 filter_results 表获取所有股票代码"""
        try:
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                codes = [row[0] for row in rows if row[0]]
                logger.info(f"从 filter_results 获取到 {len(codes)} 个股票代码")
                return codes
        except Exception as e:
            logger.error(f"获取 filter_results 股票代码失败: {e}")
            return []

    def _get_existing_trade_dates(self, code: str) -> Set[str]:
        """查询某只股票在 stock_money_flow 表中已有的交易日"""
        try:
            with get_session_ro() as db:
                rows = db.query(StockMoneyFlow.trade_date).filter(
                    StockMoneyFlow.code == code
                ).all()
                return {row[0] for row in rows if row[0]}
        except Exception as e:
            logger.error(f"查询 {code} 已有交易日失败: {e}")
            return set()

    def _get_missing_trade_dates(self, code: str, all_trade_dates: List[str]) -> List[str]:
        """获取某只股票缺失的交易日列表"""
        existing = self._get_existing_trade_dates(code)
        return [d for d in all_trade_dates if d not in existing]

    def _save_money_flow_records(self, code: str, df) -> int:
        """将 DataFrame 格式的资金流向数据保存到数据库"""
        if df is None or df.empty:
            return 0

        df_sorted = df.sort_values(by='trade_date').reset_index(drop=True)
        records = []
        for _, row in df_sorted.iterrows():
            trade_date = str(row.get('trade_date', ''))
            if len(trade_date) == 8:
                trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            records.append({
                'trade_date': trade_date,
                'pct_change': float(row.get('pct_change', 0)),
                'net_amount': float(row.get('net_amount', 0)),
                'name': row.get('name', ''),
                'close': row.get('close', 0),
                'net_amount_rate': row.get('net_amount_rate', 0),
                'net_d5_amount': row.get('net_d5_amount', 0),
                'buy_elg_amount': row.get('buy_elg_amount', 0),
                'buy_elg_amount_rate': row.get('buy_elg_amount_rate', 0),
                'buy_lg_amount': row.get('buy_lg_amount', 0),
                'buy_lg_amount_rate': row.get('buy_lg_amount_rate', 0),
                'buy_md_amount': row.get('buy_md_amount', 0),
                'buy_md_amount_rate': row.get('buy_md_amount_rate', 0),
                'buy_sm_amount': row.get('buy_sm_amount', 0),
                'buy_sm_amount_rate': row.get('buy_sm_amount_rate', 0),
            })

        saved_count = 0
        with get_session() as db:
            for rec in records:
                trade_date = rec['trade_date']
                unique_keys = {'code': code, 'trade_date': trade_date}
                update_data = {k: v for k, v in rec.items() if k != 'trade_date'}
                update_data['update_time'] = datetime.now()
                upsert_by_unique_keys(db, StockMoneyFlow, unique_keys, update_data)
                saved_count += 1
        return saved_count
