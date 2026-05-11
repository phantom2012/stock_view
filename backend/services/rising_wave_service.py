import logging
from datetime import datetime
from typing import Optional, List

from common.stock_code_convert import to_goldminer_symbol
from common.db_utils import upsert_by_unique_keys
from shared.db import get_session, FilterResult, StockScore
from shared.trade_date_util import TradeDateUtil
from stock_filter.stock_wave_analyzer import StockWaveAnalyzer
from stock_cache import get_stock_cache

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()


class RisingWaveService:
    def __init__(self):
        self.cache = get_stock_cache()
        self.wave_analyzer = StockWaveAnalyzer(self.cache)

    def run_recalc_rising_wave(self, codes: Optional[List[str]] = None):
        """
        重新计算升浪形态得分

        Args:
            codes: 股票代码列表（可选），为 None 则计算所有 filter_results(type=1) 中的股票
        """
        logger.info(f"===== 开始升浪复算: codes={codes} =====")
        try:
            if codes is None:
                with get_session() as db:
                    rows = db.query(FilterResult).filter(FilterResult.type == 1).distinct().all()
                    codes = list({row.code for row in rows if row.code})

            if not codes:
                logger.warning("无股票数据，跳过升浪复算")
                return

            success_count = 0
            failed_count = 0
            for code in codes:
                try:
                    self._calculate_and_update(code)
                    success_count += 1
                except Exception as e:
                    logger.error(f"升浪复算失败 {code}: {e}")
                    failed_count += 1

            logger.info(f"===== 升浪复算完成: 成功 {success_count}, 失败 {failed_count} =====")
        except Exception as e:
            logger.error(f"升浪复算异常: {e}")
            import traceback; traceback.print_exc()

    def _calculate_and_update(self, code: str):
        """计算单只股票的升浪得分并更新数据库"""
        symbol = to_goldminer_symbol(code)

        with get_session() as db:
            rows = db.query(FilterResult).filter(
                FilterResult.type == 1,
                FilterResult.code == code
            ).all()

            if not rows:
                logger.warning(f"[{code}] 无 filter_results 记录，跳过")
                return

            for row in rows:
                trade_date_str = row.trade_date
                if not trade_date_str:
                    continue

                trade_date = datetime.strptime(trade_date_str, '%Y-%m-%d')
                score = self.wave_analyzer.calculate_rising_wave_score(symbol, trade_date)

                update_data = {
                    'rising_wave_score': score,
                    'update_time': datetime.now(),
                }

                upsert_by_unique_keys(
                    db,
                    StockScore,
                    unique_keys={'code': code, 'trade_date': trade_date_str},
                    update_data=update_data
                )

            db.commit()


def get_rising_wave_service() -> RisingWaveService:
    return RisingWaveService()
