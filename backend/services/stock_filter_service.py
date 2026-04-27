import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from models import StockPerformance
from models.filter_params import FilterParams
from stock_cache import get_stock_cache
from stock_filter import get_stock_filter
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol
from stock_sqlite.database import get_db_cursor

logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()
stock_filter = get_stock_filter()


class StockFilterService:
    def filter_stocks(
        self,
        params: FilterParams
    ) -> List[Dict[str, Any]]:
        try:
            selected_block_codes = self._parse_block_codes(params.select_blocks or "")

            stocks_to_filter = get_stocks_by_blocks(selected_block_codes if selected_block_codes else None)

            if selected_block_codes:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")

            if params.only_main_board:
                main_board_stocks = {code for code in stocks_to_filter if stock_filter.check_is_main_board(code)}
                logger.info(f"主板过滤：从 {len(stocks_to_filter)} 只股票中筛选出 {len(main_board_stocks)} 只主板股票")
                stocks_to_filter = main_board_stocks

            filtered_results = []

            trade_date = datetime.now()

            for code in stocks_to_filter:
                if not code:
                    continue

                symbol = to_goldminer_symbol(code)

                try:
                    performance = stock_filter.check_performance(
                        symbol=symbol,
                        trade_date=trade_date,
                        interval_days=params.interval_days,
                        interval_max_rise=params.interval_max_rise,
                        recent_days=params.recent_days,
                        recent_max_day_rise=params.recent_max_day_rise,
                        prev_high_price_rate=params.prev_high_price_rate
                    )

                    if not performance.is_pass:
                        continue

                    stock_name = stock_cache.get_stock_name(symbol)

                    filtered_results.append({
                        'code': code,
                        'name': stock_name,
                        'interval_max_rise': performance.interval_max_rise,
                        'max_day_rise': performance.max_day_rise,
                        'prev_high_price_rate': performance.prev_high_price_rate
                    })

                except Exception as e:
                    logger.error(f"Error processing stock {code}: {str(e)}")
                    continue

            logger.info(f"Filter completed, found {len(filtered_results)} stocks")

            self.update_filter_config(
                config_type=2,
                interval_days=params.interval_days,
                interval_max_rise=params.interval_max_rise,
                recent_days=params.recent_days,
                recent_max_day_rise=params.recent_max_day_rise,
                prev_high_price_rate=params.prev_high_price_rate,
                block_codes=params.select_blocks or ""
            )

            return filtered_results

        except Exception as e:
            logger.error(f"Error in filter_stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def update_filter_config(
        self,
        config_type: int,
        interval_days: int,
        interval_max_rise: float,
        recent_days: int,
        recent_max_day_rise: float,
        prev_high_price_rate: float,
        block_codes: str
    ):
        try:
            with get_db_cursor() as cursor:
                cursor.execute("SELECT * FROM filter_config WHERE type = ?", (config_type,))
                existing = cursor.fetchone()

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if existing:
                    cursor.execute("""
                        UPDATE filter_config 
                        SET interval_days = ?, interval_max_rise = ?, recent_days = ?, 
                            recent_max_day_rise = ?, prev_high_price_rate = ?, select_blocks = ?, update_time = ?
                        WHERE type = ?
                    """, (interval_days, interval_max_rise, recent_days, recent_max_day_rise, prev_high_price_rate, block_codes, now, config_type))
                    logger.info(f"Updated filter config for type={config_type}")
                else:
                    cursor.execute("""
                        INSERT INTO filter_config 
                        (type, interval_days, interval_max_rise, recent_days, recent_max_day_rise, 
                         prev_high_price_rate, select_blocks, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (config_type, interval_days, interval_max_rise, recent_days, recent_max_day_rise, prev_high_price_rate, block_codes, now))
                    logger.info(f"Created filter config for type={config_type}")
        except Exception as e:
            logger.error(f"Error updating filter config: {str(e)}")

    def _parse_block_codes(self, block_codes: str) -> List[str]:
        if block_codes:
            return [code.strip() for code in block_codes.split(',') if code.strip()]
        return []


_stock_filter_service: Optional[StockFilterService] = None


def get_stock_filter_service() -> StockFilterService:
    global _stock_filter_service
    if _stock_filter_service is None:
        _stock_filter_service = StockFilterService()
    return _stock_filter_service
