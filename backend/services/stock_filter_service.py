import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from stock_cache import get_stock_cache
from stock_filter import get_stock_filter
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol

logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()
stock_filter = get_stock_filter()


class StockFilterService:
    def filter_stocks(
        self,
        recent_days: int = 10,
        max_gain: float = 20,
        daily_gain_days: int = 5,
        daily_gain_threshold: float = 7,
        price_ratio: float = 90,
        block_codes: str = "",
        only_main_board: bool = False
    ) -> List[Dict[str, Any]]:
        try:
            selected_block_codes = self._parse_block_codes(block_codes)

            stocks_to_filter = get_stocks_by_blocks(selected_block_codes if selected_block_codes else None)

            if selected_block_codes:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")

            if only_main_board:
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
                    is_pass, period_gain, max_daily_gain, price_ratio_value = stock_filter.check_performance(
                        symbol=symbol,
                        trade_date=trade_date,
                        recent_interval_days=recent_days,
                        recent_interval_max_gain=max_gain,
                        day_max_gain_days=daily_gain_days,
                        day_max_gain=daily_gain_threshold,
                        price_to_high_ratio=price_ratio
                    )

                    if not is_pass:
                        continue

                    stock_name = stock_cache.get_stock_name(symbol)

                    filtered_results.append({
                        'code': code,
                        'name': stock_name,
                        'gain': period_gain,
                        'max_daily_gain': max_daily_gain,
                        'price_to_high_ratio': price_ratio_value
                    })

                except Exception as e:
                    logger.error(f"Error processing stock {code}: {str(e)}")
                    continue

            logger.info(f"Filter completed, found {len(filtered_results)} stocks")
            return filtered_results

        except Exception as e:
            logger.error(f"Error in filter_stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

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
