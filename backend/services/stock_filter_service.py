import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from models import StockPerformance, get_session, get_session_ro
from models.filter_params import FilterParams
from stock_cache import get_stock_cache
from stock_filter import get_stock_filter
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol
from common.singleton import SingletonMixin
from services.data_sync_notify_service import get_data_sync_notify_service

logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()
stock_filter = get_stock_filter()


class StockFilterService(SingletonMixin):
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
                        params=params
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
                params=params
            )

            # 保存筛选结果到 FilterResult 表
            self._save_filter_results(filtered_results)

            # 触发同步任务（在后台异步执行，不影响筛选结果返回）
            # 筛选完成后同步 daily_data（日线数据）
            try:
                notify_service = get_data_sync_notify_service()
                sync_result = notify_service.trigger_multi_sync(
                    sync_types=['minute_data'],
                    stock_codes= [stock['code'] for stock in filtered_results]
                )
                logger.info(f"Screen sync trigger result: {sync_result}")
            except Exception as e:
                logger.error(f"Failed to trigger screen sync: {e}")

            return filtered_results

        except Exception as e:
            logger.error(f"Error in filter_stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def update_filter_config(
        self,
        config_type: int,
        params: FilterParams,
        trade_date: Optional[str] = None
    ):
        try:
            from shared.db import FilterConfig

            with get_session() as db:
                existing = db.query(FilterConfig).filter(FilterConfig.type == config_type).first()

                now = datetime.now()
                config_data = params.model_dump()
                config_data['trade_date'] = trade_date
                config_data['update_time'] = now

                if existing:
                    for key, value in config_data.items():
                        setattr(existing, key, value)
                    logger.info(f"Updated filter config for type={config_type}")
                else:
                    config_data['type'] = config_type
                    db.add(FilterConfig(**config_data))
                    logger.info(f"Created filter config for type={config_type}")
        except Exception as e:
            logger.error(f"Error updating filter config: {str(e)}")

    def _save_filter_results(self, results: List[Dict[str, Any]]):
        """
        将筛选结果保存到 FilterResult 表（type=2）

        Args:
            results: 筛选结果列表
        """
        try:
            from shared.db import FilterResult

            with get_session() as db:
                # 删除旧的 type=2 记录
                db.query(FilterResult).filter(FilterResult.type == 2).delete()

                insert_count = 0
                for stock in results:
                    code = stock.get('code', '')
                    name = stock.get('name', '')
                    interval_max_rise = stock.get('interval_max_rise', 0)
                    max_day_rise = stock.get('max_day_rise', 0)

                    symbol = to_goldminer_symbol(code)

                    filter_result = FilterResult(
                        type=2,
                        symbol=symbol,
                        code=code,
                        stock_name=name,
                        interval_max_rise=interval_max_rise,
                        max_day_rise=max_day_rise,
                        update_time=datetime.now()
                    )
                    db.add(filter_result)
                    insert_count += 1

                logger.info(f"Saved {insert_count} filter stocks to database (type=2)")
        except Exception as e:
            logger.error(f"Error saving filter results: {str(e)}")

    def _parse_block_codes(self, block_codes: str) -> List[str]:
        if block_codes:
            return [code.strip() for code in block_codes.split(',') if code.strip()]
        return []


def get_stock_filter_service() -> StockFilterService:
    return StockFilterService.get_instance()
