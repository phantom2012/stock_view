import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from models import StockDetail, FilterResult, get_session
from models.filter_params import FilterParams
from stock_filter import get_stock_filter
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol, to_pure_code
from common.singleton import SingletonMixin
from shared.trade_date_util import TradeDateUtil
from services.data_sync_notify_service import get_data_sync_notify_service

logger = logging.getLogger(__name__)

stock_filter = get_stock_filter()
trade_date_util = TradeDateUtil()


class StrategyService(SingletonMixin):
    def __init__(self):
        self._last_run_time: Optional[str] = None

    @property
    def last_run_time(self) -> Optional[str]:
        return self._last_run_time

    def run_strategy(
        self,
        params: FilterParams
    ) -> Dict[str, Any]:
        try:
            logger.info(f"Starting strategy execution: {params.model_dump()}")

            target_date = self._parse_trade_date(params.trade_date)

            selected_block_codes = self._parse_block_codes(params.select_blocks)

            stocks_to_filter = get_stocks_by_blocks(selected_block_codes)

            if selected_block_codes:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")

            if not stocks_to_filter:
                return {"status": "error", "msg": "未从数据库加载到股票数据"}

            # 主板过滤
            if params.only_main_board:
                main_board_stocks = [code for code in stocks_to_filter if stock_filter.check_is_main_board(code)]
                logger.info(f"主板过滤：从 {len(stocks_to_filter)} 只股票中筛选出 {len(main_board_stocks)} 只主板股票")
                stocks_to_filter = main_board_stocks

            stock_symbols = [to_goldminer_symbol(code) for code in stocks_to_filter]

            logger.info(f"准备筛选 {len(stock_symbols)} 只股票")

            if not stock_symbols:
                return {"status": "error", "msg": "未加载到股票数据"}

            logger.info(f"Filtering {len(stock_symbols)} stocks...")

            results = stock_filter.filter_stocks(
                symbols=stock_symbols,
                trade_date=target_date,
                params=params
            )

            logger.info(f"Strategy completed, found {len(results)} stocks")

            self._save_filter_config(params, target_date.strftime('%Y-%m-%d'))

            if results:
                self._save_results_to_db(results)

            # 通知更新数据（触发多任务同步）
            try:
                notify_service = get_data_sync_notify_service()
                # 将股票代码转换为纯数字格式，传递给同步器
                stock_codes = [stock.code for stock in results]
                notify_service.trigger_multi_sync(
                    sync_types=['minute_data', 'auction_data', 'money_flow'],
                    stock_codes=stock_codes
                )
                logger.info(f"已通知更新 minute_data, auction_data, 和 money_flow 数据，股票数量: {len(stock_codes)}")
            except Exception as e:
                logger.warning(f"通知更新数据失败: {e}")

            self._last_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return {"status": "success", "msg": f"策略运行完成，选出{len(results)}只股票", "time": self._last_run_time}
        except Exception as e:
            logger.error(f"Error running strategy: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}

    def _parse_trade_date(self, trade_date: Optional[str]) -> datetime:
        if trade_date:
            return datetime.strptime(trade_date, '%Y-%m-%d')
        else:
            latest_trade_date_str = trade_date_util.get_latest_trade_date()
            if latest_trade_date_str:
                return datetime.strptime(latest_trade_date_str, '%Y-%m-%d')
            else:
                today = datetime.now()
                yesterday = today - timedelta(days=1)
                while yesterday.weekday() >= 5:
                    yesterday -= timedelta(days=1)
                return yesterday

    def _parse_block_codes(self, block_codes: Any) -> Optional[List[str]]:
        if block_codes:
            if isinstance(block_codes, str):
                return [code.strip() for code in block_codes.split(',') if code.strip()]
            else:
                return list(block_codes)
        return None

    def _save_filter_config(self, params: FilterParams, trade_date: str):
        try:
            from shared.db import FilterConfig

            with get_session() as db:
                existing = db.query(FilterConfig).filter(FilterConfig.type == 1).first()

                now = datetime.now()
                config_data = params.model_dump()
                config_data['trade_date'] = trade_date
                config_data['update_time'] = now

                if existing:
                    for key, value in config_data.items():
                        setattr(existing, key, value)
                    logger.info(f"Updated filter config for type=1")
                else:
                    config_data['type'] = 1
                    db.add(FilterConfig(**config_data))
                    logger.info(f"Created filter config for type=1")
        except Exception as e:
            logger.error(f"Error updating filter config: {str(e)}")

    def _save_results_to_db(self, results: List[Any]):
        save_start = datetime.now()
        with get_session() as db:
            # 删除旧数据
            db.query(FilterResult).filter(FilterResult.type == 1).delete()

            insert_count = 0
            for stock in results:
                # 使用Pydantic模型自动解析数据
                if hasattr(stock, 'to_dict'):
                    stock_data = stock.to_dict()
                else:
                    stock_data = stock

                stock_data['type'] = 1
                # 使用 FilterResult.model_validate 自动过滤无效字段
                filter_result = FilterResult.model_validate(stock_data)

                # 添加到数据库会话
                db.add(filter_result)
                insert_count += 1

            save_end = datetime.now()
            save_duration = (save_end - save_start).total_seconds()
            logger.info(f"Results saved to database ({insert_count} records), elapsed time: {save_duration:.3f} seconds")


def get_strategy_service() -> StrategyService:
    return StrategyService.get_instance()
