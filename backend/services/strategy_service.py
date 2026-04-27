import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from models import StockResult, FilterResult, get_db
from models.filter_params import FilterParams
from stock_cache import get_stock_cache
from stock_filter import get_stock_filter
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol, to_pure_code
from baostock_data.trade_date_util import TradeDateUtil

logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()
stock_filter = get_stock_filter()
trade_date_util = TradeDateUtil()


class StrategyService:
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

            config = {
                'interval_days': params.interval_days,
                'interval_max_rise': params.interval_max_rise,
                'recent_days': params.recent_days,
                'recent_max_day_rise': params.recent_max_day_rise,
                'prev_high_price_rate': params.prev_high_price_rate,
            }

            logger.info(f"Filtering {len(stock_symbols)} stocks...")

            instruments = stock_cache._load_instruments_cache()
            cache_count = len(instruments) if instruments is not None else 0
            logger.info(f"instruments 缓存加载完成，共 {cache_count} 条数据")

            results = stock_filter.filter_stocks(
                symbols=stock_symbols,
                trade_date=target_date,
                weipan_exceed=params.weipan_exceed,
                zaopan_exceed=params.zaopan_exceed,
                rising_wave=params.rising_wave,
                config=config
            )

            logger.info(f"Strategy completed, found {len(results)} stocks")

            if results:
                self._save_results_to_db(results)

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

    def _save_results_to_db(self, results: List[Any]):
        save_start = datetime.now()
        db = next(get_db())
        try:
            # 删除旧数据
            db.query(FilterResult).filter(FilterResult.type == 1).delete()

            insert_count = 0
            for stock in results:
                # 使用Pydantic模型自动解析数据
                if hasattr(stock, 'to_dict'):
                    # 如果是StockFilterResult对象，先转换为字典
                    stock_data = stock.to_dict()
                else:
                    # 直接使用字典
                    stock_data = stock
                
                # 使用Pydantic模型自动解析字典，处理类型转换和默认值
                stock_obj = StockResult.parse_obj(stock_data)
                
                # 创建FilterResult ORM对象
                filter_result = FilterResult(
                    type=1,
                    symbol=stock_obj.symbol,
                    code=stock_obj.code,
                    stock_name=stock_obj.stock_name,
                    pre_avg_price=stock_obj.pre_avg_price,
                    pre_close_price=stock_obj.pre_close_price,
                    pre_price_gain=stock_obj.pre_price_gain,
                    open_price=stock_obj.open_price,
                    close_price=stock_obj.close_price,
                    next_close_price=stock_obj.next_close_price,
                    auction_start_price=stock_obj.auction_start_price,
                    auction_end_price=stock_obj.auction_end_price,
                    price_diff=stock_obj.price_diff,
                    volume_ratio=stock_obj.volume_ratio,
                    interval_max_rise=stock_obj.interval_max_rise,
                    max_day_rise=stock_obj.max_day_rise,
                    trade_date=stock_obj.trade_date,
                    higher_score=stock_obj.higher_score,
                    rising_wave_score=stock_obj.rising_wave_score,
                    weipan_exceed=stock_obj.weipan_exceed,
                    zaopan_exceed=stock_obj.zaopan_exceed,
                    rising_wave=stock_obj.rising_wave,
                    update_time=datetime.now()
                )
                
                # 添加到数据库会话
                db.add(filter_result)
                insert_count += 1

            # 提交事务
            db.commit()
            save_end = datetime.now()
            save_duration = (save_end - save_start).total_seconds()
            logger.info(f"Results saved to database ({insert_count} records), elapsed time: {save_duration:.3f} seconds")
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving results to database: {str(e)}")
        finally:
            db.close()


_strategy_service: Optional[StrategyService] = None


def get_strategy_service() -> StrategyService:
    global _strategy_service
    if _strategy_service is None:
        _strategy_service = StrategyService()
    return _strategy_service
