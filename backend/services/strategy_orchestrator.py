import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from models import StockDetail, FilterResult, StockScore, get_session
from models.filter_params import FilterParams
from stock_filter import get_stock_filter_engine
from common.block_stock_util import get_stocks_by_blocks
from common.stock_code_convert import to_goldminer_symbol, to_pure_code
from common.singleton import SingletonMixin
from shared.db import upsert_by_unique_keys
from shared.trade_date_util import TradeDateUtil
from services.data_sync_notify_service import get_data_sync_notify_service
from config import INTERVAL_RISE_SCORE_COEFFICIENT

logger = logging.getLogger(__name__)

filter_engine = get_stock_filter_engine()
trade_date_util = TradeDateUtil()


class StrategyOrchestrator(SingletonMixin):
    """
    策略编排器
    统一处理策略执行流程：参数解析、股票池获取、筛选执行、结果保存、数据同步通知
    支持 type=1（策略选股）和 type=2（板块筛选）两种模式
    """

    def __init__(self):
        self._last_run_time: Optional[str] = None

    @property
    def last_run_time(self) -> Optional[str]:
        return self._last_run_time

    def filter_type1_stocks(self, params: FilterParams) -> Dict[str, Any]:
        """
        执行策略选股（type=1）
        对应前端"刷新超预期列表"功能

        Args:
            params: 筛选参数

        Returns:
            {"status": "success"/"error", "msg": "...", "time": "..."}
        """
        try:
            logger.info(f"Starting type=1 strategy execution: {params.model_dump()}")

            target_date = self._parse_trade_date(params.trade_date)
            results = self._execute_filter_flow(params, target_date, config_type=1)

            self._last_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return {
                "status": "success",
                "msg": f"策略运行完成，选出{len(results)}只股票",
                "time": self._last_run_time
            }
        except Exception as e:
            logger.error(f"Error in filter_type1_stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}

    def filter_type2_stocks(self, params: FilterParams) -> List[Dict[str, Any]]:
        """
        执行板块筛选（type=2）
        对应前端"板块筛选"功能

        Args:
            params: 筛选参数

        Returns:
            筛选结果字典列表
        """
        try:
            # 获取最新交易日
            latest_trade_date_str = trade_date_util.get_latest_trade_date()
            if not latest_trade_date_str:
                logger.error("无法获取最新交易日，无法执行筛选")
                return []
            trade_date = datetime.strptime(latest_trade_date_str, '%Y-%m-%d')

            results = self._execute_filter_flow(params, trade_date, config_type=2)

            # 直接返回 StockDetail 的原始字典（前端统一使用 stock_name 字段）
            return [stock.model_dump() for stock in results]

        except Exception as e:
            logger.error(f"Error in filter_type2_stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    # ==================== 公共筛选流程 ====================

    def _execute_filter_flow(self, params: FilterParams, trade_date: datetime,
                             config_type: int) -> List[StockDetail]:
        """
        公共筛选流程：股票池获取 → 主板过滤 → 执行筛选 → 保存配置 → 保存结果 → 触发同步

        Args:
            params: 筛选参数
            trade_date: 交易日期
            config_type: 配置类型（1=策略选股, 2=板块筛选）

        Returns:
            筛选结果列表（StockDetail对象列表）

        Raises:
            ValueError: 股票池为空时抛出
        """
        # 1. 获取股票池
        selected_block_codes = self._parse_block_codes(params.select_blocks)
        stocks_to_filter = get_stocks_by_blocks(selected_block_codes)

        if selected_block_codes:
            logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
        else:
            logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")

        if not stocks_to_filter:
            raise ValueError("未从数据库加载到股票数据")

        # 2. 主板过滤
        if params.only_main_board:
            main_board_stocks = self._filter_main_board(stocks_to_filter)
            logger.info(f"主板过滤：从 {len(stocks_to_filter)} 只股票中筛选出 {len(main_board_stocks)} 只主板股票")
            stocks_to_filter = main_board_stocks

        # 3. 转换为掘金格式
        stock_symbols = [to_goldminer_symbol(code) for code in stocks_to_filter]
        logger.info(f"准备筛选 {len(stock_symbols)} 只股票")

        if not stock_symbols:
            raise ValueError("未加载到股票数据")

        # 4. 执行批量筛选
        results = filter_engine.filter_stocks(
            symbols=stock_symbols,
            trade_date=trade_date,
            params=params
        )
        logger.info(f"筛选完成，共 {len(results)} 只股票")

        # 5. 保存筛选配置
        self._save_filter_config(params, trade_date.strftime('%Y-%m-%d'), config_type=config_type)

        # 6. 保存结果到数据库
        if results:
            self._save_results_to_db(results, config_type=config_type)

        # 7. 触发数据同步
        self._trigger_sync(results, config_type=config_type)

        return results

    # ==================== 私有辅助方法 ====================

    def _filter_main_board(self, stock_codes: set) -> set:
        """过滤出主板股票"""
        return {
            code for code in stock_codes
            if filter_engine.analyzer.check_is_main_board(to_goldminer_symbol(code))
        }

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

    def _save_filter_config(self, params: FilterParams, trade_date: str, config_type: int):
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

    def _save_results_to_db(self, results: List[StockDetail], config_type: int):
        save_start = datetime.now()
        with get_session() as db:
            db.query(FilterResult).filter(FilterResult.type == config_type).delete()

            insert_count = 0
            for stock in results:
                if hasattr(stock, 'to_dict'):
                    stock_data = stock.to_dict()
                else:
                    stock_data = stock

                stock_data['type'] = config_type
                filter_result = FilterResult.model_validate(stock_data)
                db.add(filter_result)
                insert_count += 1

                update_data = {'update_time': datetime.now()}
                if stock.rising_wave_score > 0:
                    update_data['rising_wave_score'] = stock.rising_wave_score
                if stock.interval_max_rise != 0:
                    interval_rise_score = round(abs(stock.interval_max_rise) * INTERVAL_RISE_SCORE_COEFFICIENT, 2)
                    update_data['interval_rise_score'] = interval_rise_score

                if len(update_data) > 1:
                    upsert_by_unique_keys(
                        db,
                        StockScore,
                        unique_keys={'code': stock.code, 'trade_date': stock.trade_date},
                        update_data=update_data
                    )

            db.commit()
            save_end = datetime.now()
            save_duration = (save_end - save_start).total_seconds()
            logger.info(f"Results saved to database ({insert_count} records, type={config_type}), elapsed time: {save_duration:.3f} seconds")

    def _trigger_sync(self, results: List[StockDetail], config_type: int):
        if not results:
            return

        try:
            notify_service = get_data_sync_notify_service()
            stock_codes = [stock.code for stock in results]

            if config_type == 1:
                notify_service.notify_minute_data_sync(stock_codes)
                notify_service.notify_auction_data_sync(stock_codes)
                notify_service.notify_money_flow_sync(stock_codes)
                logger.info(f"已通知更新 minute_data, auction_data, money_flow 数据，股票数量: {len(stock_codes)}")
            else:
                notify_service.notify_daily_data_sync()
                notify_service.notify_minute_data_sync(stock_codes)
                logger.info(f"已通知更新 daily_data, minute_data 数据，股票数量: {len(stock_codes)}")
        except Exception as e:
            logger.warning(f"通知更新数据失败: {e}")

def get_strategy_orchestrator() -> StrategyOrchestrator:
    return StrategyOrchestrator.get_instance()
