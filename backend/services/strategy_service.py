import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from stock_cache import get_stock_cache
from stock_filter import get_stock_filter
from stock_sqlite.database import get_db_connection, get_db_cursor
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
        trade_date: Optional[str] = None,
        weipan_exceed: int = 0,
        zaopan_exceed: int = 0,
        rising_wave: int = 0,
        block_codes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        try:
            logger.info(f"Starting strategy execution: trade_date={trade_date}, weipan={weipan_exceed}, zaopan={zaopan_exceed}, rising={rising_wave}, block_codes={block_codes}")

            target_date = self._parse_trade_date(trade_date)

            selected_block_codes = self._parse_block_codes(block_codes)

            stocks_to_filter = get_stocks_by_blocks(selected_block_codes)

            if selected_block_codes:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")

            if not stocks_to_filter:
                return {"status": "error", "msg": "未从数据库加载到股票数据"}

            stock_symbols = [to_goldminer_symbol(code) for code in stocks_to_filter]

            logger.info(f"准备筛选 {len(stock_symbols)} 只股票")

            if not stock_symbols:
                return {"status": "error", "msg": "未加载到股票数据"}

            config = {
                'recent_interval_days': 40,
                'recent_interval_max_gain': 60,
                'day_max_gain_days': 6,
                'day_max_gain': 8,
            }

            logger.info(f"Filtering {len(stock_symbols)} stocks...")

            instruments = stock_cache._load_instruments_cache()
            cache_count = len(instruments) if instruments is not None else 0
            logger.info(f"instruments 缓存加载完成，共 {cache_count} 条数据")

            results = stock_filter.filter_stocks(
                symbols=stock_symbols,
                trade_date=target_date,
                weipan_exceed=weipan_exceed,
                zaopan_exceed=zaopan_exceed,
                rising_wave=rising_wave,
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

    def _save_results_to_db(self, results: List[Dict[str, Any]]):
        save_start = datetime.now()
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("DELETE FROM filter_results WHERE type = 1")

            insert_count = 0
            for stock in results:
                # 提取字段值
                symbol = stock.get('symbol', '')
                code = symbol.split('.')[-1] if '.' in symbol else symbol
                stock_name = stock.get('stock_name', '')
                auction_start_price = stock.get('auction_start_price', 0)
                auction_end_price = stock.get('auction_end_price', 0)
                price_diff = stock.get('price_diff', 0)
                max_gain = stock.get('max_gain', 0)
                max_daily_gain = stock.get('max_daily_gain', 0)
                today_gain = stock.get('today_gain', 0)
                next_day_gain = stock.get('next_day_gain', 0)
                trade_date = stock.get('trade_date', '')
                higher_score = stock.get('higher_score', 0)
                rising_wave_score = stock.get('rising_wave_score', 0)
                weipan_exceed = stock.get('weipan_exceed', 0)
                zaopan_exceed = stock.get('zaopan_exceed', 0)
                rising_wave = stock.get('rising_wave', 0)

                cursor.execute(
                    "INSERT OR REPLACE INTO filter_results (type, symbol, code, stock_name, auction_start_price, auction_end_price, price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, trade_date, higher_score, rising_wave_score, weipan_exceed, zaopan_exceed, rising_wave, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, symbol, code, stock_name, auction_start_price, auction_end_price, price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, trade_date, higher_score, rising_wave_score, weipan_exceed, zaopan_exceed, rising_wave, current_time)
                )
                insert_count += 1

            conn.commit()
            save_end = datetime.now()
            save_duration = (save_end - save_start).total_seconds()
            logger.info(f"Results saved to database ({insert_count} records), elapsed time: {save_duration:.3f} seconds")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving results to database: {str(e)}")
        finally:
            cursor.close()
            conn.close()


_strategy_service: Optional[StrategyService] = None


def get_strategy_service() -> StrategyService:
    global _strategy_service
    if _strategy_service is None:
        _strategy_service = StrategyService()
    return _strategy_service
