import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from stock_cache import get_stock_cache
from models import get_session, get_session_ro
from baostock_data.trade_date_util import TradeDateUtil
from common.stock_code_convert import to_goldminer_symbol, to_pure_code

logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()
trade_date_util = TradeDateUtil()


class AuctionDataService:
    def load_auction_data(self, stocks: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
        result = {
            'success': 0,
            'failed': 0,
            'total': len(stocks)
        }

        try:
            recent_trade_dates = trade_date_util.get_recent_trade_dates(days)
            if not recent_trade_dates:
                logger.error("Failed to get recent trade dates")
                return {"status": "error", "msg": "获取最近交易日失败"}

            logger.info(f"获取到 {len(recent_trade_dates)} 个交易日")

            for stock in stocks:
                code = stock.get('code', '')

                if not code:
                    result['failed'] += 1
                    continue

                symbol = to_goldminer_symbol(code)

                stock_success = True
                for date_str in recent_trade_dates:
                    try:
                        trade_date = datetime.strptime(date_str, '%Y-%m-%d')

                        auction_data = stock_cache.get_auction_data(symbol, trade_date)

                        tail_auction_data = stock_cache.get_tail_auction_data(symbol, trade_date)

                        if tail_auction_data:
                            tail_57_price = tail_auction_data.get('auction_start_price', 0)
                            close_price = tail_auction_data.get('auction_end_price', 0)
                            tail_amount = tail_auction_data.get('amount', 0)

                            pure_code = to_pure_code(symbol)
                            stock_cache._update_auction_tail_data(pure_code, date_str, tail_57_price, close_price, tail_amount)

                            logger.debug(f"更新尾盘竞价数据: {symbol} {date_str} tail_57_price={tail_57_price}, close_price={close_price}")

                    except Exception as e:
                        logger.error(f"Failed to load auction data for {symbol} on {date_str}: {e}")
                        stock_success = False

                if stock_success:
                    result['success'] += 1
                else:
                    result['failed'] += 1

            return {"status": "success", "data": result}
        except Exception as e:
            logger.error(f"Error in load_auction_data: {str(e)}")
            return {"status": "error", "msg": str(e)}

    def save_filter_stocks(self, stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            from models.db_models.filter_result import FilterResult

            with get_session() as db:
                db.query(FilterResult).filter(FilterResult.type == 2).delete()

                insert_count = 0
                for stock in stocks:
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
                return {"status": "success", "msg": f"保存成功，共{insert_count}条记录"}
        except Exception as e:
            logger.error(f"Error saving filter stocks: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}


_auction_data_service: Optional[AuctionDataService] = None


def get_auction_data_service() -> AuctionDataService:
    global _auction_data_service
    if _auction_data_service is None:
        _auction_data_service = AuctionDataService()
    return _auction_data_service
