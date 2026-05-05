import logging
from datetime import datetime
from typing import Dict, Any, List

from models import get_session
from common.stock_code_convert import to_goldminer_symbol
from common.singleton import SingletonMixin
from services.data_sync_notify_service import get_data_sync_notify_service

logger = logging.getLogger(__name__)

notify_service = get_data_sync_notify_service()


class AuctionDataService(SingletonMixin):
    def load_auction_data(self, stocks: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
        """
        触发竞价数据同步（异步方式）
        通过通知表设置 trigger_flag，由 data-sync-service 轮询处理
        data-sync-service 会从 FilterResult 表读取股票列表自行同步

        Args:
            stocks: 股票列表（仅用于计数，不传递）
            days: 同步天数（仅用于日志，不传递）

        Returns:
            触发结果
        """
        try:
            codes = [stock.get('code', '') for stock in stocks if stock.get('code')]
            if not codes:
                return {"status": "error", "msg": "股票列表为空"}

            logger.info(f"触发竞价数据同步: {len(codes)} 只股票, {days} 天")

            # 通过通知表触发同步，data-sync-service 自行从 FilterResult 表读取股票列表
            success = notify_service.notify_auction_data_sync()
            if not success:
                return {"status": "error", "msg": "触发竞价数据同步失败"}

            return {
                "status": "success",
                "msg": f"竞价数据同步已触发，共 {len(codes)} 只股票",
                "data": {"total": len(codes)}
            }
        except Exception as e:
            logger.error(f"Error in load_auction_data: {str(e)}")
            return {"status": "error", "msg": str(e)}

    def save_filter_stocks(self, stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            from shared.db import FilterResult

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


def get_auction_data_service() -> AuctionDataService:
    return AuctionDataService.get_instance()
