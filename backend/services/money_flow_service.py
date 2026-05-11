import json
import logging
from typing import Dict, Any, List, Optional

from services.data_sync_notify_service import get_data_sync_notify_service
from stock_filter.stock_money_analyzer import StockMoneyAnalyzer
from shared.db import get_session, get_session_ro, FilterResult, DataSyncNotify
from shared.trade_date_util import TradeDateUtil

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()


class MoneyFlowService:
    def __init__(self):
        self.notify_service = get_data_sync_notify_service()

    def load_money_flow_data(self, stocks: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
        """
        触发资金流向数据同步（简化版）

        流程：
        1. 调用数据同步通知服务设置 money_flow 同步标志
        2. data-sync-service 会扫描到该通知并执行同步
        3. 同步完成后 data-sync-service 会调用 backend 的完成通知 API
        4. backend 通过 SSE 向前端推送完成通知

        Args:
            stocks: 股票列表（当前版本不再使用，保留参数兼容）
            days: 天数（当前版本不再使用，保留参数兼容）

        Returns:
            Dict: 执行结果
        """
        try:
            success = self.notify_service.notify_money_flow_sync()

            if success:
                return {
                    "status": "success",
                    "msg": "资金流向数据同步已触发，请等待后台处理完成",
                    "sync_type": "money_flow"
                }
            else:
                return {
                    "status": "error",
                    "msg": "触发资金流向数据同步失败"
                }

        except Exception as e:
            logger.error(f"Error in load_money_flow_data: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}

    def run_turn_strong_calculation(self):
        """
        资金同步完成后执行转强计算
        优先使用 data_sync_notify 中 money_flow 类型指定的 stock_codes，
        未指定则回退到从 filter_results 读取全部股票
        """
        logger.info("===== 开始后台转强计算 =====")
        try:
            codes = self._get_money_flow_stock_codes()

            if not codes:
                logger.warning("无股票数据，跳过转强计算")
                return

            trade_dates = trade_date_util.get_recent_trade_dates(30)
            if not trade_dates:
                logger.warning("无交易日数据，跳过转强计算")
                return

            success_count = 0
            failed_count = 0
            for code in codes:
                try:
                    StockMoneyAnalyzer.calculate_and_update(code, trade_dates)
                    success_count += 1
                except Exception as e:
                    logger.error(f"转强计算失败 {code}: {e}")
                    failed_count += 1

            logger.info(f"===== 后台转强计算完成: 成功 {success_count}, 失败 {failed_count} =====")
        except Exception as e:
            logger.error(f"转强计算异常: {e}")
            import traceback; traceback.print_exc()

    @staticmethod
    def _get_money_flow_stock_codes() -> Optional[List[str]]:
        """
        从 data_sync_notify 表获取 money_flow 类型指定的股票列表
        未指定时回退到从 filter_results 读取全部股票
        """
        try:
            with get_session_ro() as db:
                notify = db.query(DataSyncNotify).filter(
                    DataSyncNotify.sync_type == 'money_flow'
                ).first()

                if notify and notify.stock_codes:
                    parsed = json.loads(notify.stock_codes)
                    if parsed:
                        logger.info(f"从 money_flow 通知读取到 {len(parsed)} 只股票")
                        return parsed

            logger.info("money_flow 通知未指定股票列表，回退到 filter_results")
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                codes = [row[0] for row in rows if row[0]]
                logger.info(f"从 filter_results 读取到 {len(codes)} 只股票")
                return codes
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []

    def run_recalc_turn_strong(self, codes: Optional[List[str]] = None):
        """
        重新计算转强字段

        Args:
            codes: 股票代码列表（可选），为 None 则计算所有 filter_results 中的股票
        """
        logger.info(f"===== 开始转强复算: codes={codes} =====")
        try:
            if codes is None:
                with get_session() as db:
                    rows = db.query(FilterResult.code).distinct().all()
                    codes = [row[0] for row in rows if row[0]]

            if not codes:
                logger.warning("无股票数据，跳过转强复算")
                return

            trade_dates = trade_date_util.get_recent_trade_dates(30)
            if not trade_dates:
                logger.warning("无交易日数据，跳过转强复算")
                return

            success_count = 0
            failed_count = 0
            for code in codes:
                try:
                    StockMoneyAnalyzer.calculate_and_update(code, trade_dates)
                    success_count += 1
                except Exception as e:
                    logger.error(f"转强复算失败 {code}: {e}")
                    failed_count += 1

            logger.info(f"===== 转强复算完成: 成功 {success_count}, 失败 {failed_count} =====")
        except Exception as e:
            logger.error(f"转强复算异常: {e}")
            import traceback; traceback.print_exc()


def get_money_flow_service() -> MoneyFlowService:
    return MoneyFlowService()
