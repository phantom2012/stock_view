import logging
from typing import Dict, Any, List

from services.data_sync_notify_service import get_data_sync_notify_service

logger = logging.getLogger(__name__)


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


def get_money_flow_service() -> MoneyFlowService:
    return MoneyFlowService()
