"""
数据同步通知服务
用于向 data-sync-service 发送单个数据同步通知
支持设置股票列表，由调度器按优先级有序处理
"""
import logging
import json
from datetime import datetime
from typing import Optional, List

from shared.db import get_session, DataSyncNotify

logger = logging.getLogger(__name__)


class DataSyncNotifyService:
    """发送单个数据同步任务通知"""

    def __init__(self):
        pass

    def notify_sync(self, sync_type: str, stock_codes: Optional[List[str]] = None) -> bool:
        """
        发送数据同步通知

        Args:
            sync_type: 同步类型（money_flow, stock_info, daily_data, auction_data, minute_data, clear_data）
            stock_codes: 指定股票代码列表（可选），为空则从 filter_result 表读取

        Returns:
            bool: 是否通知成功
        """
        try:
            with get_session() as db:
                notify = db.query(DataSyncNotify).filter(
                    DataSyncNotify.sync_type == sync_type
                ).first()

                if not notify:
                    logger.error(f"未找到同步类型: {sync_type} 的通知记录")
                    return False

                notify.trigger_flag = 1
                notify.status = 0
                notify.trigger_time = datetime.now()

                if stock_codes is not None:
                    notify.stock_codes = json.dumps(stock_codes)
                else:
                    notify.stock_codes = ""

                notify.success_count = 0
                notify.fail_count = 0
                notify.result_msg = ""
                notify.update_time = datetime.now()

                db.commit()
                logger.info(f"已发送 {sync_type} 数据同步通知 (股票数: {len(stock_codes) if stock_codes else '全部'}, 优先级: {notify.priority})")
                return True

        except Exception as e:
            logger.error(f"发送同步通知失败: {e}")
            return False

    def notify_money_flow_sync(self, stock_codes: Optional[List[str]] = None) -> bool:
        """通知同步资金流向数据"""
        return self.notify_sync('money_flow', stock_codes)

    def notify_stock_info_sync(self) -> bool:
        """通知同步股票信息数据"""
        return self.notify_sync('stock_info')

    def notify_daily_data_sync(self) -> bool:
        """通知同步日线数据"""
        return self.notify_sync('daily_data')

    def notify_auction_data_sync(self, stock_codes: Optional[List[str]] = None) -> bool:
        """通知同步竞价数据"""
        return self.notify_sync('auction_data', stock_codes)

    def notify_minute_data_sync(self, stock_codes: Optional[List[str]] = None) -> bool:
        """通知同步分钟数据"""
        return self.notify_sync('minute_data', stock_codes)

    def notify_clear_data(self) -> bool:
        """通知执行数据清理"""
        return self.notify_sync('clear_data')

    def notify_financial_data_sync(self, stock_codes: Optional[List[str]] = None) -> bool:
        """通知同步财务指标数据"""
        return self.notify_sync('financial_data', stock_codes)

    def notify_industry_valuation_sync(self) -> bool:
        """通知同步行业估值基准数据"""
        return self.notify_sync('industry_valuation')

    def get_sync_status(self, sync_type: str) -> Optional[dict]:
        """
        获取指定同步类型的状态

        Args:
            sync_type: 同步类型

        Returns:
            dict: 状态信息，如果不存在返回 None
        """
        try:
            with get_session() as db:
                notify = db.query(DataSyncNotify).filter(
                    DataSyncNotify.sync_type == sync_type
                ).first()

                if not notify:
                    return None

                stock_codes = None
                if notify.stock_codes:
                    try:
                        stock_codes = json.loads(notify.stock_codes)
                    except json.JSONDecodeError:
                        logger.warning(f"{sync_type} 的 stock_codes 格式无效")

                return {
                    'sync_type': notify.sync_type,
                    'priority': notify.priority,
                    'trigger_flag': notify.trigger_flag,
                    'status': notify.status,
                    'status_desc': self._get_status_desc(notify.status),
                    'result_msg': notify.result_msg,
                    'success_count': notify.success_count,
                    'fail_count': notify.fail_count,
                    'stock_codes': stock_codes,
                    'trigger_time': notify.trigger_time.isoformat() if notify.trigger_time else None,
                    'update_time': notify.update_time.isoformat() if notify.update_time else None,
                }
        except Exception as e:
            logger.error(f"获取同步状态失败: {e}")
            return None

    def _get_status_desc(self, status: int) -> str:
        status_map = {
            0: '待处理',
            1: '处理中',
            2: '已完成',
            -1: '失败',
        }
        return status_map.get(status, '未知')


_data_sync_notify_service = None


def get_data_sync_notify_service() -> DataSyncNotifyService:
    global _data_sync_notify_service
    if _data_sync_notify_service is None:
        _data_sync_notify_service = DataSyncNotifyService()
    return _data_sync_notify_service
