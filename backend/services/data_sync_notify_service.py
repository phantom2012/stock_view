"""
数据同步通知服务
用于向 data-sync-service 发送数据同步请求
支持设置股票列表和优先级，实现有序的多任务同步
"""
import logging
import json
from datetime import datetime
from typing import Optional, List

from shared.db import get_session, DataSyncNotify

logger = logging.getLogger(__name__)


class DataSyncNotifyService:
    """
    数据同步通知服务
    提供开启各类业务数据同步的通知方法
    支持设置股票列表和优先级，实现有序的多任务同步
    """

    def __init__(self):
        pass

    def notify_sync(self, sync_type: str, stock_codes: Optional[List[str]] = None) -> bool:
        """
        发送数据同步通知

        Args:
            sync_type: 同步类型（money_flow, stock_info, daily_data, auction_data, clear_data）
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

                # 设置标志位为1（需要同步）
                notify.trigger_flag = 1
                # 重置状态为0（待处理）
                notify.status = 0
                # 记录触发时间
                notify.trigger_time = datetime.now()

                # 设置股票列表（JSON格式）
                if stock_codes is not None:
                    notify.stock_codes = json.dumps(stock_codes)

                # 清理成功和失败次数
                notify.success_count = 0
                notify.fail_count = 0
                # 清空结果消息
                notify.result_msg = ""
                # 更新时间
                notify.update_time = datetime.now()

                db.commit()
                logger.info(f"已发送 {sync_type} 数据同步通知 (股票数: {len(stock_codes) if stock_codes else '全部'}, 优先级: {notify.priority})")
                return True

        except Exception as e:
            logger.error(f"发送同步通知失败: {e}")
            return False

    def trigger_multi_sync(self, sync_types: List[str], stock_codes: Optional[List[str]] = None) -> dict:
        """
        触发多个同步任务（通用方法）

        Args:
            sync_types: 同步类型列表，如 ['minute_data', 'money_flow']
            stock_codes: 股票代码列表（可选），为 None 时各同步器使用默认来源

        Returns:
            dict: 执行结果
        """
        tasks = [{'sync_type': st, 'stock_codes': stock_codes} for st in sync_types]
        success = self.submit_sync_tasks(tasks)

        if success:
            return {
                "status": "success",
                "msg": f"已触发 {len(sync_types)} 个数据同步任务，请等待后台处理完成",
                "tasks": sync_types,
                "stock_count": len(stock_codes) if stock_codes else "全部"
            }
        else:
            return {
                "status": "error",
                "msg": "提交同步任务失败"
            }

    def submit_sync_tasks(self, tasks: List[dict]) -> bool:
        """
        提交多个同步任务（按优先级有序执行）

        Args:
            tasks: 任务列表，每个任务包含:
                - sync_type: 同步类型
                - stock_codes: 股票代码列表（可选）

        Returns:
            bool: 是否全部提交成功
        """
        try:
            with get_session() as db:
                # 保存任务信息用于日志输出
                task_info_list = []

                for task in tasks:
                    sync_type = task.get('sync_type')
                    stock_codes = task.get('stock_codes')

                    notify = db.query(DataSyncNotify).filter(
                        DataSyncNotify.sync_type == sync_type
                    ).first()

                    if not notify:
                        logger.error(f"未找到同步类型: {sync_type} 的通知记录")
                        return False

                    notify.trigger_flag = 1
                    notify.status = 0
                    notify.trigger_time = datetime.now()

                    if stock_codes:
                        notify.stock_codes = json.dumps(stock_codes)

                    notify.success_count = 0
                    notify.fail_count = 0
                    notify.result_msg = ""
                    notify.update_time = datetime.now()

                    # 记录实际优先级用于排序
                    task_info_list.append({
                        'sync_type': sync_type,
                        'priority': notify.priority
                    })

                db.commit()

                # 按实际优先级排序输出日志
                sorted_tasks = sorted(task_info_list, key=lambda t: t['priority'])
                for task in sorted_tasks:
                    logger.info(f"已提交同步任务: {task['sync_type']} (优先级: {task['priority']})")

                return True

        except Exception as e:
            logger.error(f"提交同步任务失败: {e}")
            return False

    def notify_money_flow_sync(self) -> bool:
        """通知同步资金流向数据"""
        return self.notify_sync('money_flow')

    def notify_stock_info_sync(self) -> bool:
        """通知同步股票信息数据"""
        return self.notify_sync('stock_info')

    def notify_daily_data_sync(self) -> bool:
        """通知同步日线数据"""
        return self.notify_sync('daily_data')

    def notify_auction_data_sync(self) -> bool:
        """通知同步竞价数据"""
        return self.notify_sync('auction_data')

    def notify_clear_data(self) -> bool:
        """通知执行数据清理"""
        return self.notify_sync('clear_data')

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

                # 解析股票列表
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
        """获取状态描述"""
        status_map = {
            0: '待处理',
            1: '处理中',
            2: '已完成',
            -1: '失败',
        }
        return status_map.get(status, '未知')


# 单例实例
_data_sync_notify_service = None


def get_data_sync_notify_service() -> DataSyncNotifyService:
    """获取数据同步通知服务实例"""
    global _data_sync_notify_service
    if _data_sync_notify_service is None:
        _data_sync_notify_service = DataSyncNotifyService()
    return _data_sync_notify_service
