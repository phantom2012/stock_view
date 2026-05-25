"""
同步器基类
定义所有同步器的通用接口和工具方法
"""
import logging
from datetime import datetime
from typing import Tuple, Optional, List

from shared.db import get_session, get_session_ro, FilterResult, DataSyncNotify
from shared.log_utils import create_log_util

log_util = create_log_util(__name__)


class BaseSyncer:
    """
    同步器基类
    所有同步器应继承此类并实现 sync 方法
    支持通过 stock_codes 参数指定股票列表，为空则从 filter_result 表读取
    """

    sync_type: str = None

    def sync(self, stock_codes: Optional[List[str]] = None) -> Tuple[bool, int, int, str]:
        """
        执行同步操作

        Args:
            stock_codes: 指定股票代码列表（可选），为 None 时从 filter_result 表读取

        Returns:
            Tuple[bool, int, int, str]:
                - success: 是否成功
                - success_count: 成功处理的条数
                - fail_count: 失败/跳过的条数
                - result_msg: 结果描述信息
        """
        raise NotImplementedError("子类必须实现 sync 方法")

    def get_filter_stock_codes(self) -> List[str]:
        """
        从 filter_results 表获取所有股票代码

        Returns:
            股票代码列表
        """
        try:
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                return [row[0] for row in rows if row[0]]
        except Exception as e:
            log_util.limit_error(f"获取 filter_results 股票代码失败: {e}")
            return []

    def update_notify_status(self, success: bool, success_count: int,
                             fail_count: int, result_msg: str,
                             data_date: str = None):
        """
        更新 data_sync_notify 表的状态

        Args:
            success: 是否成功
            success_count: 成功条数
            fail_count: 失败条数
            result_msg: 结果消息
            data_date: 同步到的日期（可选，不传则不更新）
        """
        if self.sync_type is None:
            raise ValueError("子类必须设置 sync_type 类属性")
        try:
            with get_session() as db:
                notify = db.query(DataSyncNotify).filter(
                    DataSyncNotify.sync_type == self.sync_type
                ).first()

                if notify:
                    notify.status = 2 if success else -1
                    notify.success_count = success_count
                    notify.fail_count = fail_count
                    notify.result_msg = result_msg
                    notify.update_time = datetime.now()

                    if data_date:
                        notify.data_date = data_date

                    log_util.info(f"更新 {self.sync_type} 通知状态: status={notify.status}, data_date={notify.data_date}")
        except Exception as e:
            log_util.limit_error(f"更新 {self.sync_type} 通知状态失败: {e}")
