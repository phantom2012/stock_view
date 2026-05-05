"""
数据清理同步器
从 backend/tasks/clear_data_task.py 迁移
扫描 config_clear_data_timer 表，当 clear_flag=1 时执行对应清理操作
"""
import logging
from datetime import datetime
from typing import Tuple

from shared.db import get_session, ClearDataTimer, StockInfo
from .base_syncer import BaseSyncer

logger = logging.getLogger(__name__)


class ClearDataSyncer(BaseSyncer):
    """
    数据清理同步器
    每分钟扫描一次 config_clear_data_timer 表
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        try:
            with get_session() as db:
                config = db.query(ClearDataTimer).filter(
                    ClearDataTimer.biz_type == 'stock_free_share',
                    ClearDataTimer.clear_flag == 1,
                    ClearDataTimer.enabled == 1
                ).first()

                if not config:
                    return True, 0, 0, "无清理任务"

                logger.info("开始清理 stock_info 的 free_share 和 circ_mv 字段")

                try:
                    count = db.query(StockInfo).filter(
                        (StockInfo.free_share.isnot(None)) | (StockInfo.circ_mv.isnot(None))
                    ).update(
                        {StockInfo.free_share: None, StockInfo.circ_mv: None},
                        synchronize_session='fetch'
                    )

                    config.clear_flag = 0
                    config.last_clear_time = datetime.now()
                    config.update_time = datetime.now()
                    db.commit()

                    logger.info(f"清理完成，重置 {count} 条记录")
                    return True, count, 0, f"清理{count}条"

                except Exception as e:
                    db.rollback()
                    logger.error(f"清理失败: {e}")
                    return False, 0, 1, str(e)

        except Exception as e:
            logger.error(f"扫描清理任务异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)
