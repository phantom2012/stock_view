"""
data-sync-service 数据同步服务入口
"""
import logging
import sys
import os

# 确保 shared 包可导入
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import LOG_CONFIG
from scheduler.scheduler import DataSyncScheduler

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_CONFIG['file'], encoding='utf-8'),
    ]
)

logger = logging.getLogger(__name__)


def main():
    """服务入口"""
    logger.info("=" * 60)
    logger.info("data-sync-service 启动中...")
    logger.info("=" * 60)

    try:
        # 初始化调度器
        scheduler = DataSyncScheduler()
        scheduler.start()

        logger.info("data-sync-service 启动完成，开始运行...")

        # 保持主线程运行
        try:
            import time
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("收到停止信号，正在关闭...")
            scheduler.stop()
            logger.info("data-sync-service 已停止")

    except Exception as e:
        logger.error(f"服务启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
