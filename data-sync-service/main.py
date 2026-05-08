"""
data-sync-service 数据同步服务入口
"""
import logging
import sys
import os
import signal
import time

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

# 全局调度器实例，用于信号处理
scheduler = None


def handle_signal(signum, frame):
    """
    信号处理函数

    捕获系统中断信号，确保服务能够优雅关闭

    Args:
        signum: 信号编号 (SIGINT=2, SIGTERM=15)
        frame: 当前栈帧
    """
    signal_names = {
        signal.SIGINT: 'SIGINT (Ctrl+C)',
        signal.SIGTERM: 'SIGTERM'
    }
    signal_name = signal_names.get(signum, f"信号 {signum}")

    logger.info(f"\n{'='*60}")
    logger.info(f"收到 {signal_name}，正在关闭服务...")
    logger.info('='*60)

    if scheduler:
        scheduler.stop()

    logger.info("data-sync-service 已停止")
    sys.exit(0)


def main():
    """服务入口"""
    global scheduler

    logger.info("=" * 60)
    logger.info("data-sync-service 启动中...")
    logger.info("=" * 60)

    try:
        # 注册信号处理（确保能够响应 Ctrl+C 和系统终止信号）
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        logger.info("信号处理已注册")

        # 初始化调度器
        scheduler = DataSyncScheduler()
        scheduler.start()

        logger.info("data-sync-service 启动完成，开始运行...")
        logger.info("按 Ctrl+C 可停止服务")

        # 保持主线程运行
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        # 备用处理：如果信号处理没有生效，这里作为最后的保障
        logger.info("收到 KeyboardInterrupt，正在关闭...")
        if scheduler:
            scheduler.stop()
        logger.info("data-sync-service 已停止")

    except Exception as e:
        logger.error(f"服务启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
