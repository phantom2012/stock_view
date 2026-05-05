import logging

logger = logging.getLogger(__name__)


def log_progress(message: str, current: int, total: int) -> None:
    """
    按进度打印日志，每达到总数的十分之一时打印一次

    Args:
        message: 要打印的日志消息
        current: 当前处理数量（从1开始）
        total: 总处理数量
    """
    if total < 10:
        # 总数小于10时每次都打印
        logger.info(message)
    else:
        # 每达到总数的十分之一时打印，同时确保最后一条也会打印
        step = total // 10
        if current % step == 0 or current == total:
            logger.info(message)
