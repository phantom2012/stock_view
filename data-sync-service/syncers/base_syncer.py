"""
同步器基类
定义所有同步器的通用接口和工具方法
"""
import logging
from typing import Tuple, Optional, List

logger = logging.getLogger(__name__)


class BaseSyncer:
    """
    同步器基类
    所有同步器应继承此类并实现 sync 方法
    支持通过 stock_codes 参数指定股票列表，为空则从 filter_result 表读取
    """

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
