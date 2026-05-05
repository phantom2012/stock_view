"""
板块股票关系工具类
提供板块与股票关系的查询接口
"""
from typing import Set, List, Optional
from shared.db import get_session_ro, BlockStock


def get_stocks_by_blocks(block_codes: Optional[List[str]] = None) -> Set[str]:
    """
    根据板块代码列表获取对应的股票列表

    Args:
        block_codes: 板块代码列表，如果为None或空列表则返回所有股票

    Returns:
        股票代码集合（纯数字格式，如: '600666'）

    Examples:
        >>> # 获取指定板块的股票
        >>> stocks = get_stocks_by_blocks(['880081', '880082'])
        >>> print(stocks)
        {'600666', '000025', ...}

        >>> # 获取所有板块的股票
        >>> all_stocks = get_stocks_by_blocks()
        >>> print(len(all_stocks))
        5000
    """
    stocks = set()
    with get_session_ro() as db:
        if block_codes and len(block_codes) > 0:
            # 过滤空字符串
            valid_codes = [code.strip() for code in block_codes if code and code.strip()]

            if valid_codes:
                # 查询指定板块下的所有股票
                rows = db.query(BlockStock).filter(
                    BlockStock.block_code.in_(valid_codes)
                ).all()
                stocks = {row.stock_code for row in rows}
        else:
            # 获取所有板块的股票
            rows = db.query(BlockStock).all()
            stocks = {row.stock_code for row in rows}

    return stocks


def get_blocks_by_stock(stock_code: str) -> List[str]:
    """
    根据股票代码获取所属的板块列表

    Args:
        stock_code: 股票代码（纯数字格式，如: '600666'）

    Returns:
        板块代码列表

    Examples:
        >>> blocks = get_blocks_by_stock('600666')
        >>> print(blocks)
        ['880081', '880082', ...]
    """
    blocks = []
    with get_session_ro() as db:
        rows = db.query(BlockStock).filter(
            BlockStock.stock_code == stock_code
        ).all()
        blocks = [row.block_code for row in rows]

    return blocks
