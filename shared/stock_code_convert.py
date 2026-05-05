"""
股票代码转换工具类
统一管理不同接口的股票代码格式
- 纯数字code: 600666 (数据库存储用)
- 掘金格式: SHSE.600666 (掘金API用)
- Tushare格式: 600666.SH (Tushare API用)
"""

from typing import Optional


class StockCodeConverter:
    """
    股票代码转换器
    用于在不同格式的股票代码之间进行转换
    """

    @staticmethod
    def to_goldminer_symbol(code: str) -> str:
        """
        纯数字code转为掘金格式symbol

        Args:
            code: 纯数字股票代码，如 "600666"

        Returns:
            掘金格式symbol，如 "SHSE.600666"
        """
        if not code:
            return ""

        code = code.strip()

        # 如果已经包含点号，说明已经是完整格式，直接返回
        if '.' in code:
            return code

        # 判断交易所
        if code.startswith('6'):
            return f"SHSE.{code}"
        elif code.startswith(('0', '3')):
            return f"SZSE.{code}"
        else:
            return f"UNKNOWN.{code}"

    @staticmethod
    def to_tushare_ts_code(code: str) -> str:
        """
        纯数字code转为Tushare格式ts_code

        Args:
            code: 纯数字股票代码，如 "600666"

        Returns:
            Tushare格式ts_code，如 "600666.SH"
        """
        if not code:
            return ""

        code = code.strip()

        # 如果已经包含点号，说明已经是Tushare格式
        if '.' in code:
            return code

        # 判断交易所
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith(('0', '3')):
            return f"{code}.SZ"
        else:
            return f"{code}.UNKNOWN"

    @staticmethod
    def to_pure_code(symbol: str) -> str:
        """
        从任意格式提取纯数字code

        Args:
            symbol: 任意格式的股票代码
            - 掘金格式: SHSE.600666, SZSE.000001
            - Tushare格式: 600666.SH, 000001.SZ
            - 纯数字: 600666, 000001

        Returns:
            纯数字code，如 "600666"
        """
        if not symbol:
            return ""

        symbol = symbol.strip()

        # 如果不包含点号，说明已经是纯数字
        if '.' not in symbol:
            return symbol

        # 提取纯数字部分
        parts = symbol.split('.')
        if len(parts) == 2:
            # 可能格式: SHSE.600666 或 600666.SH
            for part in parts:
                # 如果这部分全是数字
                if part.isdigit():
                    return part
                # 如果是4位年份格式的时间字符串，不处理
                if len(part) == 4 and part.isdigit():
                    continue

        return symbol

    @staticmethod
    def is_valid_code(code: str) -> bool:
        """
        检查是否是有效的股票代码

        Args:
            code: 股票代码

        Returns:
            是否有效
        """
        if not code:
            return False

        code = code.strip()

        # 纯数字格式
        if code.isdigit():
            return len(code) == 6

        # 点号分隔格式
        if '.' in code:
            pure_code = StockCodeConverter.to_pure_code(code)
            return pure_code.isdigit() and len(pure_code) == 6

        return False

    @staticmethod
    def get_exchange(code: str) -> str:
        """
        获取股票交易所代码

        Args:
            code: 纯数字股票代码或任意格式代码

        Returns:
            交易所代码: SHSE, SZSE 或 UNKNOWN
        """
        if not code:
            return "UNKNOWN"

        pure_code = StockCodeConverter.to_pure_code(code)

        if pure_code.startswith('6'):
            return "SHSE"
        elif pure_code.startswith(('0', '3')):
            return "SZSE"
        else:
            return "UNKNOWN"


# 全局实例
_converter = StockCodeConverter()


def to_goldminer_symbol(code: str) -> str:
    """纯数字code转为掘金格式symbol"""
    return _converter.to_goldminer_symbol(code)


def to_tushare_ts_code(code: str) -> str:
    """纯数字code转为Tushare格式ts_code"""
    return _converter.to_tushare_ts_code(code)


def to_pure_code(symbol: str) -> str:
    """从任意格式提取纯数字code"""
    return _converter.to_pure_code(symbol)


def is_valid_code(code: str) -> bool:
    """检查是否是有效的股票代码"""
    return _converter.is_valid_code(code)


def get_exchange(code: str) -> str:
    """获取股票交易所代码"""
    return _converter.get_exchange(code)
