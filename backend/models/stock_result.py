from pydantic import BaseModel, Field
from typing import Optional


class StockResult(BaseModel):
    """
    股票结果模型
    用于自动解析字典数据并进行类型转换
    """
    symbol: str = ""
    stock_name: str = ""
    pre_avg_price: float = 0.0
    pre_close_price: float = 0.0
    pre_price_gain: float = 0.0
    open_price: float = 0.0
    close_price: float = 0.0
    next_close_price: float = 0.0
    auction_start_price: float = 0.0
    auction_end_price: float = 0.0
    price_diff: float = 0.0
    volume_ratio: float = 0.0
    interval_max_rise: float = 0.0
    max_day_rise: float = 0.0
    today_gain: float = 0.0
    next_day_gain: float = 0.0
    trade_date: str = ""
    higher_score: float = 0.0
    rising_wave_score: float = 0.0
    weipan_exceed: float = 0.0
    zaopan_exceed: float = 0.0
    rising_wave: float = 0.0
    
    @property
    def code(self) -> str:
        """从symbol中提取股票代码"""
        return self.symbol.split('.')[-1] if '.' in self.symbol else self.symbol
    
    @property
    def today_close(self) -> float:
        """获取今日收盘价"""
        return self.close_price
    
    @property
    def next_close(self) -> float:
        """获取次日收盘价"""
        return self.next_close_price
