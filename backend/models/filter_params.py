from pydantic import BaseModel
from typing import Optional


class FilterParams(BaseModel):
    """
    筛选参数模型
    """
    trade_date: Optional[str] = None
    weipan_exceed: int = 0
    zaopan_exceed: int = 0
    rising_wave: int = 0
    select_blocks: Optional[str] = None
    interval_days: int = 50
    interval_max_rise: float = 30.0
    recent_days: int = 5
    recent_max_day_rise: float = 7.0
    prev_high_price_rate: float = 80.0
    only_main_board: bool = False
    
    @property
    def block_codes(self):
        """获取block_codes参数，兼容字符串和列表形式"""
        return self.select_blocks
