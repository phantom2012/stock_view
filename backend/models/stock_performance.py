from dataclasses import dataclass


@dataclass
class StockPerformance:
    """
    股票表现数据模型
    用于存储股票在指定时间段内的表现指标
    """
    interval_max_rise: float = 0.0
    max_day_rise: float = 0.0
    prev_high_price_rate: float = 0.0
    is_pass: bool = False
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'interval_max_rise': self.interval_max_rise,
            'max_day_rise': self.max_day_rise,
            'prev_high_price_rate': self.prev_high_price_rate,
            'is_pass': self.is_pass
        }
    
    @classmethod
    def from_tuple(cls, data: tuple) -> 'StockPerformance':
        """从元组创建实例 (is_pass, interval_max_rise, max_day_rise, prev_high_price_rate)"""
        return cls(
            is_pass=data[0],
            interval_max_rise=data[1],
            max_day_rise=data[2],
            prev_high_price_rate=data[3]
        )
