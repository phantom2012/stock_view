from dataclasses import dataclass
from typing import Optional


@dataclass
class AuctionData:
    """
    竞价数据模型
    用于存储早盘竞价或尾盘竞价的相关数据
    """
    auction_start_price: float = 0.0
    auction_end_price: float = 0.0
    auction_amount: float = 0.0
    open_amount: float = 0.0
    volume_ratio: float = 0.0
    
    @property
    def price_diff(self) -> float:
        """计算竞价结束价与开始价的差值"""
        return round(self.auction_end_price - self.auction_start_price, 2)
    
    @property
    def price_gain_percent(self) -> float:
        """计算竞价涨幅百分比"""
        if self.auction_start_price != 0:
            return round((self.auction_end_price - self.auction_start_price) / self.auction_start_price * 100, 2)
        return 0.0
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'auction_start_price': self.auction_start_price,
            'auction_end_price': self.auction_end_price,
            'price_diff': self.price_diff,
            'auction_amount': self.auction_amount,
            'open_amount': self.open_amount,
            'volume_ratio': self.volume_ratio
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AuctionData':
        """从字典创建实例"""
        return cls(
            auction_start_price=data.get('auction_start_price', 0.0),
            auction_end_price=data.get('auction_end_price', 0.0),
            auction_amount=data.get('auction_amount', 0.0),
            open_amount=data.get('open_amount', 0.0),
            volume_ratio=data.get('volume_ratio', 0.0)
        )
