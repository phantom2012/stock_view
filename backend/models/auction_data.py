from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class AuctionData:
    """
    竞价数据模型
    用于存储早盘竞价或尾盘竞价的相关数据
    """
    open_price: float = 0.0
    open_amount: float = 0.0
    open_volume: float = 0.0
    volume_ratio: float = 0.0

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'AuctionData':
        """从字典创建实例"""
        return cls(
            open_price=data.get('open_price', 0.0),
            open_amount=data.get('open_amount', 0.0),
            open_volume=data.get('open_volume', 0.0),
            volume_ratio=data.get('volume_ratio', 0.0)
        )
