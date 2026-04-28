from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class StockAuction(Base):
    """
    竞价数据表的ORM模型
    """
    __tablename__ = 'stock_auction'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    open_price = Column(Float, default=0.0)
    open_amount = Column(Float, default=0.0)
    open_volume = Column(Integer, default=0)
    pre_close = Column(Float, default=0.0)
    turn_over_rate = Column(Float, default=0.0)
    volume_ratio = Column(Float, default=0.0)
    float_share = Column(Float, default=0.0)
    tail_57_price = Column(Float, default=0.0)
    tail_amount = Column(Float, default=0.0)
    tail_volume = Column(Integer, default=0)
    close_price = Column(Float, default=0.0)
    avg_5d_price = Column(Float, default=0.0)
    avg_10d_price = Column(Float, default=0.0)
    update_time = Column(DateTime, default=datetime.now)
