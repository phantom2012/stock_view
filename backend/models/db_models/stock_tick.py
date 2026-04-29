from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class StockTick(Base):
    """
    Tick数据表的ORM模型
    """
    __tablename__ = 'stock_tick'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    created_at = Column(String(20), nullable=False)
    price = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    cum_amount = Column(Float, default=0.0)
    cum_volume = Column(Integer, default=0)
    update_time = Column(DateTime, default=datetime.now)
