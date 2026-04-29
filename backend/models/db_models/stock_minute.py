from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class StockMinute(Base):
    """
    分钟数据表的ORM模型
    """
    __tablename__ = 'stock_minute'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    eob = Column(String(20), nullable=False)
    open = Column(Float, default=0.0)
    close = Column(Float, default=0.0)
    high = Column(Float, default=0.0)
    low = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    amount = Column(Float, default=0.0)
    update_time = Column(DateTime, default=datetime.now)
