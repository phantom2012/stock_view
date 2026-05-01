from sqlalchemy import Column, String, Float, DateTime
from datetime import datetime

from . import Base


class StockInfo(Base):
    """
    股票基本信息表的ORM模型
    """
    __tablename__ = 'stock_info'

    code        = Column(String(10), primary_key=True)
    name        = Column(String(50), nullable=False)
    exchange    = Column(String(10), default='')
    free_share  = Column(Float, default=0.0)
    circ_mv     = Column(Float, default=0.0)
    update_time = Column(DateTime, default=datetime.now)
