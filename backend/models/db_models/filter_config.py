from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class FilterConfig(Base):
    """
    筛选配置表的ORM模型
    """
    __tablename__ = 'filter_config'

    type = Column(Integer, primary_key=True)
    interval_days = Column(Integer, default=0)
    interval_max_rise = Column(Float, default=0.0)
    recent_days = Column(Integer, default=0)
    recent_max_day_rise = Column(Float, default=0.0)
    prev_high_price_rate = Column(Float, default=0.0)
    select_blocks = Column(String(500), default='')
    trade_date = Column(String(10), default='')
    update_time = Column(DateTime, default=datetime.now)
