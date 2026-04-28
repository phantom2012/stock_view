from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class FilterResult(Base):
    """
    筛选结果表的ORM模型
    """
    __tablename__ = 'filter_results'

    type = Column(Integer, nullable=False, primary_key=True)
    code = Column(String(10), nullable=False, primary_key=True)
    symbol = Column(String(20), nullable=False)
    stock_name = Column(String(50), nullable=False)
    pre_avg_price = Column(Float, default=0.0)
    pre_close_price = Column(Float, default=0.0)
    pre_price_gain = Column(Float, default=0.0)
    open_price = Column(Float, default=0.0)
    tail_57_price = Column(Float, default=0.0)
    close_price = Column(Float, default=0.0)
    next_close_price = Column(Float, default=0.0)
    price_diff = Column(Float, default=0.0)
    open_volume = Column(Float, default=0.0)
    open_volume_ratio = Column(Float, default=0.0)
    interval_max_rise = Column(Float, default=0.0)
    max_day_rise = Column(Float, default=0.0)
    trade_date = Column(String(10), default='')
    rising_wave_score = Column(Float, default=0.0)
    weipan_exceed = Column(Integer, default=0)
    zaopan_exceed = Column(Integer, default=0)
    rising_wave = Column(Integer, default=0)
    update_time = Column(DateTime, default=datetime.now)
