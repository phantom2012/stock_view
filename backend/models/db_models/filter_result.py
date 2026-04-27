from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()


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
    close_price = Column(Float, default=0.0)
    next_close_price = Column(Float, default=0.0)
    auction_start_price = Column(Float, default=0.0)
    auction_end_price = Column(Float, default=0.0)
    price_diff = Column(Float, default=0.0)
    volume_ratio = Column(Float, default=0.0)
    interval_max_rise = Column(Float, default=0.0)
    max_day_rise = Column(Float, default=0.0)
    trade_date = Column(String(10), default='')
    higher_score = Column(Float, default=0.0)
    rising_wave_score = Column(Integer, default=0)
    weipan_exceed = Column(Integer, default=0)
    zaopan_exceed = Column(Integer, default=0)
    rising_wave = Column(Integer, default=0)
    update_time = Column(DateTime, default=datetime.now)


DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///F:/gupiao/_sqlite_stock_data/stock.db')
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()