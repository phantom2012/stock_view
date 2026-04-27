from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()


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


# 数据库连接和会话管理（与filter_result.py保持一致）
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///F:/gupiao/_sqlite_stock_data/stock.db')
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# 创建表结构
def create_tables():
    Base.metadata.create_all(bind=engine)


# 依赖项，用于获取数据库会话
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()