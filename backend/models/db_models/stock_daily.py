from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()


class StockDaily(Base):
    """
    日线数据表的ORM模型
    """
    __tablename__ = 'stock_daily'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    open = Column(Float, default=0.0)
    close = Column(Float, default=0.0)
    high = Column(Float, default=0.0)
    low = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    amount = Column(Float, default=0.0)
    pre_close = Column(Float, default=0.0)
    eob = Column(String(20), default='')
    update_time = Column(DateTime, default=datetime.now)


# 数据库连接和会话管理（与其他模型保持一致）
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