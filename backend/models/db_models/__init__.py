"""
数据库 ORM 模型统一入口
集中管理数据库连接与会话，避免重复定义
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///F:/gupiao/_sqlite_stock_data/stock.db')
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    """创建所有表结构"""
    Base.metadata.create_all(bind=engine)


def get_db():
    """获取数据库会话（用于 FastAPI 依赖注入）"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


from .filter_result import FilterResult
from .stock_auction import StockAuction
from .stock_daily import StockDaily

__all__ = [
    'Base',
    'engine',
    'SessionLocal',
    'FilterResult',
    'StockAuction',
    'StockDaily',
    'get_db',
    'create_tables'
]
