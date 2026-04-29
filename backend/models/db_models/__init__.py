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


from .filter_result import FilterResult
from .stock_auction import StockAuction
from .stock_daily import StockDaily
from .stock_money_flow import StockMoneyFlow
from .block_info import BlockInfo
from .block_stock import BlockStock
from .filter_config import FilterConfig
from .stock_info import StockInfo
from .stock_minute import StockMinute
from .stock_tick import StockTick
from .db_session import get_session, get_session_ro

__all__ = [
    'Base',
    'engine',
    'SessionLocal',
    'FilterResult',
    'StockAuction',
    'StockDaily',
    'StockMoneyFlow',
    'BlockInfo',
    'BlockStock',
    'FilterConfig',
    'StockInfo',
    'StockMinute',
    'StockTick',
    'get_session',
    'get_session_ro',
    'create_tables'
]
