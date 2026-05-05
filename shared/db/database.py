"""
通用数据库会话管理工具
提供上下文管理器，自动处理会话的提交、回滚和关闭
"""
import logging
from contextlib import contextmanager
from typing import Generator

from .models import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

logger = logging.getLogger(__name__)

# ==================== 数据库连接配置 ====================
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///F:/gupiao/_sqlite_stock_data/stock.db')
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    """
    创建所有数据库表
    如果表已存在则跳过
    """
    Base.metadata.create_all(bind=engine)
    logger.info("数据库表创建/验证完成")


@contextmanager
def get_session() -> Generator:
    """
    获取数据库会话的上下文管理器（读写模式）
    自动处理：提交、回滚、关闭

    用法:
        with get_session() as db:
            db.add(obj)
            # 自动提交
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database session error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


@contextmanager
def get_session_ro() -> Generator:
    """
    获取只读数据库会话的上下文管理器
    只读会话不会提交，但会自动关闭

    用法:
        with get_session_ro() as db:
            rows = db.execute("SELECT ...")
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
