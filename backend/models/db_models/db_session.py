"""
通用数据库会话管理工具
提供上下文管理器，自动处理会话的提交、回滚和关闭
"""
from contextlib import contextmanager
from typing import Generator, Callable, Any
from sqlalchemy import text

from . import SessionLocal


@contextmanager
def get_session() -> Generator:
    """
    获取数据库会话的上下文管理器
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
    except Exception:
        db.rollback()
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
