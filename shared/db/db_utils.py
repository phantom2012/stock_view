"""
通用数据库操作工具
提供常用的数据库操作方法，减少重复代码
"""
from typing import Type, TypeVar, Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_

T = TypeVar('T')


def upsert_by_unique_keys(
    db: Session,
    model: Type[T],
    unique_keys: Dict[str, Any],
    update_data: Dict[str, Any]
) -> T:
    """
    根据唯一键查找记录，存在则更新，不存在则插入

    Args:
        db: 数据库会话
        model: ORM 模型类
        unique_keys: 唯一键字典，用于查找记录
        update_data: 要更新的字段数据

    Returns:
        更新或插入的记录对象
    """
    filters = [getattr(model, key) == value for key, value in unique_keys.items()]
    existing = db.query(model).filter(and_(*filters)).first()

    if existing:
        for key, value in update_data.items():
            setattr(existing, key, value)
        return existing
    else:
        record = model(**unique_keys, **update_data)
        db.add(record)
        return record


def batch_upsert_by_unique_keys(
    db: Session,
    model: Type[T],
    unique_key_names: List[str],
    records_data: List[Dict[str, Any]]
) -> int:
    """
    批量 upsert 操作

    Args:
        db: 数据库会话
        model: ORM 模型类
        unique_key_names: 唯一键字段名列表
        records_data: 记录数据列表

    Returns:
        处理的记录数量
    """
    count = 0
    for data in records_data:
        unique_keys = {key: data[key] for key in unique_key_names if key in data}
        update_data = {key: value for key, value in data.items() if key not in unique_key_names}

        upsert_by_unique_keys(db, model, unique_keys, update_data)
        count += 1

    return count


def delete_by_filter(db: Session, model: Type[T], **filters) -> int:
    """
    根据条件删除记录

    Args:
        db: 数据库会话
        model: ORM 模型类
        **filters: 过滤条件

    Returns:
        删除的记录数量
    """
    query = db.query(model)
    for key, value in filters.items():
        query = query.filter(getattr(model, key) == value)

    count = query.delete()
    return count


def get_or_create(
    db: Session,
    model: Type[T],
    unique_keys: Dict[str, Any],
    defaults: Optional[Dict[str, Any]] = None
) -> tuple:
    """
    获取或创建记录

    Args:
        db: 数据库会话
        model: ORM 模型类
        unique_keys: 唯一键字典
        defaults: 创建时的默认值

    Returns:
        (记录对象, 是否为新创建)
    """
    filters = [getattr(model, key) == value for key, value in unique_keys.items()]
    existing = db.query(model).filter(and_(*filters)).first()

    if existing:
        return existing, False
    else:
        data = {**unique_keys, **(defaults or {})}
        record = model(**data)
        db.add(record)
        return record, True
