"""
外部数据接口层
封装所有外部数据源的调用，提供统一的查询接口
"""
from .query_handler import ExternalDataQueryHandler, get_query_handler

__all__ = ['ExternalDataQueryHandler', 'get_query_handler']
