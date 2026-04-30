"""
通用单例模式基类
避免在每个 Service 中重复实现单例模式
"""
from typing import TypeVar, Generic, Type

T = TypeVar('T')


class SingletonMixin:
    """
    单例模式混入类
    使用方式：
        class MyService(SingletonMixin):
            pass
        
        # 获取单例实例
        service = MyService.get_instance()
    """
    _instance = None

    @classmethod
    def get_instance(cls: Type[T]) -> T:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """重置单例实例（主要用于测试）"""
        cls._instance = None
