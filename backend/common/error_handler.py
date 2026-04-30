"""
统一错误处理装饰器
避免在每个 Service 方法中重复 try-except 和日志记录
"""
import logging
import functools
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def handle_errors(
    default_return: Any = None,
    log_error: bool = True,
    raise_exception: bool = False
) -> Callable:
    """
    统一错误处理装饰器
    
    Args:
        default_return: 发生错误时的默认返回值
        log_error: 是否记录错误日志
        raise_exception: 是否重新抛出异常
        
    Returns:
        装饰后的函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                
                if raise_exception:
                    raise
                
                return default_return
        
        return wrapper
    return decorator


def handle_api_errors(
    default_return: Optional[dict] = None
) -> Callable:
    """
    API 接口错误处理装饰器
    返回标准格式的错误响应
    
    Args:
        default_return: 发生错误时的默认返回字典
        
    Returns:
        装饰后的函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> dict:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"API error in {func.__name__}: {str(e)}")
                import traceback
                traceback.print_exc()
                
                return default_return or {"status": "error", "msg": str(e)}
        
        return wrapper
    return decorator
