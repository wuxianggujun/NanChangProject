import logging
import traceback
import functools
import polars as pl

def error_handler(func):
    """
    装饰器模式，统一处理函数执行期间的错误并记录详细日志
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"执行 {func.__name__} 时发生错误")
            logging.error(f"错误类型: {type(e).__name__}")
            logging.error(f"错误详情: {str(e)}")
            logging.error(f"堆栈跟踪: {traceback.format_exc()}")
            # 根据函数的返回类型返回适当的空值
            if func.__annotations__.get('return') == pl.DataFrame:
                return pl.DataFrame()
            elif func.__annotations__.get('return') == str:
                return ""
            elif func.__annotations__.get('return') == dict:
                return {}
            return None
    return wrapper
