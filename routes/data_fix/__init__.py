"""
定位于工具或者测试接口
"""
from .view import *

# /account/data_fix
urls = [
    (r'/platform/get_log', GetLogHandler)
]
