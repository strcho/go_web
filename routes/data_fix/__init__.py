"""
定位于工具或者测试接口
"""
from .view import *

# /anfu/v2/data_fix
urls = [
    (r'/k8s', K8sHandler), # 心跳探测接口需要保留
    (r'/platform/get_log', GetLogHandler)
]
