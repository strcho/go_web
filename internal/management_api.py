from mbutils import (
    nacos,
    cfg,
)
from mbutils.app_start_init import get_nacos_config





# 配置，生产信息模块
nacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-user",
    timeout=3,
)


@nacosClient.customRequestClient(method="GET", url="/api/test1")
def apiTest1():
    pass


@nacosClient.customRequestClient(method="GET", url="/api/test2")
def apiTest2(id1: int, id2: int):
    pass


@nacosClient.customRequestClient(method="POST", url="/ebike/user/hello")
def apiTest3(formData):
    pass


@nacosClient.customRequestClient(method="POST", url="/ebike/user/hello", requestParamJson=True)
def apiTest4(jsonData):
    pass


@nacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass
