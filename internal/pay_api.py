from mbutils import (
    nacos,
)

# 支付服务
nacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-pay",
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


@nacosClient.customRequestClient(method="POST", url="/ebike/user/hello", )
def apiTest4(jsonData):
    pass


@nacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass
