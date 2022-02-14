from mbutils import (
    nacos,
)

# 支付服务
payNacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-pay",
    timeout=3,
)


@payNacosClient.customRequestClient(method="GET", url="/api/test1")
def apiTest1():
    pass


@payNacosClient.customRequestClient(method="GET", url="/api/test2")
def apiTest2(id1: int, id2: int):
    pass


@payNacosClient.customRequestClient(method="POST", url="/ebike/user/hello")
def apiTest3(formData):
    pass


@payNacosClient.customRequestClient(method="POST", url="/ebike/user/hello", )
def apiTest4(jsonData):
    pass


@payNacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass
