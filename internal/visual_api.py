from mbutils import (
    nacos,
)

# 大屏服务
visualNacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-visual",
    timeout=3,
)


@visualNacosClient.customRequestClient(method="GET", url="/api/test1")
def apiTest1():
    pass


@visualNacosClient.customRequestClient(method="GET", url="/api/test2")
def apiTest2(id1: int, id2: int):
    pass


@visualNacosClient.customRequestClient(method="POST", url="/ebike/user/hello")
def apiTest3(formData):
    pass


@visualNacosClient.customRequestClient(method="POST", url="/ebike/user/hello", )
def apiTest4(jsonData):
    pass


@visualNacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass
