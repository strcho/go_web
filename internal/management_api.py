from mbutils import (
    nacos,
    cfg,
)


# 配置，生产信息模块
managementNacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-management",
    timeout=3,
)


@managementNacosClient.customRequestClient(method="GET", url="/api/test1")
def apiTest1():
    pass


@managementNacosClient.customRequestClient(method="GET", url="/api/test2")
def apiTest2(id1: int, id2: int):
    pass


@managementNacosClient.customRequestClient(method="POST", url="/ebike/user/hello")
def apiTest3(formData):
    pass


@managementNacosClient.customRequestClient(method="POST", url="/ebike/user/hello", )
def apiTest4(jsonData):
    pass


@managementNacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass
