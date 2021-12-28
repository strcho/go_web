from mbutils import (
    nacos,
)

# 用户服务
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


@nacosClient.customRequestClient(method="POST", url="/user/detail",)
def internal_get_userinfo_by_id(jsonData):
    pass


@nacosClient.customRequestClient(method="GET", url="/api/test5")
def apiTest5(*args, **kwargs):
    pass


@nacosClient.customRequestClient(method="POST", url="/userState/depositedStateChange",)
def internal_deposited_state_change(jsonData):
    pass


@nacosClient.customRequestClient(method="POST", url="/userState/depositCardStateChange",)
def internal_deposited_card_state_change(jsonData):
    pass