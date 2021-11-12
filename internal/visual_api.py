from mbutils import nacos
from setting import ConfigNacos


def errorFun(*args):
    for item in args:
        print(item)
    return "自定义错误"


# 大屏模块
nacosClient = nacos.nacosBalanceClient(ip=ConfigNacos.nacosIp, port=ConfigNacos.nacosPort,
                                       serviceName="ebike-user",
                                       group="DEFAULT_GROUP", namespaceId="63ecef80-34dc-44e0-8c93-5e935b61f106",
                                       timeout=3,
                                       timeOutFun=errorFun, fallbackFun=errorFun)


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
def apiTest5(*args,**kwargs):
    pass