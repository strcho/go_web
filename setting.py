from utils.ip_tools import get_host_ip


class ConfigNacos:
    ip = get_host_ip()
    port = 8080
    nacosIp = "120.24.81.243"
    nacosPort = 8848
    GlobalConfig = {}
    serverName = 'ebike_account_server'
    namespaceId = '63ecef80-34dc-44e0-8c93-5e935b61f106'  # 测试用

