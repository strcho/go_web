from routes.k8s_server.view import K8sHandler


# /account
urls = [
    (r'/heartbeat/k8s', K8sHandler),  # 心跳探测接口需要保留
]