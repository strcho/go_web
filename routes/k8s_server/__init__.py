from routes.k8s_server.view import (
    K8sHandler,
    helloHandler,
)

# /account
urls = [
    (r'/heartbeat/k8s', K8sHandler),  # 心跳探测接口需要保留
    (r'/internal/hello', helloHandler)
]
