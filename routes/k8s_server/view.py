from tornado.gen import coroutine

from mbutils.mb_handler import MBHandler


class K8sHandler(MBHandler):
    """
    api:/heartbeat/k8s
    k8s探测
    """

    @coroutine
    def get(self):
        self.success("OK")
