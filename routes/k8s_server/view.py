from marshmallow import fields
from tornado.gen import coroutine

from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler


class K8sHandler(MBHandler):
    """
    api:/ebike_account/heartbeat/k8s
    k8s探测
    """

    @coroutine
    def get(self):
        self.success("OK")


class helloHandler(MBHandler):
    """
    api:/ebike_account/internal/hello
    """

    @coroutine
    @use_args_query({
        "name": fields.String(),
        "timeout": fields.Integer(),
    })
    def post(self, args):

        self.success(f"appName=ebike-account port=80 name={args.get('name')}")


class DataBaseCheckHandler(MBHandler):
    """
    api:/ebike_account/database_check
    """

    @coroutine
    @use_args_query({
        "name": fields.String(),
    })
    def post(self, args):

        self.success()
