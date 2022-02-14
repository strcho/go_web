from tornado.gen import coroutine

from service.external.data_fix import GetLogService
from mbutils import mb_async
from mbutils.constant import ValidType
from mbutils.mb_handler import MBHandler


class GetLogHandler(MBHandler):
    """
        api:/account/platform/get_log
    """
    @coroutine
    def get(self):
        valid_data = self.valid_data_all([
            ("tp", ValidType.STR, {"default": "biz"}),
            ("row_num", ValidType.INT, {"default": 50}),
        ])
        data = yield mb_async(GetLogService().get_log)(valid_data)
        self.success(data)
