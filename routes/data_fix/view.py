import re

from tornado.gen import coroutine

from service.external.data_fix import batch_auto_pay_func, GetLogService
from service.car import CarService
from mbutils import mb_async
from mbutils.constant import ValidType
from mbutils.mb_handler import MBHandler
from setting import ConfigNacos


class BatchAutoPayHandler(MBHandler):
    """
    api:/anfu/v2/data_fix/platform/batch_auto_pay
    批量把有钱未支付的订单变成已支付
    """

    @coroutine
    def get(self):
        """
        :return:
        {
            "task_num": 6
        }
        """
        authorization = self.request.xc_basic_info
        # user = authorization['userId']
        valid_data = self.valid_data_all([
            ("startTime", ValidType.INT, {"must": True}),
            ("endTime", ValidType.INT, {"must": True})
        ])
        num = yield mb_async(batch_auto_pay_func)(valid_data)
        self.success({"update_num": num})


class PlatformCarnoManageHandler(MBHandler):
    """
    /mieba/data_fix/platform/car_no/manage
    运营平台-生产信息-车辆列表-添加车辆号段
    """

    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            ("start_no", ValidType.INT, {"must": True, "funcs": [lambda x: re.match(r"^\d{9}$", str(x))]}),
            ("end_no", ValidType.INT, {"must": True, "funcs": [lambda x: re.match(r"^\d{9}$", str(x))]}),
        ])
        start_no = valid_data[0]
        end_no = valid_data[1]
        if start_no > end_no:
            return self.error(promt="不允许起始编号大于结束编号")

        # 判断车辆编号数据库是否已存在
        is_exist = (
            yield mb_async(
                CarService().judge_carno_exists)(start_no, end_no)
        )
        if is_exist:
            return self.error(promt="车辆编号数据库已存在")

        # 插入对应的数据
        count = (
            yield mb_async(
                CarService().multi_insert_carno_record)(start_no, end_no)
        )
        return self.success({"count": count})

    @coroutine
    def delete(self):
        valid_data = self.valid_data_all([
            ("start_no", ValidType.INT, {"must": True, "funcs": [lambda x: re.match(r"^\d{9}$", str(x))]}),
            ("end_no", ValidType.INT, {"must": True, "funcs": [lambda x: re.match(r"^\d{9}$", str(x))]}),
        ])
        start_no = valid_data[0]
        end_no = valid_data[1]
        if start_no > end_no:
            return self.error(promt="不允许起始编号大于结束编号")

        # 判断车辆是否下架或者绑定设备
        message = (
            yield mb_async(
                CarService().judge_car_bind_imei_status)(start_no, end_no)
        )
        if message:
            return self.error(promt=message)

        # 删除车辆号段
        count = (
            yield mb_async(
                CarService().multi_delete_carno)(start_no, end_no)
        )
        return self.success({"count": count})


class GetLogHandler(MBHandler):
    """
        api:/anfu/v2/data_fix/platform/get_log
    """
    @coroutine
    def get(self):
        valid_data = self.valid_data_all([
            ("tp", ValidType.STR, {"default": "biz"}),
            ("is_node", ValidType.BOOL, {"default": False}),
            ("row_num", ValidType.INT, {"default": 50}),
        ])
        data = yield mb_async(GetLogService().get_log)(valid_data)
        self.success(data)
