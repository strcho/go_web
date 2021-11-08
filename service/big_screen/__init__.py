from model.all_model import *
from mbutils import dao_session
from service import MBService
from utils.constant.redis_key import *


class ScreenService(MBService):

    def __init__(self, op_area_ids: tuple):
        self.op_area_ids = op_area_ids
        self.today_zero, _ = MBService.get_today_date()
        self.pay_yesterday = (0, 0)

    # 涉及到金额相关的需要对结果除100，数据库保存的为int，单位为分
    def redis_to_time(self, key_name):
        data_time = dao_session.redis_session.r.hget(SCRIPT_EXECUTE_TIME, key=key_name)
        if data_time:
            return datetime.fromtimestamp(int(data_time))
        else:
            return self.today_zero

    @staticmethod
    def acquire_all_service_id():
        service_ids = dao_session.sub_session().query(func.distinct(XcEbikeGfence2.serviceId)). \
            filter(XcEbikeGfence2.serviceId > 0).all()
        service_id_list = [s[0] for s in service_ids if s]
        return tuple(service_id_list)
