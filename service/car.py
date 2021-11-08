from datetime import datetime

from sqlalchemy import func

from mbutils import dao_session, logger
from . import MBService


class CarService(MBService):
    """ 车辆的service层 """
    # def judge_carno_exists(self, start_no: int, end_no: int) -> bool:
    #     """ 判断车辆编号数据库是否存在 """
    #
    #     result = (
    #         dao_session.sub_session().query(
    #             func.count(XcEbike2Carinfo.carId).label("count")
    #         )
    #         .filter(
    #             XcEbike2Carinfo.carId >= start_no,
    #             XcEbike2Carinfo.carId <= end_no,
    #         )
    #         .scalar()
    #         or 0
    #     )
    #     return True if result else False
    #
    # def multi_insert_carno_record(self, start_no: int, end_no: int) -> int:
    #     """ 插入车辆编号记录到数据库 """
    #
    #     start_no_int = int(start_no)
    #     end_no_int = int(end_no)
    #     datas = [
    #         XcEbike2Carinfo(
    #             carId=item, createdAt=datetime.now(), updatedAt=datetime.now()
    #         )
    #         for item in range(start_no_int, end_no_int+1)
    #     ]
    #     dao_session.session().add_all(datas)
    #     try:
    #         dao_session.session().commit()
    #     except Exception as e:
    #         logger.info(f"插入车辆编号到数据库异常: {e}")
    #         dao_session.session().rollback()
    #         return 0
    #     return len(datas)
    #
    # def judge_car_bind_imei_status(self, start_no: int, end_no: int) -> str:
    #     """ 判断车辆是否绑定或者下架 """
    #
    #     ret_info = ""
    #     result = (
    #         dao_session.sub_session().query(
    #             XcEbike2BindingInfo.carId,
    #             XcEbike2BindingInfo.imei,
    #         )
    #         .filter(
    #             XcEbike2BindingInfo.carId >= start_no,
    #             XcEbike2BindingInfo.carId <= end_no,
    #             XcEbike2BindingInfo.deletedAt.is_(None),
    #         )
    #         .all()
    #     )
    #     if not result:
    #         return ret_info
    #
    #     car_id_list = {item.carId for item in result}
    #     if car_id_list:
    #         ret_info += ",".join(car_id_list)
    #         ret_info += "已绑定设备"
    #     return ret_info
    #
    # def multi_delete_carno(self, start_no: int, end_no: int) -> int:
    #     """ 批量删除车辆号段 """
    #
    #     result = (
    #         dao_session.session().query(XcEbike2Carinfo)
    #         .filter(
    #             XcEbike2Carinfo.carId >= start_no,
    #             XcEbike2Carinfo.carId <= end_no,
    #         )
    #         .delete()
    #     )
    #     try:
    #         dao_session.session().commit()
    #     except Exception as e:
    #         logger.info(f"批量删除车辆号段异常: {e}")
    #         dao_session.session().rollback()
    #         return 0
    #     return result
    pass


