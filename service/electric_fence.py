from sqlalchemy.dialects.mysql import insert
import json
from model.all_model import *
from service import MBService
from mbutils import dao_session
from utils.constant.redis_key import *
from utils.constant.device import GfenceType
from service.geo_algorithm import search_from_yuntu

class ParkService(MBService):
    """
    停车区相关操作
    """

    @staticmethod
    def get_near_parking(lng, lat, radius, type=2)->[]:
        """
        根据当前GPS获取附近的停车点, 默认5个
        gfence_info:
            {"centerLng":"112.23962107078685","agentId":"2","gFenceId":"1042","type":"2"
            ,"maxParkingNumber":"50","shapeType":"polygon","hide":"",
            "pointList":"[[112.23956899557416,30.318704367332618],[112.239763,30.31819],
            [112.23966689620154,30.318162570332273],[112.23947914157371,30.318665006112994]]",
            "directional":"1","enable":"","tBeacon":"","name":"北京227","rfid":"",
            "centerLat":"30.318433468832445","serviceId":"7","pics":"","direction":"315"}
        :param lng:
        :param lat:
        :param radius:
        :param type:
        :return:
        """
        datas = search_from_yuntu(type, lng, lat, radius, 5)
        if not datas:
            return []
        res = []
        for d in datas:
            gfence_id = d["_name"]
            distance = d["_distance"]
            gfence_info = dao_session.redis_session.r.hgetall(GFENCE_INFO.format(gfence_id=gfence_id))
            # flag = dao_session.redis_session.r.zscore(GFENCE_PILE_SET, gfence_id)  # 满桩判断,挪车的时候判断过了,不在这判断
            if gfence_info["type"] in [GfenceType.FOR_PARK.value, GfenceType.NO_PARKING.value]:
                if not dao_session.redis_session.r.hexists(GFENCE_INFO.format(gfence_id=gfence_info["serviceId"]),
                                                           "serviceId"):
                    # 如果是站点或者禁停区, 校验服务区是否存在
                    break
            res.append({
                "gFenceId": gfence_id,
                "distance": distance,
                "shapeType": gfence_info["shapeType"],
                "serviceId": gfence_info["serviceId"],
                "pointList": json.loads(gfence_info["pointList"])
            })
        return res

    @staticmethod
    def set_parking_info(imei, park_id):
        """ 绑定停车区, 解绑禁停区, 绑定服务区 """
        if park_id:
            with dao_session.redis_session.r.pipeline(transaction=False) as pp:
                pp.set(DEVICE_BINDING_PARK_KEY.format(imei=imei), park_id)
                pp.set(TEMP_DEVICE_BINDING_PARK_KEY.format(imei=imei), park_id)
                pp.delete(DEVICE_BINDING_NO_PARK_KEY.format(imei=imei))
                pp.sadd(PARK_GFENCE_BINGING_IMEI.format(park_id=park_id), imei)
                pp.execute()

    @staticmethod
    def del_parking_info(imei):
        """ 删除停车区绑定, 删除禁停区绑定 """
        with dao_session.redis_session.r.pipeline(transaction=False) as pp:
            pp.delete(DEVICE_BINDING_PARK_KEY.format(imei=imei))
            pp.delete(DEVICE_BINDING_NO_PARK_KEY.format(imei=imei))
            pp.execute()

    @staticmethod
    def unbind_imei_park_id(imei):
        """ 删除停车区绑定关系, 并返回原来停车区id """
        park_id = None
        try:
            parking_info = dao_session.session().query(XcEbikeParking).filter_by(imei=imei).first()
            park_id = parking_info.parkingId
            parking_info.parkingId = None
            parking_info.noParkingId = None
            dao_session.session().commit()
        except Exception:
            dao_session.session().rollback()
        return park_id

    @staticmethod
    def bind_imei_park_id(imei, park_id, service_id):
        """ 创建或者更新imei的停车区信息 """
        params = {
            "imei": imei,
            "serviceId": service_id,
            "parkingId": park_id,
            "agentId": 2,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now(),
        }

        on_duplicate_key_stmt = insert(XcEbikeParking).values(**params).on_duplicate_key_update(
            **{"serviceId": service_id,
               "parkingId": park_id,
               "agentId": 2}
        )
        dao_session.session().execute(on_duplicate_key_stmt)
        dao_session.session().commit()
