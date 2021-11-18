import json
import datetime
from model.all_model import XcOpman, XcEbike2SuperRidingCard, XcEbike2RidingConfig
from service import MBService
from mbutils import dao_session, MbException
from utils.constant.account import *
from service.config import ConfigService
from mbutils import logger
from utils.constant.config import ConfigName
from service.super_riding_card.user_app import UserAppService
from utils.constant.redis_key import ALL_USER_LAST_SERVICE_ID


class PlatformService(MBService):
    def create_one(self, valid_data, phone) -> None:
        service_id, name, valid_day, available_times, iz_total_times, current_cost, origin_cost, deduction_type, free_time_second, \
        free_money_cent, free_distance_meter, effective_service_ids, auto_open, open_start_time, open_end_time, \
        image_url, description_tag, promotion_tag, detail_info = valid_data
        one = dao_session.session.tenant_db().query(XcEbike2RidingConfig.id).filter(XcEbike2RidingConfig.ridingCardName == name,
                                                                          XcEbike2RidingConfig.state <= RidingCardConfigState.ENABLE.value).first()
        if one:
            raise MbException("骑行卡名称重复")
        serial_type = DeductionType(deduction_type).get_serial_type()
        content = json.dumps({
            "ridingCardName": name,
            "serialType": serial_type,
            "expiryDate": valid_day,
            "originCost": origin_cost,
            "curCost": current_cost,
            "receTimes": available_times,
            "isTotalTimes": iz_total_times,
            "freeTime": str(round(free_time_second / 3600, 2)),
            "pictureState": 0,
            "backOfCardUrl": image_url,
            "state": 0,
            "sortType": 0,
            "slogan": name,
            "createdAt": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "freeTimeseconds": free_time_second,
            "deductionType": deduction_type,
            "freeMoney": free_money_cent,
            "freeDistance": free_distance_meter,
            "effectiveServiceIds": effective_service_ids,
            "autoOpen": auto_open,
            "openStartTime": open_start_time,
            "openEndTime": open_end_time,
            "descriptionTag": description_tag,
            "promotionTag": promotion_tag,
            "detailInfo": detail_info,
            "createdPhone": phone
        })
        params = {
            "type": serial_type,
            "content": content,
            "state": RidingCardConfigState.ENABLE.value,
            "serviceId": service_id,
            "ridingCardName": name,
            "sort_num": datetime.datetime.now().timestamp(),
            "createdAt": datetime.datetime.now(),
            "updatedAt": datetime.datetime.now()
        }
        card = XcEbike2RidingConfig(**params)
        dao_session.session.tenant_db().add(card)
        dao_session.session.tenant_db().commit()

    def detail(self, valid_data) -> dict:
        """
        :param valid_data:
        :return:
        {
            "card_id":10001,
            "service_id":2,
            "name":"7天酷骑卡",
            "valid_day":7,
            "available_times":2,
            "iz_total_times":True
            "current_cost":500,
            "origin_cost":2000,
            "deduction_type":1,
            "free_time_second": 11,
            "free_money_cent": 22,
            "free_distance_meter": 33,
            "effective_service_ids": "2;10002;3001",
            "effective_service_names": "小安测试区;大咖服务区",
            "state":1,
            "auto_open":True,
            "open_start_time":1617940403,
            "open_end_time":1617940403,
            "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
            "description_tag":"仅限武汉地区;通勤",
            "promotion_tag":"热销",
            "detail_info":"<p>富文本</p>",
            "createdAt":"2021-04-09 11:12:13",
            "createdMan":"刘德华"
        }
        """
        card_id, = valid_data
        one = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter_by(id=card_id).one()
        car_info = {"card_id": one.id, "service_id": one.serviceId, "name": one.ridingCardName, "state": one.state,
                    "createdAt": int(one.createdAt.timestamp())}
        content = json.loads(one.content)
        car_info["deduction_type"] = content["deductionType"]
        car_info["available_times"] = content["receTimes"]
        car_info["iz_total_times"] = content["isTotalTimes"]
        car_info["current_cost"] = content["curCost"]
        car_info["origin_cost"] = content["originCost"]
        car_info["valid_day"] = content["expiryDate"]
        car_info["free_time_second"] = content["freeTimeseconds"]
        car_info["free_money_cent"] = content["freeMoney"]
        car_info["free_distance_meter"] = content["freeDistance"]
        car_info["effective_service_ids"] = content["effectiveServiceIds"]
        car_info["effective_service_names"] = ""
        car_info["image_url"] = content["backOfCardUrl"]
        car_info["auto_open"] = content["autoOpen"]
        car_info["open_start_time"] = content["openStartTime"]
        car_info["open_end_time"] = content["openEndTime"]
        car_info["description_tag"] = content["descriptionTag"]
        car_info["promotion_tag"] = content["promotionTag"]
        car_info["detail_info"] = content["detailInfo"]
        one = dao_session.session.tenant_db().query(XcOpman.name).filter_by(opManId=content["createdPhone"]).first()
        car_info["createdMan"] = one.name if one else ""
        car_info["createdPhone"] = content["createdPhone"]
        return car_info

    def query_list(self, valid_data) -> list:
        """
        :param valid_data:
        :return: [{
                "card_id":1112213,
                "name":"7天骑行卡",
                "deduction_type":1,
                "current_cost":500,
                "origin_cost":2000,
                "valid_day":7,
                "state":1,
                "createdAt":"2021-04-09 11:12:13",
            }]
        """
        service_id, = valid_data
        result = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter(XcEbike2RidingConfig.serviceId == service_id,
                                                                          XcEbike2RidingConfig.state <= RidingCardConfigState.ENABLE.value).order_by(
            XcEbike2RidingConfig.sort_num.desc()).all()
        card_list = []
        for one in result:
            try:
                car_info = {"card_id": one.id, "name": one.ridingCardName}
                content = json.loads(one.content)
                car_info["deduction_type"] = content["deductionType"]
                car_info["current_cost"] = content["curCost"]
                car_info["origin_cost"] = content["originCost"]
                car_info["valid_day"] = content["expiryDate"]
                car_info["state"] = one.state
                car_info["createdAt"] = int(one.createdAt.timestamp())
                card_list.append(car_info)
            except Exception:
                # 可能是发了pre, 然后正式服上建立骑行卡,导致少参数影响测试服
                pass
        return card_list

    def get_rule_config(self, valid_data) -> dict:
        service_id, = valid_data
        return ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id)

    def set_rule_config(self, valid_data) -> dict:
        service_id, rule_info, max_num = valid_data
        value = {"rule_info": rule_info, "max_num": max_num}
        ConfigService().set_router_content(ConfigName.SUPERRIDINGCARD.value, json.dumps(value), service_id)

    def delete_one(self, valid_data) -> None:
        card_id, = valid_data
        try:
            dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter_by(id=card_id).update(
                {"state": RidingCardConfigState.DELETE.value})
            dao_session.session.tenant_db().commit()
        except Exception:
            raise MbException("无效的骑行卡id")

    def enable_one(self, valid_data) -> None:
        card_id, enable = valid_data
        try:
            one = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter_by(id=card_id).one()
            one.state = RidingCardConfigState.ENABLE.value if enable else RidingCardConfigState.DISABLE.value
            old_content = json.loads(one.content)
            old_content["autoOpen"] = False
            old_content["openStartTime"] = 0
            old_content["openEndTime"] = 0
            one.content = json.dumps(old_content)
            dao_session.session.tenant_db().commit()
        except Exception:
            raise MbException("无效的骑行卡id")

    def edit_one(self, valid_data):
        card_id, auto_open, open_start_time, open_end_time = valid_data
        try:
            one = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter_by(id=card_id).one()
            old_content = json.loads(one.content)
            old_content["autoOpen"] = auto_open
            old_content["openStartTime"] = open_start_time
            old_content["openEndTime"] = open_end_time
            one.content = json.dumps(old_content)
            dao_session.session.tenant_db().commit()
        except Exception:
            raise MbException("无效的骑行卡id")

    def change_sort(self, valid_data) -> None:
        """
        [{"id":1, "sort_num":2},{"id":3, "sort_num":4},{"id":5, "sort_num":9},{"id":6, "sort_num":7}]
        :param valid_data:
        :return:
        """
        update_list, = valid_data
        try:
            dao_session.session.tenant_db().bulk_update_mappings(XcEbike2RidingConfig, update_list)
            dao_session.session.tenant_db().commit()
            return 'OK'
        except Exception:
            raise MbException("参数错误")

    def user_card_info(self, valid_data):
        """
        :param valid_data:
        :return:
        """
        object_id, = valid_data
        try:
            dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter(
                XcEbike2SuperRidingCard.state == UserRidingCardState.USING.value,
                XcEbike2SuperRidingCard.objectId == object_id,
                XcEbike2SuperRidingCard.cardExpiredDate <= datetime.datetime.now()).update(
                {"state": UserRidingCardState.EXPIRED.value})
            dao_session.session.tenant_db().commit()
        except Exception:
            pass
        service_id = dao_session.redis_session.r.hget(ALL_USER_LAST_SERVICE_ID, object_id) or 0
        return UserAppService().query_my_list_in_platform((service_id,), object_id)

    def modify_time(self, valid_data):
        card_id, remain_times, duration = valid_data
        one = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter_by(id=card_id).first()
        if not one:
            raise MbException("无效的骑行卡id")
        try:
            if self.exists_param(duration):
                one.cardExpiredDate = datetime.datetime.now() + datetime.timedelta(days=duration)
            if self.exists_param(remain_times):
                one.remainTimes = remain_times
            one.updatedAt = datetime.datetime.now()
            dao_session.session.tenant_db().commit()
        except Exception:
            raise MbException("修改骑行卡时长失败")
