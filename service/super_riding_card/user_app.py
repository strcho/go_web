import datetime
import json
import base64
from sqlalchemy.sql import func
# from model.all_model import XcEbike2RidingConfig, XcEbike2SuperRidingCard
from service import MBService
from service.config import ConfigService
from mbutils import dao_session
from utils.constant.account import *
from utils.constant.config import ConfigName
from utils.constant.redis_key import ALL_USER_LAST_SERVICE_ID


class UserAppService(MBService):
    # 缓存限制频率
    # @staticmethod
    # def query_list(valid_data) -> list:
    #     """
    #     :param valid_data:
    #     :return:
    #     {
    #     "used":[
    #         {
    #             "card_id":10001,
    #             "name":"7天酷骑卡",
    #             "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
    #             "description_tag":"仅限武汉地区;通勤",
    #             "promotion_tag":"热销;实惠",
    #             "detail_info":"<p>富文本</p>",
    #             "valid_day":7,
    #             "remain_times":2,
    #             "current_cost":500,
    #             "origin_cost":2000
    #         }
    #     ],
    #     "rule_info": "<a name='info'>富文本</a>"
    # }
    #     """
    #     service_id, = valid_data
    #     result = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter(XcEbike2RidingConfig.serviceId == service_id,
    #                                                                       XcEbike2RidingConfig.state <= RidingCardConfigState.ENABLE.value).order_by(
    #         XcEbike2RidingConfig.sort_num.desc()).all()
    #     card_list = []
    #     for one in result:
    #         try:
    #             car_info = {"card_id": one.id, "name": one.ridingCardName}
    #             content = json.loads(one.content)
    #             if one.state == RidingCardConfigState.ENABLE.value:
    #                 if content["autoOpen"] and (datetime.datetime.now().timestamp() < content["openStartTime"]
    #                                             or datetime.datetime.now().timestamp() > content["openEndTime"]):
    #                     # 开启后,到预设结束时间关闭
    #                     one.state = RidingCardConfigState.DISABLE.value
    #                     dao_session.session.tenant_db().commit()
    #                     continue
    #             else:
    #                 if content["autoOpen"] and (
    #                         content["openStartTime"] <= datetime.datetime.now().timestamp() <= content["openEndTime"]):
    #                     # 关闭时候, 到预设时间后开启
    #                     one.state = RidingCardConfigState.ENABLE.value
    #                     dao_session.session.tenant_db().commit()
    #                 else:
    #                     continue
    #             car_info["image_url"] = content["backOfCardUrl"]
    #             car_info["description_tag"] = content["descriptionTag"]
    #             car_info["promotion_tag"] = content["promotionTag"]
    #             car_info["detail_info"] = content["detailInfo"]
    #             car_info["valid_day"] = content["expiryDate"]
    #             car_info["remain_times"] = content["receTimes"]
    #             car_info["current_cost"] = content["curCost"]
    #             car_info["origin_cost"] = content["originCost"]
    #             car_info["serial_type"] = one.type
    #             car_info["iz_total_times"] = content["isTotalTimes"]
    #             car_info["rece_times"] = content["receTimes"]
    #             card_list.append(car_info)
    #         except Exception:
    #             # 可能是发了pre, 然后正式服上建立骑行卡,导致少参数影响测试服
    #             pass
    #     rule_config = ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id)
    #     return {"used": card_list, "rule_info": rule_config.get("rule_info", "")}
    #
    # def query_my_list(self, valid_data, user_id) -> dict:
    #     """
    #     :return:
    # {
    #     "used":[
    #         {
    #             "card_id":10001,
    #             "name":"7天酷骑卡",
    #             "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
    #             "description_tag":"仅限武汉地区;通勤",
    #             "promotion_tag":"热销",
    #             "detail_info":"<p>富文本</p>",
    #             "expired_date":"2021-04-09 11:12:13",
    #             "remain_times":2
    #         }
    #     ],
    #     "expired":[
    #         {
    #             "card_id":10001,
    #             "name":"7天酷骑卡",
    #             "description_tag":"仅限武汉地区;通勤",
    #             "expired_date":"2021-04-09 11:12:13",
    #             "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
    #             "detail_info":"<p>富文本</p>"
    #         }
    #     ],
    #     "cost_use":10001
    #     "rule_info": "<a name='info'>富文本</a>"
    # }
    #     """
    #     service_id, = valid_data
    #     dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, user_id, service_id)
    #     rule_info = ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id).get("rule_info",
    #                                                                                                      "")
    #
    #     first_id = self.get_current_card_id(service_id, user_id)
    #     res_dict = {"used": [], "expired": [], "rule_info": rule_info, "cost_use": first_id}
    #     result = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter(
    #         XcEbike2SuperRidingCard.objectId == user_id,
    #         XcEbike2SuperRidingCard.state <= UserRidingCardState.EXPIRED.value,
    #         XcEbike2SuperRidingCard.cardExpiredDate >= datetime.datetime.now() - datetime.timedelta(weeks=13)).order_by(
    #         XcEbike2SuperRidingCard.createdAt.desc()).all()
    #     for one in result:
    #         car_info = {"card_id": one.id}
    #         content = json.loads(one.content)
    #         car_info["name"] = content["ridingCardName"]
    #         car_info["image_url"] = content["backOfCardUrl"]
    #         car_info["description_tag"] = content.get("descriptionTag", "限全国")
    #         car_info["detail_info"] = content.get("detailInfo", "") or str(
    #             base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
    #                 content["expiryDate"],
    #                 "累计" if one.isTotalTimes else "每日",
    #                 content["receTimes"], int(float(content["freeTime"]) * 60)).encode("utf-8")),
    #             "utf-8")
    #         car_info["cardExpiredDate"] = self.datetime2num(one.cardExpiredDate)
    #         if one.state == UserRidingCardState.EXPIRED.value:
    #             res_dict["expired"].append(car_info)
    #         else:
    #             car_info["remain_times"] = one.remainTimes
    #             car_info["iz_total_times"] = one.isTotalTimes
    #             car_info["rece_times"] = one.receTimes
    #             car_info["promotion_tag"] = content.get("promotionTag", "人气优选")
    #             car_info["deductionType"] = one.deductionType
    #             res_dict["used"].append(car_info)
    #     return res_dict
    #
    # def query_my_list_in_platform(self, valid_data, user_id) -> dict:
    #     """
    #     :return:
    # {
    #     "used":[
    #         {
    #             "card_id":10001,
    #             "name":"7天酷骑卡",
    #             "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
    #             "description_tag":"仅限武汉地区;通勤",
    #             "promotion_tag":"热销",
    #             "detail_info":"<p>富文本</p>",
    #             "expired_date":"2021-04-09 11:12:13",
    #             "remain_times":2
    #         }
    #     ],
    #     "cost_use":10001
    # }
    #     """
    #     service_id, = valid_data
    #     dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, user_id, service_id)
    #     rule_info = ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id).get("rule_info",
    #                                                                                                      "")
    #
    #     first_id = self.get_current_card_id(service_id, user_id)
    #     res_dict = {"used": [], "expired": [], "rule_info": rule_info, "cost_use": first_id}
    #     result = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter(
    #         XcEbike2SuperRidingCard.objectId == user_id,
    #         XcEbike2SuperRidingCard.state <= UserRidingCardState.EXPIRED.value,
    #         XcEbike2SuperRidingCard.cardExpiredDate >= datetime.datetime.now() - datetime.timedelta(weeks=13)).order_by(
    #         XcEbike2SuperRidingCard.createdAt.desc()).all()
    #     for one in result:
    #         if one.state != UserRidingCardState.EXPIRED.value:
    #             car_info = {"card_id": one.id}
    #             content = json.loads(one.content)
    #             car_info["name"] = content["ridingCardName"]
    #             car_info["image_url"] = content["backOfCardUrl"]
    #             car_info["description_tag"] = content.get("descriptionTag", "限全国")
    #             car_info["detail_info"] = content.get("detailInfo", "") or str(
    #                     base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
    #                         content["expiryDate"],
    #                         "累计" if one.isTotalTimes else "每日",
    #                         content["receTimes"], int(float(content["freeTime"]) * 60)).encode("utf-8")),
    #                     "utf-8")
    #             car_info["cardExpiredDate"] = self.datetime2num(one.cardExpiredDate)
    #             car_info["remain_times"] = one.remainTimes
    #             car_info["iz_total_times"] = one.isTotalTimes
    #             car_info["rece_times"] = one.receTimes
    #             car_info["free_time_second"] = one.freeTime
    #             car_info["free_distance_meter"] = one.freeDistance
    #             car_info["free_money_cent"] = one.freeMoney
    #             car_info["promotion_tag"] = content.get("promotionTag", "人气优选")
    #             car_info["deductionType"] = one.deductionType
    #             res_dict["used"].append(car_info)
    #     return res_dict
    #
    # @staticmethod
    # def get_current_card_id(service_id: int, user_id: str) -> int:
    #     """
    #     该用户的,没有过期的, 使用中的,  union,
    #     (无次卡的, deductionType越小, 最后一次使用不是今日的, 剩余次数最多的额)(次卡的, 过期时间最近的, 次数够的)
    #     服务区没有配置或者在配置服务区里面的
    #     :param service_id:
    #     :param user_id:
    #     :return:
    #     """
    #
    #     # 1.骑行卡过期判定
    #     dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter(XcEbike2SuperRidingCard.objectId == user_id,
    #                                                                 XcEbike2SuperRidingCard.state == UserRidingCardState.USING.value,
    #                                                                 XcEbike2SuperRidingCard.cardExpiredDate < datetime.datetime.now()).update(
    #         {"state": UserRidingCardState.EXPIRED.value})
    #     # 2.骑行卡次数重置, 如果上次使用时间不是今天的, 则把非次卡的, 时间和剩余次数重置到最多再计算
    #     dao_session.session.tenant_db().execute("""
    #     update xc_ebike_2_super_riding_card
    #     set remainTimes=receTimes,lastUseTime=now()
    #     where objectId=:user_id and state=:state and isTotalTimes=0 and ( lastUseTime<CURDATE() or lastUseTime IS NULL )
    #     """, {"user_id": user_id, "state": UserRidingCardState.USING.value})
    #     dao_session.session.tenant_db().commit()
    #     # 3.选出最佳骑行卡id
    #     many = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard.id,
    #                                        XcEbike2SuperRidingCard.effectiveServiceIds).filter(
    #         XcEbike2SuperRidingCard.objectId == user_id,
    #         XcEbike2SuperRidingCard.state == UserRidingCardState.USING.value,
    #         XcEbike2SuperRidingCard.cardExpiredDate >= datetime.datetime.now(),
    #         XcEbike2SuperRidingCard.remainTimes > 0
    #     ).order_by(XcEbike2SuperRidingCard.deductionType.asc(), XcEbike2SuperRidingCard.isTotalTimes.asc(),
    #                XcEbike2SuperRidingCard.cardExpiredDate.asc()).all()
    #     for _id, effectiveServiceIds in many:
    #         if not effectiveServiceIds or effectiveServiceIds == "all":
    #             return _id
    #         else:
    #             if str(service_id) in effectiveServiceIds.split(";"):
    #                 return _id
    #     return None
    #
    # @staticmethod
    # def can_buy_card(user_id: str, valid_data: tuple):
    #     service_id, = valid_data
    #     content = ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id)
    #     max_num = content.get("max_num", 20)
    #     dao_session.session.tenant_db().query()
    #     current_num = dao_session.session.tenant_db().query(func.count(XcEbike2SuperRidingCard.id)).filter(
    #         XcEbike2SuperRidingCard.objectId == user_id,
    #         XcEbike2SuperRidingCard.state <= UserRidingCardState.USING.value).scalar() or 0
    #     return True if current_num < max_num else False
    #
    # @staticmethod
    # def current_expired_date(service_id: int, user_id: str) -> int:
    #     card_id = UserAppService().get_current_card_id(service_id, user_id)
    #     if card_id:
    #         one = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard.cardExpiredDate).filter_by(id=card_id).first()
    #         return int(one[0].timestamp())
    #     return 0
    pass