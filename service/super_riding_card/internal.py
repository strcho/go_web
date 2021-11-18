import math
import json
import datetime, time
from service import MBService
from mbutils import dao_session, MbException
from utils.constant.account import *
from service.config import ConfigService
from mbutils import logger
from service.super_riding_card.user_app import UserAppService
from utils.constant.config import ConfigName
from utils.constant.redis_key import *


class InternalService(MBService):
    # def add_card(self, valid_data) -> None:
    #     """
    #     1新app, 新服务的时候, 参加老的赠送, 购买卡得到的都是新的个人骑行卡; 这种情况下用户可以看到自己的骑行卡流水,
    #     看不到骑行卡其他信息(解决方式是继续按照老的方式添加, 等到app用新的之后再走新卡添加)
    #     2老的app, 新的服务的时候, 赠送购买的卡也是新的个人骑行卡
    #
    #     :param valid_data:
    #     :return:
    #     """
    #     card_id, object_id = valid_data
    #     try:
    #         one = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter_by(id=card_id).one()
    #     except Exception:
    #         raise MbException("无该骑行卡配置")
    #     content = json.loads(one.content)
    #
    #     user_use_new = dao_session.redis_session.r.hget(USER_SUPER_CARD, object_id)
    #     reverting = dao_session.redis_session.r.hget(REVERT_USER_SUPER_CARD, object_id)
    #     if user_use_new:
    #         # todo 最多买20张卡
    #         iz_total_times = content.get("serialType", "10") == SERIAL_TYPE.RIDING_COUNT_CARD.value  # bool形可以隐式转化0,1
    #         params = {
    #             "objectId": object_id,
    #             "deductionType": content["deductionType"],
    #             "configId": card_id,
    #             "freeTime": content["freeTimeseconds"],
    #             "freeDistance": content["freeDistance"],
    #             "freeMoney": content["freeMoney"],
    #             "isTotalTimes": content.get("isTotalTimes", iz_total_times),
    #             "receTimes": content["receTimes"],
    #             "effectiveServiceIds": content.get("effectiveServiceIds", ""),
    #             "remainTimes": content["receTimes"],
    #             "lastUseTime": None,
    #             "startTime": datetime.datetime.now(),
    #             "cardExpiredDate": datetime.datetime.now() + datetime.timedelta(hours=24 * int(content["expiryDate"])),
    #             "content": one.content,
    #             "state": UserRidingCardState.USING.value,
    #             "createdAt": datetime.datetime.now(),
    #             "updatedAt": datetime.datetime.now()
    #         }
    #         try:
    #             user_card = XcEbike2SuperRidingCard(**params)
    #             dao_session.session.tenant_db().add(user_card)
    #             dao_session.session.tenant_db().commit()
    #         except Exception:
    #             raise MbException("添加超级骑行卡失败")
    #     # 如果是走老卡
    #     if not user_use_new or reverting:
    #         if one.type == SERIAL_TYPE.RIDING_COUNT_CARD.value:
    #             params = {
    #                 "objectId": object_id,
    #                 "configId": card_id,
    #                 "ridingCardType": SERIAL_TYPE.RIDING_COUNT_CARD.value,
    #                 "startTime": datetime.datetime.now(),
    #                 "cardExpiredDate": datetime.datetime.now() + datetime.timedelta(
    #                     hours=24 * int(content["expiryDate"])),
    #                 "content": one.content,
    #                 "receTimes": content["receTimes"],
    #                 "usedreceTimes": 0,
    #                 "state": UserRidingCardState.USING.value,
    #                 "createdAt": datetime.datetime.now(),
    #                 "updatedAt": datetime.datetime.now()
    #             }
    #             try:
    #                 user_card = XcEbike2RidingCountCard(**params)
    #                 dao_session.session.tenant_db().add(user_card)
    #                 dao_session.session.tenant_db().commit()
    #             except Exception as e:
    #                 logger.error("添加原骑行次卡失败 error: {}".format(e))
    #                 raise MbException("添加原骑行次卡失败")
    #         else:
    #             try:
    #                 my_card = dao_session.session.tenant_db().query(XcEbike2RidingCard).filter_by(objectId=object_id).one()
    #             except Exception:
    #                 # 如果不存在个人骑行卡
    #                 params = {
    #                     "objectId": object_id,
    #                     "ridingCardType": SERIAL_TYPE.RIDING_COUNT_CARD.value,
    #                     "startTime": datetime.datetime.now(),
    #                     "cardExpiredDate": datetime.datetime.now() + datetime.timedelta(
    #                         hours=24 * int(content["expiryDate"])),
    #                     "content": one.content,
    #                     "createdAt": datetime.datetime.now(),
    #                     "updatedAt": datetime.datetime.now()
    #                 }
    #                 try:
    #                     user_card = XcEbike2RidingCard(**params)
    #                     dao_session.session.tenant_db().add(user_card)
    #                     dao_session.session.tenant_db().commit()
    #                 except Exception:
    #                     raise MbException("添加原骑行卡失败")
    #                 return {"suc": True, "message": "添加骑行卡成功"}
    #             # 如果存在个人骑行卡
    #             # 显示成最后一种骑行卡的类型(先买月卡后买日卡显示日卡31天, 先买日卡后买月卡显示月卡31天),对计费没有影响
    #             if my_card.cardExpiredDate and my_card.cardExpiredDate >= datetime.datetime.now():
    #                 my_card.cardExpiredDate = my_card.cardExpiredDate + datetime.timedelta(
    #                     hours=24 * int(content["expiryDate"]))
    #             else:
    #                 my_card.cardExpiredDate = datetime.datetime.now() + datetime.timedelta(
    #                     hours=24 * int(content["expiryDate"]))
    #             my_card.ridingCardType = one.type
    #             my_card.content = one.content
    #             my_card.updatedAt = datetime.datetime.now()
    #             dao_session.session.tenant_db().commit()
    #     return {"suc": True, "message": "添加骑行卡成功"}
    #
    # def compute_cost(self, valid_data: tuple):
    #     """
    #     计算骑行卡费用, 如果有老卡,先扣老卡, 如果有新卡扣新卡
    #     :return:
    #     """
    #     itinerary_info, service_id, is_free_order, is_activity_free_order, is_activity_discount, object_id = valid_data
    #     response = {"hasRidingCard": False, "cost": itinerary_info["curCost"], "deduction": 0, "isUseRidingCard": False,
    #                 "isUseRidingCountCard": False, "isUserActivityDiscount": False}
    #     # 免单逻辑开始
    #     if is_free_order and not is_activity_free_order:
    #         free_time = 86400
    #         cfg = ConfigService().get_router_content(ConfigName.FREEORDER.value, service_id)
    #         if cfg:
    #             free_time = cfg.get("freeseconds", 0) or float(cfg.get("freeTime", 0)) * 3600
    #         self.subtract_free_time(response, itinerary_info, free_time, 0, service_id,
    #                                 object_id, is_activity_discount)
    #         logger.info("delete_later:compute_cost:is_free_order", response)
    #         return response
    #
    #     # 固定活动免单逻辑开始
    #     if is_activity_free_order:
    #         free_time_from_reward = 0
    #         free_order_info = dao_session.redis_session.r.get(FREE_USER_KEY.format(object_id))
    #         if free_order_info:
    #             free_order_info = json.loads(free_order_info)
    #             if int(free_order_info["num"]) > 0:
    #                 free_time_from_reward = float(free_order_info.get("second", 0)) or float(
    #                     free_order_info.get("hour", 0)) * 3600
    #             self.subtract_free_time(response, itinerary_info, free_time_from_reward, 0, service_id,
    #                                     object_id, is_activity_discount)
    #         logger.info("delete_later:compute_cost:is_activity_free_order", free_time_from_reward, free_order_info)
    #         return response
    #
    #     # 计算骑行卡收费逻辑
    #     user_use_new = dao_session.redis_session.r.hget(USER_SUPER_CARD, object_id) and \
    #                    not dao_session.redis_session.r.hget(REVERT_USER_SUPER_CARD, object_id)
    #     if not user_use_new:
    #         # 老的骑行卡方式
    #         my_card = dao_session.session.tenant_db().query(XcEbike2RidingCard).filter(
    #             XcEbike2RidingCard.objectId == object_id,
    #             XcEbike2RidingCard.cardExpiredDate >= datetime.datetime.now()).first()
    #         if my_card:
    #             response["hasRidingCard"] = True
    #             used_times = int(dao_session.redis_session.r.get(RECE_TIMES_KEY.format(user_id=object_id)) or 0)
    #             content = json.loads(my_card.content)
    #             total_times = int(content.get("receTimes", 0))
    #             if total_times > used_times:
    #                 user_free_time = float(content["freeTime"]) * 3600
    #                 self.subtract_free_time(response, itinerary_info, user_free_time, 0, service_id,
    #                                         object_id, is_activity_discount)
    #                 response["isUseRidingCard"] = True
    #                 logger.info("delete_later:compute_cost:XcEbike2RidingCard", response)
    #         else:
    #             # 老的次卡方式
    #             my_count_card = dao_session.session.tenant_db().query(XcEbike2RidingCountCard).filter(
    #                 XcEbike2RidingCountCard.objectId == object_id,
    #                 XcEbike2RidingCountCard.state == UserRidingCardState.USING.value,
    #                 XcEbike2RidingCountCard.cardExpiredDate >= datetime.datetime.now(),
    #                 XcEbike2RidingCountCard.usedreceTimes < XcEbike2RidingCountCard.receTimes
    #             ).order_by(XcEbike2RidingCountCard.cardExpiredDate.asc()).first()
    #             if my_count_card:
    #                 response["hasRidingCard"] = True
    #                 content = json.loads(my_count_card.content)
    #                 total_times = my_count_card.receTimes
    #                 used_times = my_count_card.usedreceTimes
    #                 if total_times > used_times:
    #                     user_free_time = content.get("freeTimeseconds", 0) or float(content.get("freeTime", 0)) * 3600
    #                     self.subtract_free_time(response, itinerary_info, user_free_time, 0, service_id,
    #                                             object_id, is_activity_discount)
    #                     response["isUseRidingCard"] = True
    #                     response["isUseRidingCountCard"] = True
    #                     logger.info("delete_later:compute_cost:XcEbike2RidingCountCard", response)
    #             else:
    #                 # 如果没有任何骑行卡
    #                 if is_activity_discount:
    #                     response["isUserActivityDiscount"] = True
    #
    #     else:
    #         # 新的骑行卡逻辑, 如果有合适的骑行卡
    #         # 有骑行卡 骑行时长卡>骑行里程卡>骑行金额卡>骑行金额次卡>骑行次卡 用完切换到下一张, 同类的先用快要到期的骑行卡
    #         first_card_id = response.get("card_id", None)
    #         if not first_card_id:
    #             first_card_id = UserAppService.get_current_card_id(service_id, object_id)
    #         first_card = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter_by(id=first_card_id).first()
    #         if first_card:
    #             response["hasRidingCard"] = True
    #             response["isUseRidingCard"] = True
    #             response["superRidingCard"] = first_card_id
    #             if first_card.deductionType == DeductionType.MONEY.value:
    #                 self.subtract_money(response, itinerary_info, first_card.freeMoney, is_activity_discount)
    #             else:
    #                 self.subtract_free_time(response, itinerary_info, first_card.freeTime, first_card.freeDistance,
    #                                         service_id, object_id, is_activity_discount)
    #         else:
    #             # 如果没有任何骑行卡
    #             if is_activity_discount:
    #                 response["isUserActivityDiscount"] = True
    #
    #     # 骑行完的response配合 node:verifyActivity, 来累加骑行次数和次卡次数
    #     dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, object_id, service_id)
    #     return response
    #
    # def separate_cost(self, valid_data: tuple):
    #     """
    #     计算骑行卡费用, 如果有老卡,先扣老卡, 如果有新卡扣新卡
    #     :return:
    #     """
    #     itinerary_info, service_id, is_activity_discount, object_id = valid_data
    #     response = {"hasRidingCard": False, "cost": itinerary_info["curCost"], "deduction": 0, "isUseRidingCard": False,
    #                 "isUseRidingCountCard": False, "isUserActivityDiscount": False}
    #
    #     # 计算骑行卡收费逻辑
    #     user_use_new = dao_session.redis_session.r.hget(USER_SUPER_CARD, object_id) and \
    #                    not dao_session.redis_session.r.hget(REVERT_USER_SUPER_CARD, object_id)
    #     if not user_use_new:
    #         # 老的骑行卡方式
    #         my_card = dao_session.session.tenant_db().query(XcEbike2RidingCard).filter(
    #             XcEbike2RidingCard.objectId == object_id,
    #             XcEbike2RidingCard.cardExpiredDate >= datetime.datetime.now()).first()
    #         if my_card:
    #             response["hasRidingCard"] = True
    #             used_times = int(dao_session.redis_session.r.get(RECE_TIMES_KEY.format(user_id=object_id)) or 0)
    #             content = json.loads(my_card.content)
    #             total_times = int(content.get("receTimes", 0))
    #             if total_times > used_times:
    #                 user_free_time = float(content["freeTime"]) * 3600
    #                 self.subtract_free_time(response, itinerary_info, user_free_time, 0, service_id,
    #                                         object_id, is_activity_discount)
    #                 response["isUseRidingCard"] = True
    #                 logger.info("delete_later:compute_cost:XcEbike2RidingCard", response)
    #         else:
    #             # 老的次卡方式
    #             my_count_card = dao_session.session.tenant_db().query(XcEbike2RidingCountCard).filter(
    #                 XcEbike2RidingCountCard.objectId == object_id,
    #                 XcEbike2RidingCountCard.state == UserRidingCardState.USING.value,
    #                 XcEbike2RidingCountCard.cardExpiredDate >= datetime.datetime.now(),
    #                 XcEbike2RidingCountCard.usedreceTimes < XcEbike2RidingCountCard.receTimes
    #             ).order_by(XcEbike2RidingCountCard.cardExpiredDate.asc()).first()
    #             if my_count_card:
    #                 response["hasRidingCard"] = True
    #                 content = json.loads(my_count_card.content)
    #                 total_times = my_count_card.receTimes
    #                 used_times = my_count_card.usedreceTimes
    #                 if total_times > used_times:
    #                     user_free_time = content.get("freeTimeseconds", 0) or float(content.get("freeTime", 0)) * 3600
    #                     self.subtract_free_time(response, itinerary_info, user_free_time, 0, service_id,
    #                                             object_id, is_activity_discount)
    #                     response["isUseRidingCard"] = True
    #                     response["isUseRidingCountCard"] = True
    #                     logger.info("delete_later:compute_cost:XcEbike2RidingCountCard", response)
    #             else:
    #                 # 如果没有任何骑行卡
    #                 if is_activity_discount:
    #                     response["isUserActivityDiscount"] = True
    #
    #     else:
    #         # 新的骑行卡逻辑, 如果有合适的骑行卡
    #         # 有骑行卡 骑行时长卡>骑行里程卡>骑行金额卡>骑行金额次卡>骑行次卡 用完切换到下一张, 同类的先用快要到期的骑行卡
    #         first_card_id = response.get("card_id", None)
    #         if not first_card_id:
    #             first_card_id = UserAppService.get_current_card_id(service_id, object_id)
    #         first_card = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter_by(id=first_card_id).first()
    #         if first_card:
    #             response["hasRidingCard"] = True
    #             response["isUseRidingCard"] = True
    #             response["superRidingCard"] = first_card_id
    #             if first_card.deductionType == DeductionType.MONEY.value:
    #                 self.subtract_money(response, itinerary_info, first_card.freeMoney, is_activity_discount)
    #             else:
    #                 self.subtract_free_time(response, itinerary_info, first_card.freeTime, first_card.freeDistance,
    #                                         service_id, object_id, is_activity_discount)
    #         else:
    #             # 如果没有任何骑行卡
    #             if is_activity_discount:
    #                 response["isUserActivityDiscount"] = True
    #
    #     # 骑行完的response配合 node:verifyActivity, 来累加骑行次数和次卡次数
    #     dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, object_id, service_id)
    #     return response
    #
    # def subtract_free_time(self, response: dict, itinerary_info: dict, free_time: int, free_distance: int,
    #                        service_id: int, object_id: str, is_activity_discount: bool):
    #     """
    #     虽然用户可能有折扣但是如果骑行时长在免单限制内或骑行卡免费限制内还是不用折扣的 如果没用营销活动赠送的则保持服务区原始折扣
    #     扣除免费时间后, 剩余时间的费用
    #     :param response:
    #     :param itinerary_info:
    #     :param free_time:
    #     :param service_id:
    #     :param object_id:
    #     :param is_activity_discount:
    #     :return:
    #     """
    #     # 骑行时间 单位:s
    #     itinerary_time = int(itinerary_info['curTime'] - itinerary_info['startTime'])
    #     if itinerary_time <= free_time and itinerary_info['curItinerary'] <= free_distance:
    #         # 不超过免费时间和距离的费用
    #         response["cost"] = 0
    #         response["deduction"] = int(itinerary_info["curCost"])
    #     else:
    #         # 超出免费的计费, 计算免费的时长和里程等价的费用
    #         config = self.get_cost_config(service_id, object_id, None, itinerary_info["startTime"])
    #         itinerary_info["curTime"] = itinerary_info["startTime"] + free_time
    #         itinerary_info["curItinerary"] = free_distance
    #         # 计算活动或者骑行卡抵扣多少金额
    #         cost = self.compute_origin_cost(itinerary_info, config, service_id, object_id, None,
    #                                         itinerary_info["startTime"])
    #         response["cost"] = max(int(itinerary_info["curCost"]) - cost, 0)
    #         response["deduction"] = min(cost, itinerary_info["curCost"])
    #         if is_activity_discount:
    #             response["isUserActivityDiscount"] = True
    #
    # def subtract_money(self, response: dict, itinerary_info: dict, free_money: int, is_activity_discount: bool):
    #     """
    #     免金额卡
    #     :return:
    #     """
    #     # 骑行时间 单位:s
    #     itinerary_money = int(itinerary_info["curCost"])
    #     if free_money and itinerary_money <= free_money:
    #         # 不超过免费费用时候的计费
    #         response["cost"] = 0
    #         response["deduction"] = itinerary_money
    #     else:
    #         response["cost"] = int(itinerary_info["curCost"]) - free_money
    #         response["deduction"] = free_money
    #         if is_activity_discount:
    #             response["isUserActivityDiscount"] = True
    #
    # def compute_origin_cost(self, itinerary_info: dict, config: dict, service_id: int, object_id: str, imei: str,
    #                         begin_time: float):
    #     # 普通计费或者优惠卡计费
    #     if not config:
    #         config = self.get_cost_config(service_id, object_id, imei, begin_time)
    #         if not config:
    #             return 0
    #     # config中 startDist, startDist是免费距离和时长, fixedTime,fixedDist是起步价范围
    #     ONE_DAY_SECOND = 86400
    #     COST_OF_ONE_DAY = config.get("costOfOneDay", 40)
    #     itinerary = itinerary_info["curItinerary"]
    #     duration = itinerary_info["curTime"] - itinerary_info["startTime"]
    #     # logger.error("duration:", duration)
    #     days = duration // ONE_DAY_SECOND
    #     duration %= ONE_DAY_SECOND
    #     dist_cost = int(config["fixedDistCost"]) + math.ceil((itinerary - int(config["fixedDist"])) / 1000) * int(
    #         config[
    #             "costPerMeter"])
    #     # logger.error("duration2:", config["fixedDistCost"], itinerary, config["fixedDist"])
    #     cost_cycle = int(config["costCycle"]) or 10
    #     time_cost = 0 + math.ceil((duration - int(config["fixedTime"])) / (cost_cycle * 60)) * int(config["costPerMin"])
    #     # logger.error("duration1:", dist_cost, time_cost)
    #     if itinerary < int(config["fixedDist"]):
    #         dist_cost = int(config["fixedDistCost"])
    #     if duration < int(config["fixedTime"]):
    #         time_cost = 0
    #     if itinerary < int(config["startDist"]) and duration < int(config["startTime"]):
    #         dist_cost = 0
    #     if duration < int(config["startTime"]):
    #         time_cost = 0
    #     cost = dist_cost + time_cost
    #     cost = min(COST_OF_ONE_DAY, cost)
    #     cost = cost + COST_OF_ONE_DAY * days
    #     return cost
    #
    # def get_cost_config(self, service_id: int, object_id: str, imei: str, begin_time: float):
    #     """
    #     优惠卡计费 or 普通计费
    #     :param service_id:
    #     :param object_id:
    #     :param imei:
    #     :param begin_time:
    #     :return:
    #     """
    #     router = ConfigName.COST.value
    #     if service_id and object_id:
    #         if not begin_time:
    #             # 获取行程的起始方法, 后面可能可以提出来
    #             ret = dao_session.redis_session.r.hgetall(DEVICE_ITINERARY_INFO.format(iemi=imei))
    #             if ret:
    #                 begin_time = ret["startTime"]
    #             else:
    #                 one = dao_session.session.tenant_db().query(XcEbikeDeviceItinerary.startTime).filter_by(
    #                     userId=object_id).order_by(XcEbikeDeviceItinerary.startTime.desc()).first()
    #                 if one:
    #                     begin_time = one.startTime
    #         favorableCard = dao_session.session.tenant_db().query(XcMieba2FavorableCardUser.end_time).filter_by(
    #             service_id=service_id, object_id=object_id).first()
    #         if begin_time and favorableCard and float(begin_time) < favorableCard.end_time.timestamp():
    #             router = ConfigName.FAVORABLECARDCOST.value
    #     content = ConfigService().get_router_content(router, service_id)
    #     return content
    #
    # def add_count(self, valid_data: tuple):
    #     card_id, = valid_data
    #     one = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter_by(id=card_id).first()
    #     if not one:
    #         raise MbException("无效的骑行卡")
    #     try:
    #         one.remainTimes = one.remainTimes - 1  # 可以减到负数,用于追踪异常的情况
    #         one.lastUseTime = datetime.datetime.now()
    #         one.updatedAt = datetime.datetime.now()
    #         dao_session.session.tenant_db().commit()
    #         return ''
    #     except Exception:
    #         dao_session.session.tenant_db().rollback()
    #         logger.error("骑行卡次数扣除失败,card_id:", card_id)
    #         raise MbException("骑行卡次数扣除失败")
    #
    # def count_reduce(self, valid_data: tuple):
    #     user_order_info, object_id = valid_data
    #     try:
    #         if user_order_info.get("isUseRidingCard") \
    #                 and user_order_info.get("deduction") \
    #                 and not user_order_info.get("isUseRidingCountCard") \
    #                 and not user_order_info.get("superRidingCard"):
    #             # 老骑行扣减
    #             dao_session.redis_session.r.incr(RECE_TIMES_KEY.format(user_id=object_id))
    #             expire_date = int(time.mktime(datetime.datetime.today().timetuple())) + 86400
    #             dao_session.redis_session.r.expire(RECE_TIMES_KEY.format(user_id=object_id), expire_date)
    #         elif user_order_info.get("isUseRidingCountCard") \
    #                 and user_order_info.get("deduction"):
    #             # 老次卡核销
    #             count_card = dao_session.session.tenant_db().query(XcEbike2RidingCountCard) \
    #                 .filter(XcEbike2RidingCountCard.objectId == object_id,
    #                         XcEbike2RidingCountCard.state == 1,
    #                         XcEbike2RidingCountCard.cardExpiredDate >= datetime.datetime.now(),
    #                         XcEbike2RidingCountCard.usedreceTimes < XcEbike2RidingCountCard.receTimes
    #                         ).first()
    #             if count_card:
    #                 count_card.usedreceTimes += 1
    #                 dao_session.session.tenant_db().commit()
    #         elif user_order_info.get("superRidingCard") \
    #                 and user_order_info.get("deduction"):
    #             self.add_count((user_order_info.get("superRidingCard"),))
    #     except Exception as ex:
    #         logger.error("骑行卡核销失败:", object_id, ex)
    #         raise MbException("骑行卡核销失败")
    #     return "成功"
    #
    # def current_during_time(self, valid_data: tuple):
    #     """
    #
    #     :param valid_data:
    #     :return: {"freeTime": 0,  #单位秒
    #                     "freeDistance": 0, #单位米
    #                     "freeMoney": 0 单位分
    #                     }
    #     """
    #     service_id, object_id = valid_data
    #     user_use_new = dao_session.redis_session.r.hget(USER_SUPER_CARD, object_id)
    #     reverting = dao_session.redis_session.r.hget(REVERT_USER_SUPER_CARD, object_id)
    #     if user_use_new:
    #         first_card_id = UserAppService.get_current_card_id(service_id, object_id)
    #         if first_card_id:
    #             first_card = dao_session.session.tenant_db().query(XcEbike2SuperRidingCard).filter_by(id=first_card_id).first()
    #             if first_card:
    #                 return {"freeTime": first_card.freeTime,
    #                         "freeDistance": first_card.freeDistance,
    #                         "freeMoney": first_card.freeMoney
    #                         }
    #         else:
    #             return {"freeTime": 0,
    #                     "freeDistance": 0,
    #                     "freeMoney": 0
    #                     }
    #     # 如果是走老卡
    #     elif not user_use_new or reverting:
    #         my_card = dao_session.session.tenant_db().query(XcEbike2RidingCard).filter(
    #             XcEbike2RidingCard.objectId == object_id,
    #             XcEbike2RidingCard.cardExpiredDate >= datetime.datetime.now()).first()
    #         if my_card:
    #             used_times = int(dao_session.redis_session.r.get(RECE_TIMES_KEY.format(user_id=object_id)) or 0)
    #             content = json.loads(my_card.content)
    #             total_times = int(content.get("receTimes", 0))
    #             if total_times > used_times:
    #                 user_free_time = float(content["freeTime"]) * 3600
    #                 return {"freeTime": user_free_time,
    #                         "freeDistance": 0,
    #                         "freeMoney": 0
    #                         }
    #
    #         # 老的次卡方式
    #         my_count_card = dao_session.session.tenant_db().query(XcEbike2RidingCountCard).filter(
    #             XcEbike2RidingCountCard.objectId == object_id,
    #             XcEbike2RidingCountCard.state == UserRidingCardState.USING.value,
    #             XcEbike2RidingCountCard.cardExpiredDate >= datetime.datetime.now(),
    #             XcEbike2RidingCountCard.usedreceTimes < XcEbike2RidingCountCard.receTimes
    #         ).order_by(XcEbike2RidingCountCard.cardExpiredDate.asc()).first()
    #         if my_count_card:
    #             content = json.loads(my_count_card.content)
    #             total_times = my_count_card.receTimes
    #             used_times = my_count_card.usedreceTimes
    #             if total_times > used_times:
    #                 user_free_time = content.get("freeTimeseconds", 0) or float(content.get("freeTime", 0)) * 3600
    #                 return {"freeTime": user_free_time,
    #                         "freeDistance": 0,
    #                         "freeMoney": 0
    #                         }
    #     return {"freeTime": 0,
    #             "freeDistance": 0,
    #             "freeMoney": 0
    #             }
    pass