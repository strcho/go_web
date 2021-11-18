import json
import time
from model.all_model import *
from mbutils import dao_session
from mbutils import logger
from utils.constant.redis_key import CONFIG_ROUTER_SERVICE_KEY, CONFIG_ROUTER_KEY, COST_CHANGE_NOTIFY
from . import MBService
from utils.constant.config import ConfigName
from utils.constant.account import SERIAL_TYPE
from sqlalchemy import func


class FavorableCardService(MBService):

    def query_one(self, valid_data):
        """
        获取用户的优惠卡
        """
        # pin_id, _ = valid_data
        # params = {"pin_id": pin_id}
        # try:
        #     card = dao_session.session.tenant_db().query(TFavorableCard).filter_by(**params).first()
        # except Exception as e:
        #     dao_session.session.tenant_db().rollback()
        #     logger.error("query user favorable_card is error: {}".format(e))
        #     logger.exception(e)
        # print(card)
        return 'card'

    def insert_one(self, valid_data):
        """
        插入一条优惠卡信息
        """
        service_id, card_name, original_price, present_price, card_time, card_img, config_id = valid_data
        params = {
            "service_id": service_id,
            "card_name": card_name,
            "original_price": original_price * 100,
            "present_price": present_price * 100,
            "card_time": card_time,
            "card_img": card_img,
            "config_id": config_id,
            "card_type": 0,
            "enable": 0,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        params = self.remove_empty_param(params)
        card = TFavorableCard(**params)
        dao_session.session.tenant_db().add(card)
        try:
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
            logger.error("add favorable card is error:", service_id, card_name)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    def update_one_disable(self, valid_data):
        card_id, disable = valid_data
        dao_session.session.tenant_db().query(XcMieba2FavorableCard). \
            filter(XcMieba2FavorableCard.id == card_id).update({XcMieba2FavorableCard.enable: disable,
                                                                XcMieba2FavorableCard.updated_at: datetime.now()})
        try:
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
            logger.error("update favorable card enable is error:", card_id)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    def query_list(self, valid_data, enable=2):
        service_id, page, size = valid_data
        card = dao_session.session.tenant_db().query(XcMieba2FavorableCard).filter(XcMieba2FavorableCard.service_id == service_id)
        card_count = dao_session.session.tenant_db().query(func.count(XcMieba2FavorableCard.id)). \
            filter(XcMieba2FavorableCard.service_id == service_id)
        if enable == 2:
            card = card.filter(XcMieba2FavorableCard.enable != 2)
            card_count = card_count.filter(XcMieba2FavorableCard.enable != 2)
        else:
            card = card.filter(XcMieba2FavorableCard.enable == enable)
            card_count = card_count.filter(XcMieba2FavorableCard.enable == enable)
        card_list = card.order_by(XcMieba2FavorableCard.created_at.desc()).limit(size).offset(page * size).all()
        rows = []
        try:
            card_count = card_count.scalar()
            rows = [{
                "card_id": first.id,
                "service_id": first.service_id,
                "card_name": first.card_name,
                "creat_time": self.datetime2num(first.created_at),
                "original_price": first.original_price / 100,
                "present_price": first.present_price / 100,
                "card_time": first.card_time,
                "card_img": first.card_img,
                "enable": first.enable
            } for first in card_list]
        except Exception as ex:
            logger.exception(ex)
        return card_count, rows


class FavorableCardAccountService(MBService):

    def query_one(self, valid_data):
        service_id, object_id = valid_data
        card_info = dao_session.session.tenant_db().query(XcMieba2FavorableCard). \
            join(XcMieba2FavorableCardAccount, XcMieba2FavorableCard.id == XcMieba2FavorableCardAccount.card_id). \
            filter(XcMieba2FavorableCardAccount.object_id == object_id,
                   XcMieba2FavorableCardAccount.service_id == service_id).first()
        return card_info.card_img

    def query_one_count(self, valid_data):
        service_id = valid_data
        """
        优惠卡的流水统计
        """
        zero_today, _ = self.get_today_date()
        account = dao_session.session.tenant_db().query(func.count(XcMieba2FavorableCardAccount.price).label("count"),
                                              XcMieba2FavorableCard.present_price, XcMieba2FavorableCard.card_time). \
            join(XcMieba2FavorableCard, XcMieba2FavorableCard.id == XcMieba2FavorableCardAccount.card_id). \
            filter(XcMieba2FavorableCardAccount.price > 0).group_by(XcMieba2FavorableCardAccount.card_id)
        card_price = dao_session.session.tenant_db().query(func.distinct(XcMieba2FavorableCard.present_price))
        if service_id:
            account = account.filter(XcMieba2FavorableCardAccount.service_id.in_(service_id))
            card_price = card_price.filter(XcMieba2FavorableCard.service_id.in_(service_id))
        account = account.group_by(XcMieba2FavorableCardAccount.card_id)
        card_price = card_price.all()
        today_account = account.filter(XcMieba2FavorableCardAccount.created_at >= zero_today).all()
        total_account = account.all()
        today_dict = {"{}".format(today.present_price): today.count for today in today_account}
        total_dict = {"{}".format(total.present_price): total.count for total in total_account}
        today_list = [{
            "money": card[0],
            "num": today_dict.get(str(card[0]), 0)
        } for card in card_price if today_dict.get(str(card[0]), 0)]
        total_list = [{
            "money": card[0],
            "num": total_dict.get(str(card[0]), 0)
        } for card in card_price if total_dict.get(str(card[0]), 0)]
        return today_list, total_list

    def query_one_sum(self, valid_data):
        service_id = valid_data
        """
        优惠卡的流水统计
        """
        zero_today, _ = self.get_today_date()
        account = dao_session.session.tenant_db().query(
            func.ifnull(func.sum(XcMieba2FavorableCardAccount.price), 0).label("price"))
        if service_id:
            account = account.filter(XcMieba2FavorableCardAccount.service_id.in_(service_id))
        today_account = account.filter(XcMieba2FavorableCardAccount.created_at >= zero_today)
        favorable_water = self.get_number_price(account.scalar())
        favorable_water_today = self.get_number_price(today_account.scalar())
        favorable_water_refund = self.get_number_price(
            abs(account.filter(XcMieba2FavorableCardAccount.price < 0).scalar()))
        favorable_water_refund_today = self.get_number_price(
            abs(today_account.filter(XcMieba2FavorableCardAccount.price < 0).scalar()))
        return favorable_water, favorable_water_today, favorable_water_refund, favorable_water_refund_today

    def query_list(self, valid_data):
        """
        根据创建时间倒叙查询，只提供基础查询
        """
        service_id, object_id, page, size = valid_data
        account_info = dao_session.session.tenant_db().query(XcMieba2FavorableCardAccount, XcMieba2FavorableCard). \
            outerjoin(XcMieba2FavorableCard, XcMieba2FavorableCard.id == XcMieba2FavorableCardAccount.card_id). \
            filter(XcMieba2FavorableCardAccount.object_id == object_id,
                   XcMieba2FavorableCardAccount.service_id == service_id).order_by(
            XcMieba2FavorableCardAccount.created_at.desc()).limit(size).offset(page * size).all()
        return account_info

    def query_list_to_user(self, valid_data):
        account_info = self.query_list(valid_data)
        account_list, card_info, card_img = [], {}, ''
        for account, card in account_info:
            if card_img == '' and account.price >= 0:
                card_img = card.card_img
                card_info["card_image"] = card_img
                card_info["card_name"] = card.card_name
                card_info["card_price"] = card.original_price
            account_list.append({
                "buy_time": self.datetime2num(account.created_at),
                "buy_price": account.price * 1 / 100,
                "card_name": card.card_name
            })
        account_dict = {"card_info": card_info, "account_list": account_list}
        return account_dict

    def query_list_platform(self, valid_data):
        account_info = self.query_list(valid_data)
        account_list = []
        for account, card in account_info:
            account_dict = {
                "creat_time": self.datetime2num(account.created_at),
                "buy_price": account.price / 100,
                "card_time": card.card_time,
                "channel": account.channel,
                "trade_no": account.trade_no,
                "iz_found": account.iz_found,
                "serial_type": "用户购买"
            }
            if account.serial_type == SERIAL_TYPE.FAVORABLE_CARD_ADD_PAY.value:
                account_dict["serial_type"] = "用户购买"
            elif account.serial_type == SERIAL_TYPE.FAVORABLE_CARD_REFUND.value:
                account_dict["serial_type"] = "平台退款"
            account_list.append(account_dict)
        return account_list

    def query_list_platform_screen(self, valid_data, service_ids):
        start_time, end_time = valid_data
        s_time, e_time = self.millisecond2datetime(start_time), self.millisecond2datetime(end_time)
        count_list = dao_session.session.tenant_db().query(
            func.ifnull(func.count(XcMieba2FavorableCardAccount.id).label("times"), 0),
            func.ifnull(func.date_format(XcMieba2FavorableCardAccount.created_at, "%Y-%m-%d"), '').label("day"),
            func.ifnull(XcMieba2FavorableCardAccount.price, 0).label("price")
        ).filter(XcMieba2FavorableCardAccount.created_at >= s_time,
                 XcMieba2FavorableCardAccount.created_at <= e_time,
                 XcMieba2FavorableCardAccount.price > 0)
        price_days = dao_session.session.tenant_db().query(XcMieba2FavorableCard)
        if service_ids:
            count_list = count_list.filter(XcMieba2FavorableCardAccount.service_id.in_(service_ids))
            price_days = price_days.filter(XcMieba2FavorableCard.service_id.in_(service_ids))
        count_list = count_list.group_by("day", "price").all()
        price_days = price_days.all()
        price_days_dict = {self.get_number_price(i.present_price): i.card_time for i in price_days if i}
        count_dict, price_set, price_days = {}, set(), {}
        for i, j, z in count_list:
            z = self.get_number_price(z)
            price_set.add(z)
            if j in count_dict.keys():
                count_dict[j][z] = i
            else:
                count_dict[j] = {}
                count_dict[j][z] = i
        return count_dict, price_set, price_days_dict


class FavorableCardUserService(MBService):

    # 获取当前用户的优惠卡剩余天数
    def query_one(self, valid_data):
        service_id, object_id = valid_data
        card_info = None
        try:
            card_info = dao_session.session.tenant_db().query(XcMieba2FavorableCardUser). \
                filter(XcMieba2FavorableCardUser.object_id == object_id,
                       XcMieba2FavorableCardUser.service_id == service_id).first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("show favorable card days is error: {}".format(e))
            logger.exception(e)
        return card_info

    # 获取当前用户的优惠卡剩余天数
    def query_one_day(self, valid_data):
        card_info = self.query_one(valid_data)
        day_time, end_time_str = -1, '-'
        if card_info:
            end_time = card_info.end_time
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M")
            day_time = (end_time - datetime.now()).days
        return day_time + 1 if day_time >= 0 else 0, end_time_str

    # 将押金卡的日期同步到优惠卡
    def insert_into_user_days(self, valid_data):
        service_id, = valid_data
        # 查询该服务区下是否有优惠卡，如果没有优惠卡则返回
        card_id = dao_session.session.tenant_db().query(XcMieba2FavorableCardConfig.id).filter(
            XcMieba2FavorableCardConfig.service_id == service_id).first()
        if not card_id:
            return False, "该服务区下未查询到优惠卡的相关信息，不能进行押金卡数据的迁移"
        # user_id = dao_session.session.tenant_db().query(XcMieba2FavorableCardUser.id).filter(
        #     XcMieba2FavorableCardUser.service_id == service_id).first()
        # if user_id:
        #     return False, "该服务区下已有用户购买优惠卡，不能进行押金卡数据的迁移"
        try:
            rows = dao_session.session.tenant_db().execute(
                """INSERT IGNORE INTO `xc_mieba_2_favorable_card_user`(`service_id` , `config_id` , `object_id` , 
                   `begin_time` , `end_time`, `created_at`,`updated_at` ) 
                   SELECT xs.`serviceId` , {}, xs.`id` , date_format(now(),'%Y-%m-%d %H:%i:%s') as n_date , 
                   xs.`depositCardExpiredDate` as c_date ,
                   date_format(now(),'%Y-%m-%d %H:%i:%s') as n_date, date_format(now(),'%Y-%m-%d %H:%i:%s') as n_date
                   FROM `xc_ebike_usrs_2` as xs 
                   WHERE xs.`serviceId` = {} and xs.`haveDepositCard` = 1 
                   and xs.`depositCardExpiredDate`  >= now();""".format(card_id[0], service_id))
            dao_session.session.tenant_db().commit()
            return True, rows.rowcount
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.exception(ex)
            return False, "执行有误，联系开发人员"


class FavorableCardConfigService(MBService):

    def __init__(self):
        self.favorable_card_config = {
            "allowInNoParkingZone": False,
            "allowOutOfParkingZone": False,
            "allowOutOfServiceZone": False,
            "costCycle": 0,
            "costOfOneDay": 0,
            "costPerMeter": 0,
            "costPerMin": 0,
            "discount": 0,
            "fixedDist": 0,
            "fixedDistCost": 0,
            "fixedTime": 0,
            "fixedTimeCost": 0,
            "largeDistance": 0,
            "smallPenalty": 0,
            "largePenalty": 0,
            "penaltyOfInNoParkingZone": 0,
            "penaltyOfOutOfServiceZone": 0,
            "startDist": 0,
            "startTime": 0,
            "bufferDistance": 0,
            "coefficientOfDifficulty": 0,
        }

    def query_one(self, valid_data, router):
        service_id, config_id = valid_data
        config_info = dao_session.session.tenant_db().query(XcEbike2Config).filter(XcEbike2Config.serviceId == service_id,
                                                                         XcEbike2Config.rootRouter == router).first()
        return config_info

    def query_one_config_cost(self, valid_data):
        router = ConfigName.COST.value
        config_info = self.query_one(valid_data, router)
        if config_info:
            return json.loads(config_info.content), config_info.version
        return self.favorable_card_config, time.time()

    def query_one_config_favorable(self, valid_data):
        router = ConfigName.FAVORABLECARDCOST.value
        config_info = self.query_one(valid_data, router)
        if config_info:
            return json.loads(config_info.content), config_info.version
        return {}, 0

    def query_one_config(self, valid_data):
        service_id, config_id = valid_data
        config_info = dao_session.session.tenant_db().query(XcMieba2FavorableCardConfig). \
            filter(XcMieba2FavorableCardConfig.service_id == service_id).first()
        if config_info:
            return config_info.id
        return 0

    def _dict_data_valid(self, data_dict, service_id):
        favorable_card_config = {
            "allow_in_no_parking": data_dict.get("allowInNoParkingZone", 0),
            "allow_out_of_parking": data_dict.get("allowOutOfParkingZone", 0),
            "allow_out_of_service": data_dict.get("allowOutOfServiceZone", 0),
            "cost_cycle": data_dict.get("costCycle", 0),
            "cost_one_day": data_dict.get("costOfOneDay", 0),
            "cost_per_meter": data_dict.get("costPerMeter", 0),
            "cost_per_min": data_dict.get("costPerMin", 0),
            "discount": data_dict.get("discount", 0),
            "fixed_dist": data_dict.get("fixedDist", 0),
            "fixed_dist_cost": data_dict.get("fixedDistCost", 0),
            "fixed_time": data_dict.get("fixedTime", 0),
            "fixed_time_cost": data_dict.get("fixedTimeCost", 0),
            "large_distance": data_dict.get("largeDistance", 0),
            "small_penalty": data_dict.get("smallPenalty", 0),
            "large_penalty": data_dict.get("largePenalty", 0),
            "penalty_in_no_parking": data_dict.get("penaltyOfInNoParkingZone", 0),
            "penalty_out_of_service": data_dict.get("penaltyOfOutOfServiceZone", 0),
            "start_dist": data_dict.get("startDist", 0),
            "start_time": data_dict.get("startTime", 0),
            "buffer_distance": data_dict.get("bufferDistance", 0),
            "coefficient_of_difficulty": data_dict.get("coefficientOfDifficulty", 0),
            "service_id": service_id,
        }
        return favorable_card_config

    def _valid_data_dict(self, valid_data):
        allow_in_no_parking, allow_out_of_parking, allow_out_of_service, cost_cycle, cost_one_day, cost_per_meter, \
        cost_per_min, discount, fixed_dist, fixed_dist_cost, fixed_time, fixed_time_cost, large_distance, \
        small_penalty, large_penalty, penalty_in_no_parking, penalty_out_of_service, start_dist, start_time, \
        buffer_distance, coefficient_of_difficulty, service_id = valid_data
        favorable_card_config1 = {
            "allow_in_no_parking": allow_in_no_parking,
            "allow_out_of_parking": allow_out_of_parking,
            "allow_out_of_service": allow_out_of_service,
            "cost_cycle": cost_cycle,
            "cost_one_day": cost_one_day,
            "cost_per_meter": cost_per_meter,
            "cost_per_min": cost_per_min,
            "discount": discount,
            "fixed_dist": fixed_dist,
            "fixed_dist_cost": fixed_dist_cost,
            "fixed_time": fixed_time,
            "fixed_time_cost": fixed_time_cost,
            "large_distance": large_distance,
            "small_penalty": small_penalty,
            "large_penalty": large_penalty,
            "penalty_in_no_parking": penalty_in_no_parking,
            "penalty_out_of_service": penalty_out_of_service,
            "start_dist": start_dist,
            "start_time": start_time,
            "buffer_distance": buffer_distance,
            "coefficient_of_difficulty": coefficient_of_difficulty,
            "service_id": service_id,
        }
        favorable_card_config2 = {
            "allowInNoParkingZone": allow_in_no_parking,
            "allowOutOfParkingZone": allow_out_of_parking,
            "allowOutOfServiceZone": allow_out_of_service,
            "costCycle": cost_cycle,
            "costOfOneDay": cost_one_day,
            "costPerMeter": cost_per_meter,
            "costPerMin": cost_per_min,
            "discount": discount,
            "fixedDist": fixed_dist,
            "fixedDistCost": fixed_dist_cost,
            "fixedTime": fixed_time,
            "fixedTimeCost": fixed_time_cost,
            "largeDistance": large_distance,
            "smallPenalty": small_penalty,
            "largePenalty": large_penalty,
            "penaltyOfInNoParkingZone": penalty_in_no_parking,
            "penaltyOfOutOfServiceZone": penalty_out_of_service,
            "startDist": start_dist,
            "startTime": start_time,
            "bufferDistance": buffer_distance,
            "coefficientOfDifficulty": coefficient_of_difficulty,
        }
        return favorable_card_config1, favorable_card_config2

    def insert_one(self, valid_data, favorable_config=None):
        config_data, service_id = valid_data
        if favorable_config is None:
            favorable_card_config, _ = self._valid_data_dict(config_data)
        else:
            favorable_card_config = self._dict_data_valid(favorable_config, service_id)
        favorable_card_config["created_at"] = datetime.now()
        favorable_card_config["updated_at"] = datetime.now()
        params = self.remove_empty_param(favorable_card_config)
        card = XcMieba2FavorableCardConfig(**params)
        dao_session.session.tenant_db().add(card)
        try:
            dao_session.session.tenant_db().commit()
            dao_session.redis_session.r.delete(COST_CHANGE_NOTIFY.format(**{"service_id":service_id}))
            return card.id
        except Exception as e:
            logger.error("add favorable card config is error:", service_id)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    def insert_one_config(self, valid_data, favorable_card_config=None):
        config_data, service_id = valid_data
        if favorable_card_config is None and config_data:
            _, favorable_card_config = self._valid_data_dict(config_data)
        version = time.time()
        router = ConfigName.FAVORABLECARDCOST.value
        params = {
            "rootRouter": router,
            "content": json.dumps(favorable_card_config),
            "version": version,
            "serviceId": service_id,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
        params = self.remove_empty_param(params)
        config = XcEbike2Config(**params)
        dao_session.session.tenant_db().add(config)
        redis_dict = {"content": json.dumps(favorable_card_config), "version": str(version)}
        if service_id:
            dao_session.redis_session.r.hmset(CONFIG_ROUTER_SERVICE_KEY.format(
                            router=router, serviceId=service_id), redis_dict)
            dao_session.redis_session.r.delete(COST_CHANGE_NOTIFY.format(**{"service_id": service_id}))

        else:
            dao_session.redis_session.r.hmset(CONFIG_ROUTER_KEY.format(router=router), redis_dict)
        try:
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
            logger.error("add favorable card config is error:", service_id)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    def update_one(self, config_id, valid_data):
        config_data, service_id = valid_data
        favorable_card_config, _ = self._valid_data_dict(config_data)
        favorable_card_config["updated_at"] = datetime.now()
        params = self.remove_empty_param(favorable_card_config)
        dao_session.session.tenant_db().query(XcMieba2FavorableCardConfig). \
            filter(XcMieba2FavorableCardConfig.id == config_id).update(params)
        try:
            dao_session.session.tenant_db().commit()
            dao_session.redis_session.r.delete(COST_CHANGE_NOTIFY.format(**{"service_id":service_id}))

            return True
        except Exception as e:
            logger.error("update favorable card config is error:", service_id)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    def update_one_config(self, valid_data):
        config_data, service_id = valid_data
        _, favorable_card_config = self._valid_data_dict(config_data)
        version = time.time()
        router = ConfigName.FAVORABLECARDCOST.value
        params = {
            "content": json.dumps(favorable_card_config),
            "version": version,
            "updatedAt": datetime.now()
        }
        params = self.remove_empty_param(params)
        dao_session.session.tenant_db().query(XcEbike2Config).filter(XcEbike2Config.serviceId == service_id,
                                                           XcEbike2Config.rootRouter == router).update(params)
        try:
            dao_session.session.tenant_db().commit()
            redis_dict = {"content": json.dumps(favorable_card_config), "version": version}
            if service_id:
                dao_session.redis_session.r.hmset(CONFIG_ROUTER_SERVICE_KEY.format(
                    router=router, serviceId=service_id), redis_dict)
                dao_session.redis_session.r.delete(COST_CHANGE_NOTIFY.format(**{"service_id":service_id}))

            else:
                dao_session.redis_session.r.hmset(CONFIG_ROUTER_KEY.format(router=router), redis_dict)
            return True
        except Exception as e:
            logger.error("update favorable card config is error:", service_id)
            logger.exception(e)
            dao_session.session.tenant_db().rollback()
            return False

    # 计费配置添加（第一次查询）
    def insert_one_select(self, valid_data):
        favorable_card_config = {
            "allowInNoParkingZone": False,
            "allowOutOfParkingZone": False,
            "allowOutOfServiceZone": False,
            "costCycle": 0,
            "costOfOneDay": 0,
            "costPerMeter": 0,
            "costPerMin": 0,
            "discount": 0,
            "fixedDist": 0,
            "fixedDistCost": 0,
            "fixedTime": 0,
            "fixedTimeCost": 0,
            "largeDistance": 0,
            "smallPenalty": 0,
            "largePenalty": 0,
            "penaltyOfInNoParkingZone": 0,
            "penaltyOfOutOfServiceZone": 0,
            "startDist": 0,
            "startTime": 0,
            "bufferDistance": 0,
            "coefficientOfDifficulty": 0,
        }
        favorable_router = ConfigName.FAVORABLECARDCOST.value
        router = ConfigName.COST.value
        favorable_config = self.query_one(valid_data, favorable_router)
        if favorable_config:
            return json.loads(favorable_config.content), favorable_config.version
        else:
            first_config = self.query_one(valid_data, router)
            if first_config:
                return json.loads(first_config.content), first_config.version
            else:
                return favorable_card_config, time.time()
