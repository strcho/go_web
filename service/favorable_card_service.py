import json
import time
from datetime import (
    datetime,
    timedelta,
)

from internal.marketing_api import MarketingApi
from internal.user_apis import (
    internal_get_userinfo_by_id,
    UserApi,
)
from mbshort.str_and_datetime import datetime_filter
from mbutils import (
    dao_session,
    MbException,
)
from mbutils import logger
from model.all_model import TFavorableCard
from utils.constant.redis_key import EDIT_USER_FAVORABLE_CARD_LOCK
from utils.constant.user import UserState
from utils.redis_lock import (
    lock,
    release_lock,
)
from . import MBService
from .kafka import PayKey
from .kafka.producer import KafkaClient


class FavorableCardUserService(MBService):
    """
    用户优惠卡
    """

    def query_one(self, args: dict) -> TFavorableCard:
        """
        获取当前用户在某服务区的优惠卡
        """
        pin = args['pin']
        card_info = None
        try:
            card_info = dao_session.session.tenant_db().query(TFavorableCard). \
                filter(TFavorableCard.pin == pin, TFavorableCard.service_id == args['service_id']).first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("show favorable card days is error: {}".format(e))
            logger.exception(e)
        return card_info

    # 获取当前用户的优惠卡剩余天数
    def query_one_day(self, args):
        card_info = self.query_one(args)
        day_time, end_time_str = -1, '-'
        if card_info:
            end_time = card_info.end_time
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M")
            day_time = (end_time - datetime.now()).days

        data = {
            'days': day_time + 1 if day_time >= 0 else 0,
            'expired_date_str': end_time_str
        }

        return data

    # 插入一张优惠卡
    def insert_one(self, args):

        commandContext = args.get("commandContext")
        # params = {"pin": args.get("pin"), 'commandContext': commandContext}
        # user_res = user_apis.internal_get_userinfo_by_id(params)
        # user_info = json.loads(user_res).get("data")
        # service_id = user_info.get('serviceId')
        # pin_phone = user_info.get("phone")
        # pin_name = user_info.get("authName")

        card_content = MarketingApi.get_favorable_card_info(config_id=args["config_id"], command_context=commandContext)

        param = self.get_model_common_field(commandContext)
        param.update({
            "pin": args['pin'],
            "config_id": args['config_id'],
            "service_id": args['service_id'],
            "content": json.dumps(card_content),
            "begin_time": datetime.now(),
            "end_time": datetime.now() + timedelta(days=args["card_time"]),
        })
        try:
            user_card = TFavorableCard(**param)
            dao_session.session.tenant_db().add(user_card)
            dao_session.session.tenant_db().commit()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user favorable card is error: {}".format(e))
            logger.exception(e)
            return False
        return True

    def send_user_favorable_card(self, args: dict):
        """
        添加用户优惠卡
        """
        try:
            config_id = args.get("config_id")
            commandContext = args.get("commandContext")
            user_card: TFavorableCard = self.query_one(args)
            if not user_card:
                res = self.insert_one(args)
            else:
                # 用户优惠卡已过期，则从当前时间开始计算过期时间
                if user_card.end_time < datetime.now():
                    user_card.end_time = datetime.now() + timedelta(days=args["card_time"])
                # 用户优惠卡未过期，累计优惠卡使用时间
                else:
                    user_card.end_time += timedelta(days=args["card_time"])

                # 更新卡
                user_card.config_id = config_id
                card_content = MarketingApi.get_favorable_card_info(config_id=args["config_id"],
                                                                    command_context=commandContext)
                user_card.content = json.dumps(card_content)
                dao_session.session.tenant_db().commit()

            user_info = UserApi.get_user_info(pin=args["pin"], command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            favorable_card_info = MarketingApi.get_favorable_card_info(config_id=config_id, command_context=commandContext)
            name = favorable_card_info.get("card_name")
            amount = favorable_card_info.get("present_price")
            card_time = favorable_card_info.get("card_time")
            favorable_card_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("pin"),
                "version": commandContext.get("version", ""),
                "updated_pin": commandContext.get('pin'),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": amount,
                "paid_at": args.get("paid_at") or int(time.time()),

                "name": name,
                "duration": card_time,
            }
            logger.info(f"favorable_card_record send is {favorable_card_dict}")
            KafkaClient().visual_send(favorable_card_dict, PayKey.FAVORABLE_CARD.value)
            res = True
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("send user favorable card is error: {}".format(ex))
            logger.exception(ex)
            res = False

        return res

    def modify_time(self, args: dict):
        """
        编辑用户优惠卡时间
        """

        if not lock(EDIT_USER_FAVORABLE_CARD_LOCK.format(
                **{"tenant_id": args['commandContext']['tenantId'],
                    "pin": args['pin'],
                    "service_id": args["service_id"]})):
            raise MbException("修改用户优惠卡时间中,请2s后重试")

        self.user_can_modify_favorable_card_duration(args['commandContext'], args['pin'], args['service_id'])

        duration = args['duration']
        favorable_card: TFavorableCard = self.query_one(args)
        if not favorable_card:
            raise MbException("未找到优惠卡")

        try:
            if self.exists_param(duration):
                if duration == 0:
                    favorable_card.end_time = datetime.now()
                else:
                    favorable_card.end_time = datetime.now() + timedelta(days=duration)
            dao_session.session.tenant_db().commit()

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("modify user favorable card is error: {}".format(ex))
            logger.exception(ex)
            return False
        finally:
            release_lock(EDIT_USER_FAVORABLE_CARD_LOCK.format(**{
                "tenant_id": args['commandContext']['tenantId'],
                "pin": args['commandContext']['pin'],
                "service_id": args["service_id"]}))

        return True

    def user_can_modify_favorable_card_duration(self, command_context: dict, pin: str, service_id: int):
        """
        判断当前用户能否修改优惠卡时长,用户购卡信息表,非流水表
        """
        param = {"pin": pin, 'commandContext': command_context}
        user_res = internal_get_userinfo_by_id(param)
        user_res_data = json.loads(user_res)
        if not user_res_data.get("success"):
            raise MbException("用户服务调用失败")

        user_info = user_res_data.get('data')
        user_state = user_info.get('userState')
        if not user_info:
            raise MbException("获取用户信息失败")
        if user_state == UserState.SIGN_UP.value:
            raise MbException("用户没有实名,无法进行退款")
        if user_state in [UserState.LEAVING.value, UserState.RIDING.value]:
            raise MbException("用户使用车辆中,请不要进行退优惠卡操作")
        if user_state == UserState.TO_PAY.value:
            raise MbException("用户有未完结的订单,完成支付后才能进行退款操作")

        # # todo 这边到底要不要记流水啊？!
        # favorable_buy_record = internal_get_userinfo_by_id({})  # 获取用户的购卡记录
        # if not favorable_buy_record:
        #     raise MbException("用户没有优惠卡购买记录,无法进行退款")

        return True

    def get_user_card_list(self, args,):

        pin = args['pin']
        card_data = {"used": [], "expired": []}
        try:
            card_list = dao_session.session.tenant_db().query(TFavorableCard). \
                filter(TFavorableCard.pin == pin).all()
            for card in card_list:
                card: TFavorableCard = card

                card_dict = dict(
                    id=card.id,
                    pin=card.pin,
                    config_id=card.config_id,
                    service_id=card.service_id,
                    begin_time=datetime_filter(card.begin_time),
                    end_time=datetime_filter(card.end_time),
                    content=json.loads(card.content),
                )

                if card.end_time > datetime.now():

                    card_data["used"].append(card_dict)
                else:
                    card_data["expired"].append(card_dict)

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("show favorable card days is error: {}".format(e))
            logger.exception(e)
        return card_data

    def get_card_info(self, config_id: int, args):
        """
        获取卡的详细信息
        """

        card_info = MarketingApi.get_favorable_card_info(config_id=config_id, command_context=args.get("commandContext"))
        return card_info

