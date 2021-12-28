from datetime import (
    datetime,
    timedelta,
)

from internal import user_apis
from internal.user_apis import internal_get_userinfo_by_id
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
from .kafka.producer import kafka_client


class FavorableCardUserService(MBService):
    """
    用户骑行卡
    """

    def query_one(self, args: dict) -> TFavorableCard:
        """
        获取当前用户的优惠卡
        """
        pin = args['pin']
        card_info = None
        try:
            card_info = dao_session.session.tenant_db().query(TFavorableCard). \
                filter(TFavorableCard.pin == pin).first()
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
        commandContext = args['commandContext']
        param = self.get_model_common_field(commandContext)
        param.update({
            "pin": args['pin'],
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
                dao_session.session.tenant_db().commit()
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
                {"tenant_id": args['commandContext']['tenant_id'],
                 "pin": args['commandContext']['pin']}
        )):
            raise MbException("修改用户优惠卡时间中,请2s后重试")

        self.user_can_modify_favorable_card_duration(args['commandContext']['pin'], )

        duration = args['duration']

        riding_card: TFavorableCard = self.query_one(args)
        if not riding_card:
            raise MbException("未找到优惠卡")

        try:
            if self.exists_param(duration):
                if duration == 0:
                    riding_card.end_time = datetime.now()
                else:
                    riding_card.end_time = datetime.now() + timedelta(days=duration)
            dao_session.session.tenant_db().commit()

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("modify user favorable card is error: {}".format(ex))
            logger.exception(ex)
            return False
        finally:
            release_lock(EDIT_USER_FAVORABLE_CARD_LOCK.format(
                {"tenant_id": args['commandContext']['tenant_id'],
                 "pin": args['commandContext']['pin']}))

        return True

    def user_can_modify_favorable_card_duration(self, pin: str, service_id: int):
        """
        判断当前用户能否修改优惠卡时长,用户购卡信息表,非流水表
        """

        user_info = internal_get_userinfo_by_id({})  # todo 获取用户信息
        user_state = user_info.get('userState')
        if not user_info:
            raise MbException("获取用户信息失败")
        if user_state == UserState.SIGN_UP.value:
            raise MbException("用户没有实名,无法进行退款")
        if user_state in [UserState.LEAVING.value, UserState.RIDING.value]:
            raise MbException("用户使用车辆中,请不要进行退优惠卡操作")
        if user_state == UserState.TO_PAY.value:
            raise MbException("用户有未完结的订单,完成支付后才能进行退款操作")

        # todo 这边到底要不要记流水啊？!
        favorable_buy_record = internal_get_userinfo_by_id({})  # 获取用户的购卡记录
        if not favorable_buy_record:
            raise MbException("用户没有优惠卡购买记录,无法进行退款")

        return True

    @staticmethod
    def favorable_card_to_kafka(context, args: dict):
        # todo 根据用户id查询服务区id，
        try:
            user_info = user_apis.internal_get_userinfo_by_id({"user_id": args.get("pin_id")})
            service_id = user_info.get('service_id')
        except Exception as e:
            # service_id获取失败暂不报错
            logger.info(f"user_apis err: {e}")
            service_id = 61193175763522450

        try:
            favorable_card_dict = {
                "tenant_id": context.get('tenant_id'),
                "created_pin": args.get("created_pin"),
                "pin_id": args.get("pin_id"),
                "service_id": service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "name": "deposit",
                "amount": args.get("amount"),
            }
            logger.info(f"deposit_card_record send is {favorable_card_dict}")
            state = kafka_client.pay_send(favorable_card_dict, PayKey.FAVORABLE_CARD.value)
            if not state:
                return {"suc": False, "data": "kafka send failed"}
        except Exception as e:
            logger.info(f"favorable_card_record send err {e}")
            return {"suc": False, "data": f"favorable_card_to_kafka err: {e}"}
        return {"suc": True, "data": "favorable_kafka send success"}
