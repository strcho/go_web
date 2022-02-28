import base64
import json
import time
from datetime import (
    datetime,
    timedelta,
)
from operator import and_

from sqlalchemy import or_

from internal import (
    user_apis,
    marketing_api,
)
from internal.marketing_api import MarketingApi
from internal.user_apis import UserApi
from mbutils import (
    dao_session,
    logger,
    MbException,
)

from model.all_model import (
    TRidingCard,
)
from service import MBService
from service.kafka import PayKey
from service.kafka.producer import KafkaClient
from utils.constant.account import (
    UserRidingCardState,
    SERIAL_TYPE,
)
from utils.constant.redis_key import (
    ALL_USER_LAST_SERVICE_ID,
)


class RidingCardService(MBService):
    """
    骑行卡
    """

    def query_one(self, card_id: str, tenant_id: str = None):
        """
        通过id查询一张骑行卡
        """
        try:
            filter_param = [TRidingCard.id == card_id]
            if tenant_id is not None:
                filter_param.append(TRidingCard.tenant_id == tenant_id)
            riding_card = dao_session.session.tenant_db().query(TRidingCard) \
                .filter(*filter_param).first()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
            riding_card = None
        return riding_card

    def insert_one(self, pin: str, args: dict):
        """
        插入一条骑行卡
        """
        commandContext = args['commandContext']
        data = self.get_model_common_field(commandContext)

        data['pin'] = pin
        user_riding_card = TRidingCard(**data)
        dao_session.session.tenant_db().add(user_riding_card)
        try:
            print('insert one')
            dao_session.session.tenant_db().commit()
            return user_riding_card

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user riding card is error: {}".format(e))
            logger.exception(e)
            return False

    def modify_time(self, args: dict):

        card_id = args.get('card_id')
        remain_times = args.get('remain_times')
        duration = args.get('duration')
        _args = (card_id,)
        tenant_id = args['commandContext']['tenantId']
        if self.exists_param(tenant_id):
            _args = (card_id, tenant_id)

        riding_card: TRidingCard = self.query_one(*_args)
        if not riding_card:
            raise MbException("无效的骑行卡")

        try:
            if self.exists_param(duration):
                riding_card.card_expired_date = datetime.now() + timedelta(days=duration)
            if self.exists_param(remain_times):
                riding_card.remain_times = remain_times
            riding_card.updated_at = datetime.now()
            dao_session.session.tenant_db().commit()
            return "ok"
        except Exception:
            raise MbException("修改骑行卡时长失败")

    def user_card_info(self, args):
        """
        获取用户骑行卡信息
        """
        pin = args['pin']
        try:
            dao_session.session.tenant_db().query(TRidingCard).filter(
                TRidingCard.state == UserRidingCardState.USING.value,
                TRidingCard.pin == pin,
                TRidingCard.card_expired_date <= datetime.now()).update(
                {"state": UserRidingCardState.EXPIRED.value})
            dao_session.session.tenant_db().commit()
        except Exception as ex:
            print(ex)
        user_info = UserApi.get_user_info(pin=pin, command_context=args.get("commandContext"))
        service_id = user_info.get('serviceId')
        return self.query_my_list_in_platform(service_id, pin)

    def get_user_card_list(self, args):
        """
        获取用户骑行卡列表
        """
        pin = args['pin']
        try:
            # 更新骑行卡过期时间
            dao_session.session.tenant_db().query(TRidingCard).filter(
                TRidingCard.state == UserRidingCardState.USING.value,
                TRidingCard.pin == pin,
                TRidingCard.card_expired_date <= datetime.now()).update(
                {"state": UserRidingCardState.EXPIRED.value})
            dao_session.session.tenant_db().commit()
        except Exception:
            pass
        user_info = UserApi.get_user_info(pin=pin, command_context=args.get("commandContext"))
        service_id = user_info.get('serviceId')
        return self.query_my_list_in_platform(service_id, pin)

    def query_my_list_in_platform(self, service_id: int, pin: str) -> dict:
        """
        :return:
    {
        "used":[
            {
                "card_id":10001,
                "name":"7天酷骑卡",
                "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
                "description_tag":"仅限武汉地区;通勤",
                "promotion_tag":"热销",
                "detail_info":"<p>富文本</p>",
                "expired_date":"2021-04-09 11:12:13",
                "remain_times":2
            }
        ],
        "cost_use":10001
    }
        """
        # dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, pin, service_id)

        first_id = self.get_current_card_id(service_id, pin)
        res_dict = {"used": [], "expired": [], "rule_info": "", "cost_use": first_id}
        result = dao_session.session.tenant_db().query(TRidingCard).filter(
            TRidingCard.pin == pin,
            TRidingCard.state <= UserRidingCardState.EXPIRED.value,
            TRidingCard.card_expired_date >= datetime.now() - timedelta(weeks=13)).order_by(
            TRidingCard.created_at.desc()).all()
        for one in result:
            one: TRidingCard = one
            car_info = {"card_id": one.config_id}
            content = json.loads(one.content)
            car_info["id"] = one.id
            car_info["name"] = content["name"]
            car_info["image_url"] = content["image_url"]
            car_info["description_tag"] = content.get("description_tag", "限全国")
            car_info["detail_info"] = content.get("detail_info", "") or str(
                base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                    content["valid_day"],
                    "累计" if one.iz_total_times else "每日",
                    content["available_times"], int(float(content["free_time_second"]))).encode("utf-8")),
                "utf-8")
            car_info["card_expired_date"] = one.card_expired_date
            car_info["remain_times"] = one.remain_times
            car_info["iz_total_times"] = one.iz_total_times
            car_info["rece_times"] = one.rece_times
            car_info["free_time_second"] = one.free_time
            car_info["free_distance_meter"] = one.free_distance
            car_info["free_money_cent"] = one.free_money
            car_info["promotion_tag"] = content.get("promotion_tag", "人气优选")
            car_info["deductionType"] = one.deduction_type
            car_info["effective_service_ids"] = content.get("effective_service_ids")
            car_info["effective_service_names"] = content.get("effective_service_names")
            if one.state != UserRidingCardState.EXPIRED.value:
                res_dict["used"].append(car_info)
            else:
                res_dict['expired'].append(car_info)
        return res_dict

    @staticmethod
    def get_current_card_id(service_id: int, pin: str) -> int:
        """
        该用户的,没有过期的, 使用中的,  union,
        (无次卡的, deductionType越小, 最后一次使用不是今日的, 剩余次数最多的额)(次卡的, 过期时间最近的, 次数够的)
        服务区没有配置或者在配置服务区里面的
        :param service_id:
        :param pin:
        :return:
        """

        # 1.骑行卡过期判定
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date < datetime.now()
        ).update({"state": UserRidingCardState.EXPIRED.value})

        # 2.骑行卡次数重置, 如果上次使用时间不是今天的, 则把非次卡的, 时间和剩余次数重置到最多再计算
        now = datetime.now()
        zeroToday = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,
                                    microseconds=now.microsecond)
        lastToday = zeroToday + timedelta(hours=23, minutes=59, seconds=59)
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.iz_total_times == 0,
            or_(and_(TRidingCard.last_use_time < zeroToday,
                     TRidingCard.last_use_time > lastToday),
                TRidingCard.last_use_time is None),
        ).update(
            {
                "remain_times": TRidingCard.rece_times,
                "last_use_time": datetime.now()
            }
        )
        # 过期当日将非次卡的可用次数置零
        now = datetime.now()
        zeroToday = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
        lastToday = zeroToday + timedelta(hours=23, minutes=59, seconds=59)
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.iz_total_times == 0,
            TRidingCard.card_expired_date >= zeroToday,
            TRidingCard.card_expired_date <= lastToday,
        ).update(
            {
                "remain_times": 0,
                "last_use_time": datetime.now()
            }
        )

        dao_session.session.tenant_db().commit()
        # 3.选出最佳骑行卡id
        many = dao_session.session.tenant_db().query(
            TRidingCard.id,
            TRidingCard.effective_service_ids
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date >= datetime.now(),
            TRidingCard.remain_times > 0
        ).order_by(TRidingCard.deduction_type.asc(), TRidingCard.iz_total_times.asc(),
                   TRidingCard.card_expired_date.asc()).all()
        for _id, effective_service_ids in many:
            if not effective_service_ids or effective_service_ids == "all":
                return _id
            else:
                if str(service_id) in effective_service_ids.split(";"):
                    return _id
        return None

    def send_riding_card(self, args,) -> str:
        """
        添加骑行卡
        """
        pin = args['pin']
        config_id = args['config_id']
        commandContext = args.get("commandContext")

        user_info = UserApi.get_user_info(pin=args["pin"], command_context=commandContext)
        service_id = user_info.get('serviceId')
        pin_phone = user_info.get("phone")
        pin_name = user_info.get("authName")

        card_info = MarketingApi.get_riding_card_info(config_id=config_id, command_context=commandContext)
        name = card_info.get("name")
        valid_day = card_info.get("valid_day")
        current_cost = card_info.get('current_cost')
        iz_total_times = card_info.get("iz_total_times")
        deduction_type = card_info.get("deduction_type")
        free_time_second = card_info.get("free_time_second")
        free_distance_meter = card_info.get("free_distance_meter")
        free_money_cent = card_info.get("free_money_cent")
        available_times = card_info.get("available_times")
        effective_service_ids = card_info.get("effective_service_ids")

        params = self.get_model_common_field(commandContext)

        params.update({
            "pin": pin,
            "deduction_type": deduction_type,
            "config_id": config_id,
            "free_time": free_time_second,
            "free_distance": free_distance_meter,
            "free_money": free_money_cent,
            "iz_total_times": iz_total_times,
            "rece_times": available_times,
            "effective_service_ids": effective_service_ids,
            "remain_times": available_times,
            "last_use_time": None,
            "start_time": datetime.now(),
            "card_expired_date": datetime.now() + timedelta(hours=24 * int(valid_day)),
            "content": json.dumps(card_info),
            "state": UserRidingCardState.USING.value,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "service_id": service_id,
        })
        try:
            riding_card = TRidingCard(**params)
            dao_session.session.tenant_db().add(riding_card)
            dao_session.session.tenant_db().commit()

            riding_card_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("pin"),
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get('pin'),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": current_cost,
                "paid_at": args.get("paid_at") or int(time.time()),

                "config_id": config_id,
                "name": name,
                "duration": valid_day,
            }
            riding_card_dict = self.remove_empty_param(riding_card_dict)
            logger.info(f"riding_card_record send is {riding_card_dict}")
            KafkaClient().visual_send(riding_card_dict, PayKey.RIDING_CARD.value)

            try:
                if args.get(type) == 1:  # 充值购买
                    MarketingApi.buy_riding_card_judgement(pin=args.get("pin"), service_id=service_id,
                                                           buy_time=args.get("paid_at") or int(time.time()),
                                                           command_context=commandContext)
            except Exception as e:
                logger.error(f"营销活动回调失败 buy_riding_card_judgement： {e}")

        except Exception as ex:
            raise MbException("添加超级骑行卡失败")

        return "添加骑行卡成功"

    def add_count(self, args: dict):
        card_id = args['card_id']
        tenant_id = args['commandContext']['tenantId']
        one: TRidingCard = dao_session.session.tenant_db().query(TRidingCard).filter(
            TRidingCard.id == card_id,
            TRidingCard.tenant_id == tenant_id
        ).first()
        if not one:
            raise MbException("无效的骑行卡")
        try:
            one.remain_times -= 1  # 可以减到负数,用于追踪异常的情况
            one.last_use_time = datetime.now()
            one.updated_at = datetime.now()
            dao_session.session.tenant_db().commit()
            return ''
        except Exception:
            dao_session.session.tenant_db().rollback()
            logger.error("骑行卡次数扣除失败,card_id:", card_id)
            raise MbException("骑行卡次数扣除失败")

    def current_during_time(self, args: dict):
        """
        查询当前骑行卡的持续时间
        :return: {"free_time": 0,  #单位秒
                  "free_distance": 0, #单位米
                  "free_money": 0 单位分
                }
        """
        service_id = args['service_id']
        pin = args['pin']
        first_card_id = self.get_current_card_id(service_id=service_id, pin=pin)
        if first_card_id:
            first_card: TRidingCard = dao_session.session.tenant_db().query(TRidingCard).filter_by(id=first_card_id).first()
            if first_card:
                return {"free_time": first_card.free_time,
                        "free_distance": first_card.free_distance,
                        "free_money": first_card.free_money
                        }
        else:
            return {"free_time": 0,
                    "free_distance": 0,
                    "free_money": 0
                    }

    def current_duriong_card(self, args: dict):

        pin = args['pin']
        commandContext = args.get("commandContext")
        user_info = UserApi.get_user_info(pin=args["pin"], command_context=commandContext)
        service_id = user_info.get('serviceId')

        # 1.骑行卡过期判定
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date < datetime.now()
        ).update({"state": UserRidingCardState.EXPIRED.value})

        # 2.骑行卡次数重置, 如果上次使用时间不是今天的, 则把非次卡的, 时间和剩余次数重置到最多再计算
        now = datetime.now()
        zeroToday = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,
                                    microseconds=now.microsecond)
        lastToday = zeroToday + timedelta(hours=23, minutes=59, seconds=59)
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.iz_total_times == 0,
            or_(and_(TRidingCard.last_use_time < zeroToday,
                     TRidingCard.last_use_time > lastToday),
                TRidingCard.last_use_time is None),
        ).update(
            {
                "remain_times": TRidingCard.rece_times,
                "last_use_time": datetime.now()
            }
        )
        dao_session.session.tenant_db().commit()
        # 3.选出最佳骑行卡
        many = dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin == pin,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date >= datetime.now(),
            TRidingCard.remain_times > 0
        ).order_by(TRidingCard.deduction_type.asc(), TRidingCard.iz_total_times.asc(),
                   TRidingCard.card_expired_date.asc()).all()
        card = {}
        for card in many:
            if not card.effective_service_ids or card.effective_service_ids == "all":
                return card
            else:
                if str(service_id) in card.effective_service_ids.split(";"):
                    return card
        return card

    def refund_riding_card(self, args):
        """
        骑行卡退款
        """
        pin = args['pin']
        config_id = args['config_id']
        commandContext = args.get("commandContext")

        user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
        service_id = user_info.get('serviceId')
        pin_phone = user_info.get("phone")
        pin_name = user_info.get("authName")

        card_info = MarketingApi.get_riding_card_info(config_id=config_id, command_context=commandContext)
        name = card_info.get("name")
        valid_day = card_info.get("valid_day")
        current_cost = card_info.get('current_cost')

        try:
            riding_card_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("pin"),
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get('pin'),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": args.get("amount") if args.get("amount") is not None else current_cost,
                "paid_at": args.get("paid_at") or int(time.time()),

                "config_id": config_id,
                "name": name,
                "duration": valid_day,
            }
            riding_card_dict = self.remove_empty_param(riding_card_dict)
            logger.info(f"riding_card_record send is {riding_card_dict}")
            KafkaClient().visual_send(riding_card_dict, PayKey.RIDING_CARD.value)
        except Exception:
            dao_session.session.tenant_db().rollback()
            logger.error("骑行卡退款失败")
            return False

        return True
