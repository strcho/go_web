import json
import time
from datetime import (
    datetime,
    timedelta,
)

from internal.marketing_api import MarketingApi
from internal.user_apis import (
    internal_deposited_card_state_change,
    UserApi,
)
from mbshort.str_and_datetime import datetime_filter
from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TDepositCard
from service import MBService
from service.kafka import PayKey
from service.kafka.producer import KafkaClient
from utils.constant.account import UserDepositCardOperate


class DepositCardService(MBService):
    """
    押金卡（会员卡）
    """

    def query_one(self, args: dict):
        """
        查询一张押金卡
        """

        try:
            pin = args['pin']
            filter_param = [
                TDepositCard.tenant_id == args['commandContext']['tenantId'],
                TDepositCard.pin == pin,
            ]
            # if "service_id" in args:
            #     filter_param.append(TDepositCard.service_id == args['service_id'])
            deposit_card: TDepositCard = (
                dao_session.session.tenant_db().query(TDepositCard).filter(
                    *filter_param
                ).first()
            )
            if deposit_card:
                deposit_card.content = json.loads(deposit_card.content) if deposit_card.content else {}
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('查询用户押金卡失败')

        return deposit_card

    def insert_one(self, args: dict, card_content):
        """
        插入一张押金卡
        """

        try:
            params = self.get_model_common_field(args['commandContext'])
            params.update({
                "pin": args['pin'],
                "config_id": args['config_id'],
                # "money": args['money'],
                # "channel": args['channel'],
                # "days": args['days'],
                # "trade_no": args['trade_no'],
                "content": json.dumps(card_content),
                "service_id": args['service_id'],
                "expired_date": datetime.now() + timedelta(days=args['duration']),
            })
            deposit_card = TDepositCard(**params)
            dao_session.session.tenant_db().add(deposit_card)
            dao_session.session.tenant_db().commit()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('用户获取押金卡失败')

        return deposit_card

    def update_one(self, args: dict):
        """
        更新一张押金卡
        """
        pass

    def send_deposit_card(self, args: dict):
        """
        向用户发送一张押金卡
        """

        try:
            commandContext = args.get("commandContext")
            config_id = args.get("config_id")

            deposit_card_info = MarketingApi.get_deposit_card_info(config_id=config_id, command_context=commandContext)
            name = deposit_card_info.get("name")
            card_service_id = deposit_card_info.get("service_id")
            amount = deposit_card_info.get("discount_money")
            card_time = deposit_card_info.get("card_duration_day")
            args["service_id"] = card_service_id
            args["duration"] = card_time
            deposit_card: TDepositCard = self.query_one(args)
            # 有卡则更新卡过期时间
            if not deposit_card:
                deposit_card = self.insert_one(args, deposit_card_info)
            else:
                days = args['duration']
                if deposit_card.expired_date < datetime.now():
                    expired_date = datetime.now() + timedelta(days=days)
                else:
                    expired_date = deposit_card.expired_date + timedelta(days=days)
                deposit_card.expired_date = expired_date
                deposit_card.content = json.dumps(deposit_card_info),
                dao_session.session.tenant_db().commit()

            # 向用户系统推送押金卡状态
            internal_deposited_card_state_change({
                'pin': commandContext['pin'],
                'commandContext': commandContext,
                'depositCardOperate': UserDepositCardOperate.Buy.value,
                'depositCardExpire': datetime_filter(deposit_card.expired_date),
            })

            user_info = UserApi.get_user_info(pin=args["pin"], command_context=commandContext)
            # user_service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            deposit_card_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("pin"),
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get('pin'),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": card_service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": amount,
                "paid_at": args.get("paid_at") or int(time.time()),

                "config_id": config_id,
                "name": name,
                "duration": card_time,
            }
            deposit_card_dict = self.remove_empty_param(deposit_card_dict)
            logger.info(f"deposit_card_record send is {deposit_card_dict}")
            KafkaClient().visual_send(deposit_card_dict, PayKey.DEPOSIT_CARD.value)

            try:
                if args.get(type) == 1:  # 充值购买
                    MarketingApi.buy_deposit_card_judgement(pin=args.get("pin"), service_id=card_service_id,
                                                            buy_time=args.get("paid_at") or int(time.time()),
                                                            command_context=commandContext)
            except Exception as e:
                logger.error(f"营销活动回调失败 buy_deposit_card_judgement： {e}")

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('用户获取押金卡失败')

        return True if deposit_card else False

    def modify_deposit_card_time(self, args: dict) -> TDepositCard:
        """
        更新押金卡时限
        """

        try:
            deposit_card: TDepositCard = self.query_one(args)
            if not deposit_card:
                raise MbException('用户没有押金卡')

            # 可用时长设置为 0
            days = args['duration']
            if days == 0:
                expired_date = datetime.now()
            else:
                expired_date = datetime.now() + timedelta(days=days)

            deposit_card.expired_date = expired_date
            dao_session.session.tenant_db().commit()

            # 向用户系统推送押金卡状态
            internal_deposited_card_state_change({
                'pin': args['commandContext']['pin'],
                'depositCardOperate': UserDepositCardOperate.ModifyTime.value,
                'commandContext': args['commandContext'],
                'depositCardExpire': datetime_filter(expired_date),
            })

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('更新用户押金卡时长失败')
        return deposit_card

    def query_one_day(self, args):
        """
        获取当前用户的押金卡剩余天数
        """

        card_info: TDepositCard = self.query_one(args)
        day_time, expired_date_str = -1, '-'
        if card_info:
            expired_date = card_info.expired_date
            expired_date_str = expired_date.strftime("%Y-%m-%d %H:%M")
            day_time = (expired_date - datetime.now()).days
        data = {
            'days': day_time + 1 if day_time >= 0 else 0,
            'expired_date_str': expired_date_str
        }

        return data

    def refund_deposit_card(self, args):

        commandContext = args.get("commandContext")
        config_id = args.get("config_id")

        user_info = UserApi.get_user_info(pin=args["pin"], command_context=commandContext)
        pin_phone = user_info.get("phone")
        pin_name = user_info.get("authName")

        deposit_card_info = MarketingApi.get_deposit_card_info(config_id=config_id, command_context=commandContext)
        name = deposit_card_info.get("name")
        card_service_id = deposit_card_info.get("service_id")
        amount = deposit_card_info.get("discount_money")
        card_time = deposit_card_info.get("card_duration_day")

        deposit_card_dict = {
            "tenant_id": commandContext.get('tenantId'),
            "created_pin": commandContext.get("pin"),
            "version": commandContext.get("version", 0),
            "updated_pin": commandContext.get('pin'),

            "pin_id": args.get("pin"),
            "pin_phone": pin_phone,
            "pin_name": pin_name,
            "service_id": card_service_id,
            "type": args.get("type"),
            "channel": args.get("channel"),
            "sys_trade_no": args.get("sys_trade_no"),
            "merchant_trade_no": args.get("merchant_trade_no"),
            "amount": args.get("amount") if args.get("amount") is not None else amount,
            "paid_at": args.get("paid_at") or int(time.time()),

            "config_id": config_id,
            "name": name,
            "duration": card_time,
        }
        deposit_card_dict = self.remove_empty_param(deposit_card_dict)
        logger.info(f"deposit_card_record send is {deposit_card_dict}")
        KafkaClient().visual_send(deposit_card_dict, PayKey.DEPOSIT_CARD.value)

