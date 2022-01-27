import json
from datetime import (
    datetime,
    timedelta,
)

from internal import user_apis
from internal.user_apis import internal_deposited_card_state_change
from mbshort.str_and_datetime import datetime_filter
from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TDepositCard
from service import MBService
from service.kafka import PayKey


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
            deposit_card: TDepositCard = (
                dao_session.session.tenant_db().query(TDepositCard).filter(
                    TDepositCard.tenant_id == args['commandContext']['tenantId'],
                    TDepositCard.pin == pin,
                ).first()
            )
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('查询用户押金卡失败')

        return deposit_card

    def insert_one(self, args: dict):
        """
        插入一张押金卡
        """

        try:
            params = self.get_model_common_field(args['commandContext'])
            params.update({
                "pin": args['pin'],
                # "config_id": args['config_id'],
                # "money": args['money'],
                # "channel": args['channel'],
                # "days": args['days'],
                # "trade_no": args['trade_no'],
                # "content": args['content'],
                # "service_id": args['service_id'],
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
            deposit_card: TDepositCard = self.query_one(args)
            # 有卡则更新卡过期时间
            if deposit_card:
                self.modify_deposit_card_time(args)
            else:
                deposit_card = self.insert_one(args)

                # 向用户系统推送押金卡状态
                internal_deposited_card_state_change({
                    'pin': args['commandContext']['pin'],
                    # 'depositCardOperate': ,  # todo
                    'commandContext': args['commandContext'],
                    'depositCardExpire': datetime_filter(datetime.now() + timedelta(days=args['duration'])),
                })

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('用户获取押金卡失败')

        return True if deposit_card else False

    def modify_deposit_card_time(self, args: dict):
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
                # 'depositCardOperate': ,  # todo
                'commandContext': args['commandContext'],
                'depositCardExpire': datetime_filter(expired_date),
            })

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('更新用户押金卡时长失败')

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

    @staticmethod
    def deposit_card_to_kafka(context, args: dict):
        # todo 根据用户id查询服务区id，
        try:
            commandContext = args.get("commandContext")
            param = {"pin": args.get("pin"), 'commandContext': commandContext}
            user_res = user_apis.internal_get_userinfo_by_id(param)
            user_info = json.loads(user_res).get("data")
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")
        except Exception as e:
            # service_id获取失败暂不报错
            logger.info(f"user_apis err: {e}")
            return {"suc": False, "data": "用户信息获取失败"}
            # service_id = 61193175763522450
            # pin_phone = ''
            # pin_name = ''
        try:
            deposit_card_dict = {
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
                "pin_phone": pin_phone,
                "pin_name": pin_name
            }
            logger.info(f"deposit_card_record send is {deposit_card_dict}")
            state = KafkaClient.visual_send(deposit_card_dict, PayKey.DEPOSIT_CARD.value)
            if not state:
                return {"suc": False, "data": "kafka send failed"}
        except Exception as e:
            logger.info(f"deposit_card_record send err {e}")
            return {"suc": False, "data": f"deposit_card_to_kafka err: {e}"}
        return {"suc": True, "data": "deposit_card_kafka send success"}



