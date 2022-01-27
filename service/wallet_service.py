import json

from internal import user_apis
from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TUserWallet
from service import MBService
from service.kafka import (
    PayKey,
    TransactionType,
    ChannelType,
)
from service.kafka.producer import KafkaClient
from utils.constant.redis_key import (
    USER_WALLET_CACHE,
    USER_REFUND_RECHARGE_LOCK,
)


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, args: dict):
        user_wallet = {}
        try:
            pin = args['pin']
            tenant_id = args['commandContext']['tenantId']
            user_wallet = dao_session.session.tenant_db().query(TUserWallet) \
                .filter(TUserWallet.pin == pin,
                        TUserWallet.tenant_id == tenant_id).first()
            if not user_wallet:
                is_suc = self.insert_one(pin, args)
                if is_suc:
                    user_wallet = dao_session.session.tenant_db().query(TUserWallet) \
                        .filter(TUserWallet.pin == pin,
                                TUserWallet.tenant_id == tenant_id).first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
        return user_wallet

    def update_one(self, pin: str, tenant_id: str, params: dict):

        try:
            dao_session.session.tenant_db().query(TUserWallet) \
                .filter(TUserWallet.pin == pin, TUserWallet.tenant_id == tenant_id) \
                .update(params)
            dao_session.session.tenant_db().commit()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def insert_one(self, pin: str, args: dict):

        commandContext = args['commandContext']
        data = self.get_model_common_field(commandContext)

        data['pin'] = pin
        user_wallet = TUserWallet(**data)
        dao_session.session.tenant_db().add(user_wallet)
        try:
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user wallet is error: {}".format(e))
            logger.exception(e)
            return False

    def query_list(self, valid_data, enable=2):

        pin_list, commandContext = valid_data

        user_wallets = dao_session.session.tenant_db() \
            .query(TUserWallet) \
            .filter(TUserWallet.pin.in_(pin_list), TUserWallet.tenant_id == commandContext['tenantId']) \
            .all()
        data_list = []
        try:
            for user_wallet in user_wallets:
                user_wallet: TUserWallet = user_wallet
                data_list.append(
                    dict(
                        pin=user_wallet.pin,
                        balance=user_wallet.balance,
                        recharge=user_wallet.recharge,
                        present=user_wallet.present,
                        deposited_mount=user_wallet.deposited_mount,
                        deposited_stats=user_wallet.deposited_stats,
                    )
                )
            exist_pin = {w.pin for w in user_wallets}
            [data_list.append(dict(
                pin=pin,
                balance=0,
                recharge=0,
                present=0,
                deposited_mount=0,
                deposited_stats=0,
            )) for pin in pin_list if pin not in exist_pin]

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("")
            logger.exception(e)
            return False

        return data_list

    def get_user_wallet(self, pin: str, args: dict):
        """
        从redis或mysql获取用户钱包信息
        """
        user_wallet: TUserWallet = self.query_one(args=args)
        user_wallet_dict = orm_to_dict(user_wallet, TUserWallet)

        return user_wallet_dict

    def set_user_wallet(self, pin: str, args: dict, ):

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)
            scene_key = PayKey.WALLET.value

            if self.exists_param(args['change_recharge']):
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']
                scene_key = PayKey.WALLET.value

            if self.exists_param(args['change_present']):
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['balance'] += args['change_present']

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_dict)

            param = {"pin": args.get("pin"), 'commandContext': commandContext}
            user_res = user_apis.internal_get_userinfo_by_id(param)
            user_res_data = json.loads(user_res)
            print(user_res_data)
            if not user_res_data.get("success"):
                raise MbException("用户服务调用失败")

            user_info = user_res_data.get('data')
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": args.get("created_pin"),
                "pin_id": args.get("pin"),
                "service_id": service_id,
                "type": args.get("type") or TransactionType.BOUGHT.value,
                "channel": args.get("channel") or ChannelType.ALIPAY_LITE.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "recharge_amount": args.get("change_recharge", 0),
                "present_amount": args.get("change_present", 0),
                "pin_phone": pin_phone,
                "pin_name": pin_name
            }
            wallet_dict_msg = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict_msg}")
            state = KafkaClient().visual_send(wallet_dict_msg, scene_key)

            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def bus_set_user_wallet(self, pin: str, args: dict, ):
        """
        B端
        """

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)
            pay_key = PayKey.WALLET.value

            if self.exists_param(args['change_recharge']):
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']
                pay_key = PayKey.WALLET.value

            if self.exists_param(args['change_present']):
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['recharge'] += args['change_present']
                pay_key = PayKey.WALLET.value

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_dict)

            param = {"pin": args.get("pin"), 'commandContext': commandContext}
            user_res = user_apis.internal_get_userinfo_by_id(param)
            user_res_data = json.loads(user_res)
            if not user_res_data.get("success"):
                raise MbException("用户服务调用失败")

            user_info = user_res_data.get('data')
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": args.get("created_pin"),
                "pin_id": args.get("pin"),
                "service_id": service_id,
                "type": args.get("type") or TransactionType.BOUGHT.value,
                "channel": args.get("channel") or ChannelType.PLATFORM.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "recharge_amount": args.get("change_recharge", 0),
                "present_amount": args.get("change_present", 0),
                "pin_phone": pin_phone,
                "pin_name": pin_name
            }
            logger.info(f"wallet_record send is {wallet_dict}")
            state = KafkaClient().visual_send(wallet_dict, pay_key)

            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def deduction_balance(self, pin: str, args: dict, ):

        deduction_amount = args['deduction_amount']
        tenant_id = args['commandContext']['tenantId']

        try:
            # if args.get("type") == TransactionType.BOUGHT.value:
            #     if not lock(USER_REFUND_RECHARGE_LOCK.format({"pin": pin}), 1, 60*60*24*30):
            #         raise MbException("30天内不可多次退款")

            user_wallet = self.get_user_wallet(pin, args)
            dao_session.redis_session.r.delete(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin))
            balance = user_wallet['balance'] - deduction_amount
            # 优先扣减充值余额
            if deduction_amount > user_wallet['recharge']:
                if balance < 0:
                    present = 0
                    recharge = balance
                else:
                    recharge = 0
                    present = balance
            else:
                recharge = user_wallet['recharge'] - deduction_amount
                present = user_wallet['present']

            params = dict(
                pin=pin,
                tenant_id=tenant_id,
                balance=balance,
                recharge=recharge,
                present=present,
            )

            self.update_one(pin=pin, tenant_id=tenant_id, params=params)
            return True
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(ex))
            logger.exception(ex)
            raise MbException("更新余额失败")

    @staticmethod
    def wallet_to_kafka(context, args: dict):
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
            # return {"suc": False, "data": f"用户信息获取失败: err: {e}"}
            service_id = 0
            pin_phone = ''
            pin_name = ''
        try:
            wallet_dict = {
                "tenant_id": context.get('tenantId'),
                "created_pin": args.get("created_pin"),
                "pin_id": args.get("pin"),
                "service_id": service_id,
                "type": args.get("type") or TransactionType.BOUGHT.value,
                "channel": args.get("channel") or ChannelType.ALIPAY_LITE.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "recharge_amount": args.get("change_recharge", 0),
                "present_amount": args.get("change_present", 0),
                "pin_phone": pin_phone,
                "pin_name": pin_name
            }
            logger.info(f"wallet_record send is {wallet_dict}")
            state = KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)
            if not state:
                return {"suc": False, "data": "kafka send failed"}
        except Exception as e:
            logger.info(f"wallet_record send err {e}")
            return {"suc": False, "data": f"wallet_to_kafka err: {e}"}
        return {"suc": True, "data": "wallet_kafka send success"}

    def wallet_data_format(self, wallet: dict):
        """
        用户钱包信息
        """

        wallet['can_refund_amount'] = 0
        if not dao_session.redis_session.r.get(USER_REFUND_RECHARGE_LOCK.format({"pin": wallet.get("pin")})):
            wallet["can_refund_amount"] = wallet.get("recharge")

        return wallet
