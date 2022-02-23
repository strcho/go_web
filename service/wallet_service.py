import datetime
import time

from internal.user_apis import UserApi
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


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, args: dict):
        user_wallet = {}
        try:
            pin = args['pin']
            tenant_id = str(args['commandContext']['tenantId'])
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

    def update_one(self, pin: str, tenant_id: str, params: dict, update_pin: str = None):

        try:

            params["updated_at"] = datetime.datetime.now()
            params["updated_pin"] = update_pin or pin

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

            if self.exists_param(args['change_recharge']):
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']

            if self.exists_param(args['change_present']):
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['balance'] += args['change_present']

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_dict)

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin"),
                "version": commandContext.get("version", ""),
                "updated_pin": commandContext.get("pin"),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.BOUGHT.value,
                "channel": args.get("channel") if args.get("channel") is not None else ChannelType.ALIPAY_LITE.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": args.get("change_recharge", 0) + args.get("change_present", 0),
                "paid_at": args.get("paid_at") or int(time.time()),
                "recharge_amount": abs(args.get("change_recharge", 0)),
                "present_amount": abs(args.get("change_present", 0)),
            }

            wallet_dict_msg = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict_msg}")
            KafkaClient().visual_send(wallet_dict_msg, PayKey.WALLET.value)

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
            args["channel"] = ChannelType.PLATFORM.value
            if args['change_recharge']:
                if -1000000000 >= args['change_recharge'] >= 1000000000:
                    MbException("参数越界")
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']
                args["type"] = TransactionType.PLATFORM_BOUGHT.value if args['change_recharge'] > 0 else TransactionType.PLATFORM_REFUND.value

            elif args['change_present']:
                if -1000000000 >= args['change_recharge'] >= 1000000000:
                    MbException("参数越界")
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['balance'] += args['change_present']
                args["type"] = TransactionType.PLATFORM_BOUGHT.value if args['change_present'] > 0 else TransactionType.PLATFORM_REFUND.value
            else:
                MbException("参数错误")

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_dict)

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin"),
                "version": commandContext.get("version", ""),
                "updated_pin": commandContext.get("pin"),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.PLATFORM_BOUGHT.value,
                "channel": args.get("channel") or ChannelType.PLATFORM.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": abs(args.get("change_recharge", 0) + args.get("change_present", 0)),
                "paid_at": args.get("paid_at") or int(time.time()),
                "recharge_amount": abs(args.get("change_recharge", 0)),
                "present_amount": abs(args.get("change_present", 0)),
            }
            wallet_dict = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict}")
            KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)

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
            user_wallet = self.get_user_wallet(pin, args)
            balance = user_wallet['balance'] - deduction_amount
            # 优先扣减充值余额
            if deduction_amount > user_wallet['recharge']:
                if balance < 0:
                    recharge = balance
                    present = 0
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

            commandContext = args.get("commandContext")

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin"),
                "version": commandContext.get("version", ""),
                "updated_pin": commandContext.get("pin"),

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.PLATFORM_BOUGHT.value,
                "channel": args.get("channel") or ChannelType.PLATFORM.value,
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "amount": args.get("change_recharge", 0) + args.get("change_present", 0),
                "paid_at": args.get("paid_at") or int(time.time()),
                "recharge_amount": abs(args.get("change_recharge", 0)),
                "present_amount": abs(args.get("change_present", 0)),
            }
            wallet_dict = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict}")
            KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)

            return True
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(ex))
            logger.exception(ex)
            raise MbException("更新余额失败")
