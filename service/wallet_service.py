import datetime
import time
import traceback

from internal.marketing_api import MarketingApi
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
            logger.error("query user wallet is error: {}".format(e), extra=args['commandContext'])
            logger.exception(e)

        return user_wallet

    def query_one_with_row_lock(self, args: dict):
        user_wallet = {}
        try:
            pin = args['pin']
            tenant_id = str(args['commandContext']['tenantId'])
            user_wallet = dao_session.session.tenant_db().query(TUserWallet) \
                .filter(TUserWallet.pin == pin,
                        TUserWallet.tenant_id == tenant_id).with_for_update().first()
            if not user_wallet:
                is_suc = self.insert_one(pin, args)
                if is_suc:
                    user_wallet = dao_session.session.tenant_db().query(TUserWallet) \
                        .filter(TUserWallet.pin == pin,
                                TUserWallet.tenant_id == tenant_id).with_for_update().first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("query user wallet is error: {}".format(e), extra=args['commandContext'])
            logger.exception(e)

        return user_wallet

    def update_one(self, pin: str, tenant_id: str, params: dict, update_pin: str = None, commandContext = None):

        try:

            params["updated_at"] = datetime.datetime.now()
            params["updated_pin"] = update_pin or pin

            dao_session.session.tenant_db().query(TUserWallet) \
                .filter(TUserWallet.pin == pin, TUserWallet.tenant_id == tenant_id) \
                .update(params)
            dao_session.session.tenant_db().commit()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e), extra=commandContext)
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
            logger.error("insert user wallet is error: {}".format(e), extra=args['commandContext'])
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

    def get_user_wallet(self, pin: str, args: dict, with_row_lock=False):
        """
        从redis或mysql获取用户钱包信息
        """
        if with_row_lock:
            user_wallet: TUserWallet = self.query_one_with_row_lock(args=args)
        else:
            user_wallet: TUserWallet = self.query_one(args=args)
        user_wallet_dict = orm_to_dict(user_wallet, TUserWallet)

        return user_wallet_dict

    def set_user_wallet(self, pin: str, args: dict, ):

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)

            user_wallet_param = {
                "recharge": TUserWallet.recharge + args.get("change_recharge", 0),
                "present": TUserWallet.present + args.get("change_present", 0),
                "balance": TUserWallet.balance + args.get("change_recharge", 0) + args.get("change_present", 0),
            }

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_param, commandContext=commandContext)

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin") or "",
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get("pin") or "",

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.BOUGHT.value,
                "channel": args.get("channel") if args.get("channel") is not None else ChannelType.ALIPAY_LITE.value,
                "sys_trade_no": args.get("sys_trade_no") or "",
                "merchant_trade_no": args.get("merchant_trade_no") or "",
                "amount": abs(args.get("change_recharge", 0) + args.get("change_present", 0)),
                "paid_at": args.get("paid_at") or int(time.time()),
                "iz_refund": args.get("iz_refund", 0),
                "recharge_amount": abs(args.get("change_recharge", 0)),
                "present_amount": abs(args.get("change_present", 0)),
            }

            wallet_dict_msg = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict_msg}")
            KafkaClient().visual_send(wallet_dict_msg, PayKey.WALLET.value)

            try:
                if args.get("type") == 1 and self.exists_param(args['change_recharge']):  # 充值购买
                    MarketingApi.buy_wallet_judgement(pin=args.get("pin"), service_id=service_id,
                                                      buy_time=args.get("paid_at") or int(time.time()),
                                                      command_context=commandContext)
            except Exception as e:
                logger.error(f"营销活动回调失败 buy_wallet_judgement： {e}", extra=args['commandContext'])

            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e), extra=args['commandContext'])
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
                pass  # 去除此功能
                # if -1000000 > args['change_recharge'] > 1000000:
                #     MbException("参数越界")
                # user_wallet_dict['balance'] += args['change_recharge']
                # user_wallet_dict['recharge'] += args['change_recharge']
                # args["type"] = TransactionType.PLATFORM_BOUGHT.value if args['change_recharge'] > 0 else TransactionType.PLATFORM_REFUND.value

            elif args['change_present']:
                if -10000000 < args['change_present'] < 10000000:
                    args["type"] = TransactionType.PLATFORM_BOUGHT.value if args['change_present'] > 0 else TransactionType.PLATFORM_REFUND.value
                else:
                    raise MbException("参数越界")
            else:
                raise MbException("参数错误")

            user_wallet_param = {
                "recharge": TUserWallet.recharge + args.get("change_recharge", 0),
                "present": TUserWallet.present + args.get("change_present", 0),
                "balance": TUserWallet.balance + args.get("change_recharge", 0) + args.get("change_present", 0),
            }

            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=commandContext["tenantId"], params=user_wallet_param, commandContext=commandContext)

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin") or "",
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get("pin") or "",

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.PLATFORM_BOUGHT.value,
                "channel": args.get("channel") or ChannelType.PLATFORM.value,
                "sys_trade_no": args.get("sys_trade_no") or "",
                "merchant_trade_no": args.get("merchant_trade_no") or "",
                # "amount": abs(args.get("change_recharge", 0) + args.get("change_present", 0)),
                "amount": abs(args.get("change_present", 0)),
                "paid_at": args.get("paid_at") or int(time.time()),
                "iz_refund": args.get("iz_refund", 0),
                # "recharge_amount": abs(args.get("change_recharge", 0)),
                "recharge_amount": 0,
                "present_amount": abs(args.get("change_present", 0)),
            }
            wallet_dict = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict}")
            KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)

            return True

        except MbException as mb:
            raise mb

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e), extra=args['commandContext'])
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def deduction_balance(self, pin: str, args: dict, ):

        deduction_amount = args['deduction_amount']
        tenant_id = args['commandContext']['tenantId']

        try:
            user_wallet = self.get_user_wallet(pin, args, with_row_lock=True)

            old_recharge_amount = user_wallet["recharge"]
            old_present_amount = user_wallet["present"]

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

            recharge_amount = old_recharge_amount - recharge
            present_amount = old_present_amount - present

            user_wallet_param = dict(
                balance=TUserWallet.balance - deduction_amount,
                recharge=TUserWallet.recharge - recharge_amount,
                present=TUserWallet.present - present_amount,
            )
            commandContext = args.get("commandContext")
            self.update_one(pin=pin, tenant_id=tenant_id, params=user_wallet_param, commandContext=commandContext)

            user_info = UserApi.get_user_info(pin=pin, command_context=commandContext)
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")

            wallet_dict = {
                "tenant_id": commandContext.get('tenantId'),
                "created_pin": commandContext.get("created_pin") or "",
                "version": commandContext.get("version", 0),
                "updated_pin": commandContext.get("pin") or "",

                "pin_id": args.get("pin"),
                "pin_phone": pin_phone,
                "pin_name": pin_name,
                "service_id": service_id,
                "type": args.get("type") or TransactionType.PLATFORM_BOUGHT.value,
                "channel": args.get("channel") or ChannelType.PLATFORM.value,
                "sys_trade_no": args.get("sys_trade_no") or "",
                "merchant_trade_no": args.get("merchant_trade_no") or "",
                "amount": deduction_amount,
                "paid_at": args.get("paid_at") or int(time.time()),
                "iz_refund": args.get("iz_refund", 0),
                "recharge_amount": recharge_amount,
                "present_amount": present_amount,
            }
            wallet_dict = self.remove_empty_param(wallet_dict)
            logger.info(f"wallet_record send is {wallet_dict}")
            KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)

            return True, {"recharge_amount": recharge_amount, "present_amount": present_amount}
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)
            raise MbException("更新余额失败")
