import datetime
import json
import time

from internal import user_apis
from internal.user_apis import internal_deposited_state_change
from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TUserWallet
from service.kafka import (
    PayKey,
)
from service.kafka.producer import KafkaClient
from service.wallet_service import WalletService
from utils.constant.user import DepositedState


class UserDepositService(WalletService):
    """
    用户押金
    """

    def update_one(self, pin: str, tenant_id: str, params: dict, update_pin: str = None):

        params["updated_at"] = datetime.datetime.now()
        params["updated_pin"] = update_pin or pin

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

    def get_user_deposit(self, pin: str, args: dict):
        """从redis或mysql获取用户钱包信息"""
        tenant_id = args['commandContext']['tenantId']
        # find_user_wallet = dao_session.redis_session.r.hgetall(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin))
        find_user_wallet = None
        if find_user_wallet:
            pass
            # try:
            #     user_wallet_dict = json.loads(find_user_wallet['content'])
            # except Exception:
            #     print('user_wallet_dict = find_user_wallet["content"]')
            #     user_wallet_dict = find_user_wallet["content"]
        else:
            user_wallet: TUserWallet = self.query_one(args=args)
            user_wallet_dict = orm_to_dict(user_wallet, TUserWallet)

            # if user_wallet:
            #     dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin),
            #                                      mapping={"content": json.dumps(user_wallet_dict),
            #                                               "version": datetime.now().timestamp()})

        return user_wallet_dict

    def edit_user_deposited(self, args: dict):
        """
        编辑用户押金
        """
        user_wallet = self.get_user_wallet(args['pin'], args)

        if args.get("deposited_stats") == DepositedState.DEPOSITED.value:
            if args.get("deposited_stats") < 0:
                raise MbException("押金金额有误")
            deposited_amount = args.get("deposited_mount")
            deposited_stats = DepositedState.DEPOSITED.value

        elif args.get("deposited_stats") == DepositedState.REFUNDED.value:
            deposited_amount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.REFUNDED.value

        elif args.get("deposited_stats") == DepositedState.FROZEN.value:
            deposited_amount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.FROZEN.value

        else:
            raise MbException("参数错误")

        update_params = {
            "deposited_mount": deposited_amount if deposited_stats != DepositedState.REFUNDED.value else 0,
            "deposited_stats": deposited_stats,
        }
        self.update_one(pin=args['pin'], tenant_id=args['commandContext']['tenantId'], params=update_params)

        #  向用户服务上报押金状态
        internal_deposited_state_change({
            'pin': args["pin"],
            'depositedOperate': deposited_stats,
            'commandContext': args['commandContext']
        })

        # 发送流水
        if args.get("deposited_stats") != DepositedState.FROZEN.value:
            commandContext = args.get("commandContext")
            param = {"pin": args.get("pin"), 'commandContext': commandContext}
            user_res = user_apis.internal_get_userinfo_by_id(param)
            user_res_data = json.loads(user_res)
            if not user_res_data.get("success"):
                raise MbException("用户服务调用失败")

            user_info = user_res_data.get('data')
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")
            deposit_dict = {
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

                "amount": deposited_amount,
                "paid_at": args.get("paid_at") or int(time.time()),
            }
            logger.info(f"wallet_record send is {deposit_dict}")
            KafkaClient().visual_send(deposit_dict, PayKey.DEPOSIT.value)

        return {"suc": True, "data": "更新成功"}

    def bus_edit_user_deposited(self, args: dict):
        """
        [B端]编辑用户押金
        """
        user_wallet = self.get_user_wallet(args['pin'], args)

        if args.get("deposited_stats") == DepositedState.DEPOSITED.value:
            if args.get("deposited_stats") < 0:
                raise MbException("押金金额有误")
            deposited_amount = args.get("deposited_mount")
            deposited_stats = DepositedState.DEPOSITED.value

        elif args.get("deposited_stats") == DepositedState.REFUNDED.value:
            deposited_amount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.REFUNDED.value

        elif args.get("deposited_stats") == DepositedState.FROZEN.value:
            deposited_amount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.FROZEN.value

        else:
            raise MbException("参数错误")

        update_params = {
            "deposited_mount": deposited_amount if deposited_stats != DepositedState.REFUNDED.value else 0,
            "deposited_stats": deposited_stats,
        }
        self.update_one(pin=args['pin'], tenant_id=args['commandContext']['tenantId'], params=update_params)

        #  向用户服务上报押金状态
        internal_deposited_state_change({
            'pin': args["pin"],
            'depositedOperate': deposited_stats,
            'commandContext': args['commandContext']
        })

        # 发送流水
        if args.get("deposited_stats") != DepositedState.FROZEN.value:
            commandContext = args.get("commandContext")
            param = {"pin": args.get("pin"), 'commandContext': commandContext}
            user_res = user_apis.internal_get_userinfo_by_id(param)
            user_res_data = json.loads(user_res)
            if not user_res_data.get("success"):
                raise MbException("用户服务调用失败")

            user_info = user_res_data.get('data')
            service_id = user_info.get('serviceId')
            pin_phone = user_info.get("phone")
            pin_name = user_info.get("authName")
            deposit_dict = {
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

                "amount": deposited_amount,
                "paid_at": args.get("paid_at") or int(time.time()),
            }
            logger.info(f"wallet_record send is {deposit_dict}")
            KafkaClient().visual_send(deposit_dict, PayKey.DEPOSIT.value)

        return {"suc": True, "data": "更新成功"}