import json
from datetime import datetime

from internal import user_apis, marketing_api
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
    TransactionType,
    ChannelType,
)
from service.kafka.producer import KafkaClient
from service.wallet_service import WalletService
from utils.constant.user import DepositedState


class UserDepositService(WalletService):
    """
    用户押金
    """

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
    #
    # def set_user_deposit(self, pin: str, args: dict,):
    #
    #     try:
    #         deposited_stats = None
    #         user_wallet_dict = self.get_user_wallet(pin=pin, args=args)
    #         if self.exists_param(args['change_deposited_mount']):
    #             user_wallet_dict['deposited_mount'] += args['change_deposited_mount']
    #             if args['change_deposited_mount'] < 0:
    #                 deposited_stats = DepositedState.REFUNDED.value
    #
    #             elif args['change_deposited_mount'] > 0:
    #                 deposited_stats = DepositedState.DEPOSITED.value
    #             else:
    #                 raise MbException('押金变动金额有误')
    #
    #         if self.exists_param(args['deposited_stats']):
    #             user_wallet_dict['deposited_stats'] = args['deposited_stats']
    #             deposited_stats = args['deposited_stats']
    #
    #         #  向用户服务上报押金状态
    #         if deposited_stats:
    #             internal_deposited_state_change({
    #                 'pin': pin,
    #                 'depositedOperate': deposited_stats,
    #                 'commandContext': args['commandContext']
    #             })
    #
    #         self.update_one(pin=pin, args=user_wallet_dict)
    #
    #         # 发送资产流水
    #         commandContext = args.get("commandContext")
    #         try:
    #             user_res = user_apis.internal_get_userinfo_by_id(
    #                 {"pin": args.get("pin_id"), 'commandContext': commandContext})
    #             user_info = json.loads(user_res).get("data")
    #             service_id = user_info.get('serviceId')
    #             pin_phone = user_info.get("phone")
    #             pin_name = user_info.get("authName")
    #
    #             wallet_dict = {
    #                 "tenant_id": commandContext.get('tenantId'),
    #                 "created_pin": args.get("created_pin"),
    #                 "pin_id": args.get("pin_id"),
    #                 "service_id": service_id,
    #                 "type": args.get("type") or TransactionType.BOUGHT.value,
    #                 "channel": args.get("channel") or ChannelType.ALIPAY_LITE.value,
    #                 "sys_trade_no": args.get("sys_trade_no"),
    #                 "merchant_trade_no": args.get("merchant_trade_no"),
    #                 "recharge_amount": args.get("change_recharge", 0),
    #                 "present_amount": args.get("change_present", 0),
    #                 "pin_phone": pin_phone,
    #                 "pin_name": pin_name
    #             }
    #             logger.info(f"wallet_record send is {wallet_dict}")
    #             state = KafkaClient().visual_send(wallet_dict, PayKey.WALLET.value)
    #             if not state:
    #                 return {"suc": False, "data": "kafka send failed"}
    #         except Exception as e:
    #             logger.info(f"wallet_record send err {e}")
    #             return {"suc": False, "data": f"wallet_to_kafka err: {e}"}
    #
    #         return True
    #
    #     except Exception as e:
    #         dao_session.session.tenant_db().rollback()
    #         logger.error("update user wallet is error: {}".format(e))
    #         logger.exception(e)
    #         raise MbException("更新用户押金失败")

    @staticmethod
    def deposit_to_kafka(context, args: dict):
        # todo 根据用户id查询服务区id，
        try:
            user_info = user_apis.apiTest4({"user_id": args.get("pin_id")})
            service_id = user_info.get('service_id')
        except Exception as e:
            # service_id获取失败暂不报错
            logger.info(f"user_apis err: {e}")
            service_id = 61193175763522450
        # todo 获取诚信金金额
        try:
            deposit_info = marketing_api.apiTest1({"service_id": service_id})
        except Exception as e:
            logger.info(f"{e}")
            return {"suc": False, "data": "获取诚信金失败"}

        try:
            deposit_dict = {
                "tenant_id": context.get('tenantId'),
                "created_pin": args.get("created_pin"),
                "pin_id": args.get("pin_id"),
                "service_id": service_id,
                "type": args.get("type"),
                "channel": args.get("channel"),
                "sys_trade_no": args.get("sys_trade_no"),
                "merchant_trade_no": args.get("merchant_trade_no"),
                "name": "deposit",
                "amount": deposit_info.get("amount"),
            }
            logger.info(f"wallet_record send is {deposit_dict}")
            state = KafkaClient.visual_send(deposit_dict, PayKey.DEPOSIT.value)
            if not state:
                return {"suc": False, "data": "kafka send failed"}
        except Exception as e:
            logger.info(f"deposit_record send err {e}")
            return {"suc": False, "data": f"deposit_to_kafka err: {e}"}
        return {"suc": True, "data": "deposit_to_kafka send success"}

    def edit_user_deposited(self, args: dict):
        """
        编辑用户押金
        """
        user_wallet = self.get_user_wallet(args['pin'], args)

        if args.get("deposited_stats") == DepositedState.DEPOSITED.value:
            if args.get("deposited_stats") < 0:
                raise MbException("押金金额有误")
            deposited_mount = args.get("deposited_mount")
            deposited_stats = DepositedState.DEPOSITED.value

        elif args.get("deposited_stats") == DepositedState.REFUNDED.value:
            deposited_mount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.REFUNDED.value

        elif args.get("deposited_stats") == DepositedState.FROZEN.value:
            deposited_mount = user_wallet.get("deposited_mount")
            deposited_stats = DepositedState.FROZEN.value

        else:
            raise MbException("参数错误")

        update_params = {
            "deposited_mount": deposited_mount,
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
        commandContext = args.get("commandContext")
        param = {"pin": args.get("pin"), 'commandContext': commandContext}
        user_info = user_apis.internal_get_userinfo_by_id(param)
        service_id = user_info.get('service_id')
        context = args['commandContext']
        deposit_dict = {
            "tenant_id": context.get('tenantId'),
            "created_pin": args.get("created_pin"),
            "pin_id": args.get("pin_id"),
            "service_id": service_id,
            "type": args.get("type"),
            "channel": args.get("channel"),
            "sys_trade_no": args.get("sys_trade_no"),
            "merchant_trade_no": args.get("merchant_trade_no"),
            "name": "deposit",
            "amount": deposited_mount,
        }
        logger.info(f"wallet_record send is {deposit_dict}")
        state = KafkaClient.visual_send(deposit_dict, PayKey.DEPOSIT.value)

        return {"suc": True, "data": "更新成功"}
