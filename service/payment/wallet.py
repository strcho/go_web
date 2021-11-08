import json
import time
from urllib.parse import quote_plus

import requests

from model.all_model import *
from service.internal_call import node_auto_lock
from service.kafka import KafkaRetry, PayKey
from service.kafka.producer import kafka_client
from service.payment import PayDBService, PayNotifyNxLock
from service.payment import PayHelper
from service.payment.pay_utils.alilitepay import AliLiteService
from service.payment.pay_utils.alipay import AliService
from service.payment.pay_utils.pay_constant import *
from service.payment.pay_utils.sub_account import SubAccount
from service.payment.pay_utils.unionpay_app import UnionPayForApp
from service.payment.pay_utils.unionpay_code import UnionPayForCode
from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
from service.payment.pay_utils.wepay import WePayService
from service.payment.pay_utils.wxlitepay import WeLiteService
from service.payment.payment_interface import BusinessFullPayInterface, NotifyMixing
from service.user import UserReward
from mbutils import cfg, MbException
from mbutils import dao_session, logger
from utils.constant.account import PAY_TYPE, SERIAL_TYPE
from utils.constant.account import RIDING_CHANNEL_TYPE
from utils.constant.redis_key import ALI_TRADE_INFO, ACTIVE_ID_ROUTER_TRADE_NO
from datetime import datetime, timedelta


class WalletService:
    """
    select_activity,
    """
#
#     # 是否可退款
#     def is_user_refund(self, refund_fee, object_id) -> bool:
#         user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
#         return bool(refund_fee > user_info.recharge)
#
#     @staticmethod
#     def select_activity(active_id) -> XcEbike2MarketingActivityConfig:
#         """
#         查询活动相关的信息，支付这里目前这里只有充值活动,所以将充值活动相关的放在钱包的操作中
#         @param active_id:
#         @return:
#         """
#         return dao_session.session().query(XcEbike2MarketingActivityConfig). \
#             filter(XcEbike2MarketingActivityConfig.id == active_id).first()
#
#     def operate_activity(self, object_id, trade_no, active_id):
#         """
#         支付回调时调用的操作(该操作为充值活动，所以暂时放在wallet中)
#         @return:
#         """
#         logger.info(f"operate_activity object_id:{object_id}, trade_no:{trade_no}, active_id:{active_id}")
#         active = self.select_activity(active_id)
#         contents = json.loads(active.content)
#         if not contents:
#             logger.info(f"operate_activity contents is null, active_id:{active_id}, trade_no:{trade_no}")
#             return True
#         active_type = active.activetype
#         active_channel = contents.get("channal", "noActive")
#         logger.info(f"operate_activity active_type:{active_type}, active_channel: {active_channel}, content:{contents}")
#         if active_type == RechargeActiveType.GIVE_AMOUNT.value or active_channel == "giveAmount":
#             amount = contents.get("activeGiveAmount", 0)
#             if isinstance(amount, str):
#                 amount = int(amount)
#             if amount > 0:
#                 UserReward().add_balance_2_user(object_id, amount, 3)
#         if active_type == RechargeActiveType.GIVE_RIDING_CARD.value or active_channel == "giveRidingCard":
#             riding_card_id = contents.get("activeGiveRidingCardId", 0)
#             if int(riding_card_id) > 0:
#                 UserReward().add_riding_card_2_user(object_id, riding_card_id, 3)
#         return True
#
#     # 退款渠道是否正确
#     @staticmethod
#     def account_info(trade_no, object_id, channel=""):
#         account_info = dao_session.session().query(XcEbikeAccount2).filter(
#             XcEbikeAccount2.trade_no == trade_no, XcEbikeAccount2.objectId == object_id,
#             XcEbikeAccount2.type == SERIAL_TYPE.CHARGE.value)
#         if channel:
#             account_info = account_info.filter(XcEbikeAccount2.channel == channel).first()
#         account_info = account_info.first()
#         return account_info
#
#     def refund_verify(self, object_id, refund_fee, trade_no, type):
#         if not self.is_user_refund(refund_fee, object_id):
#             return {"suc": False, "info": "充值余额不够，不能进行退款"}
#         if not self.account_info(trade_no, object_id, type):
#             return {"suc": False, "info": "退款方式不对"}
#         return {"scu": True}
#
#     @staticmethod
#     def set_wallet_data(object_id, amount, channel, trade_no):
#         try:
#             user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
#             if not user_info:
#                 logger.info("回调时用户信息获取失败", object_id)
#                 return False
#             user_info.balance += -amount
#             user_info.recharge += -amount
#             #  更新购买记录的退款字段
#             account_info = dao_session.session().query(XcEbikeAccount2).filter(
#                 XcEbikeAccount2.trade_no == trade_no, XcEbikeAccount2.objectId == object_id,
#                 XcEbikeAccount2.type == SERIAL_TYPE.CHARGE.value).first()
#             if not account_info:
#                 logger.info("未查询到相关购买记录", object_id, trade_no)
#                 return False
#             account_info.refund_amount = amount
#
#             account = XcEbikeAccount2()
#             account.objectId = object_id
#             account.amount = -amount
#             account.type = SERIAL_TYPE.BALANCE_REFUND.value
#             account.channel = channel
#             account.trade_no = trade_no
#             account.paid_at = datetime.now()
#             account.refund_amount = amount
#             dao_session.session().add(account)
#             dao_session.session().flush()
#
#             wallet = XcEbikeUserWalletRecord()
#             wallet.userId = object_id
#             wallet.type = PAY_TYPE.BALANCE_REFUND.value
#             wallet.change = -amount
#             wallet.rechargeChange = -amount
#             wallet.presentChange = 0
#             wallet.orderId = account.serialNo
#             wallet.agentId = user_info.agentId  # 充值购买时，service_id,agent_id跟随用户id
#             wallet.serviceId = user_info.serviceId
#             dao_session.session().add(wallet)
#
#             dao_session.session().commit()
#             logger.info("钱包充值成功, object_id:{}, trade_no:{}, amount:{}, channel:{}".
#                         format(object_id, trade_no, -amount, channel))
#             return True
#             # todo:调用自动换车接口（考虑使用kafka发送一个消息，其他服务去做操作）
#         except Exception as e:
#             logger.error("钱包充值失败，{}".format(e))
#             return False
#
#     @staticmethod
#     def user_can_refund_wallet(object_id, refund_fee, trade_no):
#         rd_db_service = PayDBService(object_id)
#         user_res = rd_db_service.get_user()
#         if not user_res:
#             return {"suc": False, "info": f"用户不存在 {object_id}"}
#
#         if user_res.recharge < refund_fee:
#             logger.info(
#                 f'walletBalanceRefund, 退款金额大于充值余额, objectId: {object_id}, trade_no: {trade_no}, refund_fee: {refund_fee}')
#             return {'suc': False, "info": "退款金额大于充值余额"}
#
#         account_record = dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no,
#                                                                              XcEbikeAccount2.objectId == object_id).first()
#         # todo  判断退款渠道channel
#         if not account_record:
#             return {"suc": False, "info": "流水号: {trade_no},没有对应的购买记录数据,请先进行核实".format(trade_no=trade_no)}
#         return {'suc': True, "info": account_record}
#
#     @staticmethod
#     def wallet_record(object_id, amount, trade_no, gmt_create, account_channel, active_id):
#         """
#         object_id = data_dict.get("object_id", "")
#         amount = data_dict.get("amount", "")
#         account_channel = data_dict.get("account_channel", "")
#         trade_no = data_dict.get("trade_no", "")
#         gmt_create = data_dict.get("gmt_create", "")
#         @param object_id: 用户id
#         @param amount: 变化的金额(以分为单位，传进来的之前，判断一下目前的单位是什么)
#         @param trade_no: 交易流水单号
#         @param gmt_create: 支付时间
#         @param account_channel: 交易渠道(根据不同的回调，传不同的值)
#         @return:
#         """
#         wallet_dict = {
#             "object_id": object_id,
#             "amount": amount,
#             "trade_no": trade_no,
#             "gmt_create": gmt_create,
#             "account_channel": account_channel,
#             "active_id": active_id,
#         }
#         logger.info(f"wallet_record send is {wallet_dict}")
#         return kafka_client.pay_send(wallet_dict, key=PayKey.WALLET.value)
#
#     @staticmethod
#     def handle_wallet(data_dict):
#         logger.info(f"enter wallet consumer, data_dict: {data_dict}")
#         object_id = data_dict.get("object_id", "")
#         amount = data_dict.get("amount", "")
#         account_channel = data_dict.get("account_channel", "")
#         trade_no = data_dict.get("trade_no", "")
#         gmt_create = data_dict.get("gmt_create", "")
#         active_id = data_dict.get("active_id", 0)
#         try:
#             check = PayNotifyNxLock.check_trade_no(PayKey.WALLET.value, trade_no)
#             if not check["suc"]:
#                 return
#             account_state = PayDBService().is_account_by_trade(trade_no)
#             if account_state:
#                 logger.info("该消息已被消费，为重复数据，trade_no：{}".format(trade_no))
#                 return
#             user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
#             if not user_info:
#                 logger.info("回调时用户信息获取失败，{}".format(object_id))
#                 return
#             user_info.balance += amount
#             user_info.recharge += amount
#
#             account = XcEbikeAccount2()
#             account.objectId = object_id
#             account.amount = amount
#             account.type = SERIAL_TYPE.CHARGE.value
#             account.channel = account_channel
#             account.trade_no = trade_no
#             account.paid_at = datetime.now()
#             account.createdAt = datetime.now()
#             account.updatedAt = datetime.now()
#             dao_session.session().add(account)
#             dao_session.session().flush()
#
#             wallet = XcEbikeUserWalletRecord()
#             wallet.userId = object_id
#             wallet.type = PAY_TYPE.WALLET.value
#             wallet.change = amount
#             wallet.rechargeChange = amount
#             wallet.presentChange = 0
#             wallet.orderId = account.serialNo
#             wallet.agentId = user_info.agentId  # 充值购买时，service_id,agent_id跟随用户id
#             wallet.serviceId = user_info.serviceId
#             wallet.createdAt = datetime.now()
#             wallet.updatedAt = datetime.now()
#             dao_session.session().add(wallet)
#             dao_session.session().flush()
#
#             dao_session.session().commit()
#             logger.info("钱包充值成功, object_id:{}, trade_no:{}, amount:{}, channel:{}, active_id:{}".
#                         format(object_id, trade_no, amount, account_channel, active_id))
#             if active_id:
#                 WalletService().operate_activity(object_id, trade_no, active_id)
#             # todo:调用自动换车接口（考虑使用kafka发送一个消息，其他服务去做操作）
#             # autoLock支付完了通知自动锁车autoLock
#             node_auto_lock(object_id)
#
#             logger.info("handle_wallet success")
#         except Exception as e:
#             logger.error("钱包充值失败，{}".format(e))
#             # 插入失败后，将该消息重新插入到队列中
#             dao_session.session().rollback()
#             dao_session.session().close()
#             PayNotifyNxLock().del_pay_notify_lock(PayKey.WALLET.value, trade_no)
#             raise KafkaRetry()
#
#
# # 钱包视图调用函数
# class WalletCreateService(BusinessFullPayInterface):
#
#     def __init__(self, info):
#         super().__init__()
#         self.channel, self.object_id, self.amount, self.open_id, self.active_id, \
#         self.user_auth_code, self.car_id, self.front_url, self.buyer_id, self.single_split = info
#         self.url = "/wallet"
#         self.subject = cfg.get("alipay", {}).get("walletSubject", "钱包充值")
#
#     def _check(self):
#         user = PayDBService(self.object_id).get_user()
#         if not user:
#             return {"suc": False, "info": "用户不存在"}
#         activity = WalletService.select_activity(self.active_id)
#         if activity:
#             content = json.loads(activity.content)
#             activity_amount = content.get("amount", 0)
#             try:
#                 if int(activity_amount) != self.amount:
#                     logger.info(f"充值金额不匹配赠送id active_id:{self.active_id} "
#                                 f"activity_amount: {activity_amount}, amount: {self.amount} ")
#                     return {"suc": False, "info": "活动金额和购买金额不一致"}
#             except Exception as e:
#                 logger.info(f"活动金额设置有误，请检查。active_id:{self.active_id}, e:{e}")
#         return {"suc": True, "info": ""}
#
#     def ali_pay(self):
#         ali = AliService()
#         passback_params = {"objectId": self.object_id, "active_id": self.active_id}
#         trade_no = PayHelper.rand_str_24()
#         biz_countent = {
#             "subject": self.subject,
#             "out_trade_no": trade_no,
#             "timeout_express": '2h',
#             "product_code": 'QUICK_MSECURITY_PAY',
#             "passback_params": passback_params,
#             "total_amount": round(self.amount / 100, 2)
#         }
#         sign_status = ali.sign(self.url, biz_countent)
#         logger.info("alipay buy wallet, {},{},{}".format(self.object_id, RIDING_CHANNEL_TYPE.ALIPAY.value, self.amount))
#         return sign_status
#
#     def wx_pay(self):
#         attach = json.dumps({"objectId": self.object_id, "active_id": self.active_id})
#         trade_no = PayHelper.rand_str_24()
#         pay_params = WePayService().wx_pay(
#             self.subject, attach, self.amount, PayHelper.rand_str_32(), trade_no,
#             self.url, RIDING_CHANNEL_TYPE.WEPAY.value)
#         if not pay_params:
#             raise MbException("发起支付失败")
#         logger.info(f'wx create_order success {pay_params.get("pay_data")}')
#         return pay_params.get('pay_data')
#
#     def wx_lite(self):
#         attach = json.dumps({"objectId": self.object_id, "active_id": self.active_id})
#         trade_no = PayHelper.rand_str_24()
#         logger.info("wx_lite out_trade_no is {}".format(trade_no))
#         pay_params = WeLiteService().wx_pay(
#             self.subject, attach, self.amount, PayHelper.rand_str_32(), trade_no,
#             self.url, RIDING_CHANNEL_TYPE.WXLITE.value, self.open_id)
#         if not pay_params:
#             raise MbException("发起支付失败")
#         pre_value = {
#             "object_id": self.object_id,
#             "amount": self.amount,
#             "trade_no": trade_no,
#             "active_id": self.active_id,
#             "account_channel": RIDING_CHANNEL_TYPE.WXLITE.value,
#             "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
#             "app_id": cfg.get("wx_lite", {}).get("appid", "")
#         }
#         WeLiteService().set_info(pay_type="wallet", value={json.dumps(pre_value): str(time.time()).split('.')[0]})
#         logger.info(f'wx_lite create_order success {pay_params.get("pay_data")}')
#         return pay_params.get('pay_data')
#
#     def ali_lite(self):
#         trade_no = PayHelper.rand_str_24()
#         passback_params = {"objectId": self.object_id, "active_id": str(self.active_id)}
#         pay = AliLiteService().pay(self.buyer_id, trade_no, self.amount, self.url, self.subject, passback_params)
#         if not pay:
#             raise MbException("订单生成失败")
#         return pay
#
#     def union_pay_app(self):
#         attach = json.dumps({"objectId": self.object_id, "active_id": self.active_id})
#         trade_no = PayHelper.rand_str_24()
#         pay_params = UnionPayForApp().pay(self.amount, attach, trade_no, self.url)
#         if not pay_params:
#             raise MbException("订单生成失败")
#         return pay_params
#
#     def union_pay_code(self):
#         uc = UnionPayForCode()
#         get_union_user_id = uc.get_unionpay_app_user_id(PayHelper.rand_str_24(), self.user_auth_code, self.car_id)
#         if not get_union_user_id.get('suc'):
#             raise MbException("获取用户认证失败")
#         trade_no = PayHelper.rand_str_24()
#         order_id = get_union_user_id.get('order_id')
#         app_user_id = get_union_user_id.get('app_user_id')
#
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "out_trade_no": trade_no,
#             "backFrontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("normal_front_url")
#         })
#         pay_params = uc.pay(order_id, self.car_id, app_user_id, self.amount, attach)
#         if not pay_params:
#             raise MbException("订单生成失败")
#         # 暂存交易关联的一些信息 2day
#         trade_info = {"objectId": self.object_id, "active_id": self.active_id}
#         dao_session.redis_session.r.set(ALI_TRADE_INFO.format(order_id), json.dumps(trade_info), ex=172800)
#         return pay_params
#
#     def union_pay_lite(self):
#         user = PayDBService(self.object_id).get_user()
#         service_id = user.serviceId
#
#         Unionpay_WXlite = SubAccount("wallet", service_id, self.single_split).get_payment_config()
#         logger.info(f"wallet union_pay_lite union_pay_wx_lite: {Unionpay_WXlite}")
#         notify_url_head = Unionpay_WXlite.get("notify_url_head").replace("ebike", "anfu")
#         # notify_url_head = "http://denghui.c8612afb6df52455e9ac5a9a87a652908.cn-shenzhen.alicontainer.com/anfu/pay"  # 测试使用
#         body = "{}-钱包充值".format(Unionpay_WXlite.get('mchName'))
#         notify_url = "{}/wallet/unionpayNotify".format(notify_url_head)
#
#         trade_no = PayHelper.rand_str_24()
#         attach = json.dumps({"objectId": self.object_id, "out_trade_no": trade_no, "active_id": self.active_id})
#         pay_params = UnionPayForWXLite().pay(
#             self.open_id, self.amount, body, notify_url, trade_no, PayHelper.rand_str_32(), attach,
#             Unionpay_WXlite)
#         if not pay_params["suc"]:
#             raise MbException("订单生成失败")
#         return pay_params["data"]
    pass


class WalletNotifyService(BusinessFullPayInterface, NotifyMixing):

    def __init__(self, channel):
        super().__init__()
        self.url = "/wallet"
        self.subject = cfg.get("alipay", {}).get("walletSubject", "钱包充值")
        self.channel = channel

    def wx_pay(self, xml) -> str:
        logger.info("wx notify signature start")
        wx_notify = self.get_notify_func()(xml)
        logger.info(f"wx notify signature {wx_notify}")
        if wx_notify['suc']:
            attach_json = wx_notify.get("attach_json")
            notify = wx_notify.get("notify")
            if attach_json and attach_json.get('objectId') and notify:
                object_id = attach_json.get('objectId')
                active_id = attach_json.get('active_id')
                time_end = notify.get('time_end')
                trade_no = notify.get('transaction_id')  # 微信返回的订单id
                out_trade_no = notify.get('out_trade_no')  # 系统内部的订单id
                total_fee = notify.get('total_fee')
            else:
                return WX_FAILED_XML
        else:
            return WX_FAILED_XML
        if not isinstance(total_fee, int):
            try:
                total_fee = int(total_fee)
            except Exception as e:
                logger.error(f"wx_pay total fee is error, total_fee: {total_fee}, e:{e}")
                return WX_ERROR_XML
        state = WalletService.wallet_record(object_id, total_fee, trade_no, time_end, self.channel, active_id)
        if not state:
            return WX_ERROR_XML
        pre_value = {
            "object_id": object_id,
            "amount": total_fee,
            "trade_no": out_trade_no,
            "active_id": active_id,
            "account_channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        logger.info(f"del_info pre_value:{pre_value}, channel:{self.channel}")
        if self.channel == RIDING_CHANNEL_TYPE.WXLITE.value:
            WeLiteService.del_info("wallet", json.dumps(pre_value))
        return WX_SUCCESS_XML

    def wx_lite(self, xml):
        return self.wx_pay(xml)

    def ali_pay(self, notify: dict) -> dict:
        logger.info(f'ali notify: {notify}')
        res = self.get_notify_func()(notify)
        if not res['suc']:
            return ALI_FAILED_RESP

        total_amount = notify.get('total_amount')
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        active_id = params.get("active_id")
        amount = int(float(total_amount) * 100)
        wallet = WalletService.wallet_record(object_id, amount, trade_no, gmt_payment, self.channel, active_id)
        if not wallet:
            logger.info('信息写入失败，ali_lite_notify is failed')
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def ali_lite(self, notify) -> dict:
        return self.ali_pay(notify)

    def union_pay_app(self, notify) -> dict:
        query_id = notify.get("queryId")
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return UNION_FAILED_RESP
        attach = decode_notify.get('info')
        object_id = attach.get('objectId')
        active_id = attach.get('active_id')
        txn_time = notify.get('txnTime')
        txn_amt = notify.get('txnAmt')
        user = PayDBService(object_id).get_user()
        if not user:
            return UNION_FAILED_RESP
        amount = int(txn_amt)
        state = WalletService.wallet_record(object_id, amount, query_id, txn_time, self.channel, active_id)
        if not state:
            logger.info('优惠卡信息写入失败')
            return UNION_FAILED_RESP
        return UNION_SUCCESS_RESP

    def union_pay_lite(self, notify):
        validate = self.get_notify_func()(notify)
        if not validate['suc']:
            logger.info(f"wallet union_pay_lite failed validate:{validate}")
            return WX_FAILED_XML
        logger.info(f"wallet union_pay_lite validate:{validate}")
        attach = validate.get('attach_json')
        notify_info = validate.get('notify')
        object_id = attach.get('objectId')
        active_id = attach.get('active_id')
        txn_time = notify_info.get('time_end', '')
        txn_amt = notify_info.get('total_fee', 0)
        transaction_id = notify_info.get('transaction_id')
        user = PayDBService(object_id).get_user()
        if not user:
            return WX_ERROR_XML
        amount = int(txn_amt)
        state = WalletService.wallet_record(object_id, amount, transaction_id, txn_time, self.channel, active_id)
        if not state:
            logger.info('钱包信息写入失败')
            return WX_ERROR_XML
        return WX_SUCCESS_XML

    def union_pay_code(self, notify) -> dict:
        return self.union_pay_app(notify)


class WalletRefundService(BusinessFullPayInterface):
    def __init__(self, object_id, trade_no, refund_fee, channel: int):
        self.object_id = object_id
        self.trade_no = trade_no
        self.refund_fee = int(refund_fee)
        self.total_fee = 0
        self.channel = channel
    #
    # def _check(self):
    #     """
    #     判断交易单号是否存在，退款金额是否大于购买金额，用户是否存在，交易类型是否一致，需要做参数校验
    #     @return:
    #     """
    #     refund_account = dao_session.session().query(XcEbikeAccount2) \
    #         .filter(XcEbikeAccount2.trade_no == self.trade_no,
    #                 XcEbikeAccount2.objectId == self.object_id,
    #                 XcEbikeAccount2.type == SERIAL_TYPE.BALANCE_REFUND.value).first()
    #     if refund_account:
    #         return {"suc": False, "info": "已退款"}
    #     account = dao_session.session().query(XcEbikeAccount2) \
    #         .filter(XcEbikeAccount2.trade_no == self.trade_no,
    #                 XcEbikeAccount2.objectId == self.object_id,
    #                 XcEbikeAccount2.type == SERIAL_TYPE.CHARGE.value).first()
    #     if account and (account.channel != self.channel or account.amount < self.refund_fee):
    #         return {"suc": False, "info": "退款失败0"}
    #     else:
    #         self.total_fee = account.amount
    #         return {"suc": True, "info": ""}
    #
    # def wallet_refund(self, refund_func):
    #     """
    #     object_id, trade_no, refund_fee
    #     """
    #     logger.info("wallet card is refund ,", refund_func)
    #     # 参数校验
    #     refund_verify = WalletService.user_can_refund_wallet(self.object_id, self.refund_fee, self.trade_no)
    #     if not refund_verify['suc']:
    #         return refund_verify
    #     res = refund_func(self.object_id, self.trade_no, self.refund_fee, self.total_fee)
    #     if res["suc"]:
    #         save = WalletService.set_wallet_data(self.object_id, self.refund_fee, self.channel, self.trade_no)
    #         if save:
    #             return {"suc": True, "info": "退款成功"}
    #         else:
    #             return {"suc": False, "info": "退款失败1"}
    #     else:
    #         return {"suc": False, "info": "退款失败2"}
    pass

    def wx_pay(self):
        return self.wallet_refund(WePayService().refund)

    def ali_pay(self):
        return self.wallet_refund(AliService().refund)

    def wx_lite(self):
        return self.wallet_refund(WeLiteService().refund)

    def ali_lite(self):
        return self.wallet_refund(AliLiteService().refund)

    def union_pay_app(self):
        return self.wallet_refund(UnionPayForApp().refund)

    def union_pay_lite(self):
        return self.wallet_refund(UnionPayForWXLite().refund)

    def union_pay_code(self):
        return self.wallet_refund(UnionPayForCode().refund)
