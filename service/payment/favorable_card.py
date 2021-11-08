import json
import time
from datetime import timedelta

from model.all_model import *
from service.kafka import KafkaRetry, PayKey
from service.kafka.producer import kafka_client
from service.payment.pay_utils.wxlitepay import WeLiteService
from service.payment.payment_interface import BusinessFullPayInterface, NotifyMixing
from service.payment import UserDBService
from service.payment import PayHelper
from service.payment import PayNotifyNxLock, PayDBService
from service.payment.pay_utils.alilitepay import AliLiteService
from service.payment.pay_utils.alipay import AliService
from service.payment.pay_utils.pay_constant import *
from service.payment.pay_utils.sub_account import SubAccount
from service.payment.pay_utils.unionpay_app import UnionPayForApp
from service.payment.pay_utils.unionpay_code import UnionPayForCode
from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
from service.payment.riding_card import WePayService, RidingCardService
from mbutils import dao_session, logger, MbException
# 优惠卡支付重复部分，共用函数
from utils.constant.account import RIDING_CHANNEL_TYPE, SERIAL_TYPE
from utils.constant.redis_key import ALI_TRADE_INFO, REFUND_FAVORABLECARD_LOCK
from utils.constant.user import UserState


class FavorableCardService:
#
#     # @staticmethod
#     def favorable_verify(self, object_id, favorable_card_id):
#         rd_db_service = PayDBService(object_id)
#         user_res = rd_db_service.get_user()
#         if not user_res:
#             logger.info(f"favorable card payment user is not exist:object_id:{object_id}, card_id:{favorable_card_id}")
#             return {"suc": False, "info": '用户不存在'}
#         favorable = self.get_favorable_card_by_id(favorable_card_id)
#         if not favorable:
#             logger.info(f"favorable card payment card is not exist:object_id:{object_id}, card_id:{favorable_card_id}")
#             return {"suc": False, "info": '该优惠卡的信息有误'}
#         logger.info(f"card is exist, price: {favorable.present_price}")
#         return {"suc": True, "info": {"price": favorable.present_price}}
#
#     @staticmethod
#     def user_can_refund_favorable_card(object_id, trade_no, refund_fee):
#         refund_lock = dao_session.redis_session.r.set(
#             REFUND_FAVORABLECARD_LOCK.format(object_id=object_id),
#             1, nx=True, px=5000)
#         if not refund_lock:
#             return {"suc": False, "info": "优惠卡退款中,请5s后重试"}
#         if refund_fee <= 0:
#             return {"suc": False, "info": "请输入正整数的退款金额"}
#         user_state = UserDBService.get_user_state(object_id)
#         if isinstance(user_state, str):
#             user_state = int(user_state)
#         if not user_state:
#             return {"suc": False, "info": "获取当前用户信息失败,无法进行退款操作"}
#         if user_state == UserState.SIGN_UP.value:
#             return {"suc": False, "info": "用户没有实名,请先进行实名操作"}
#         if user_state == UserState.LEAVING.value or user_state == UserState.RIDING.value:
#             return {"suc": False, "info": "用户正在使用中,请不要进行退款"}
#         if user_state == UserState.TO_PAY.value:
#             return {"suc": False, "info": "用户有未完结的订单,请用户完结后进行退款"}
#
#         favorable_card_record = dao_session.session().query(XcMieba2FavorableCardAccount).filter(
#             XcMieba2FavorableCardAccount.trade_no == trade_no, XcMieba2FavorableCardAccount.is_found == 0).order_by(
#             XcMieba2FavorableCardAccount.created_at.desc()).first()
#         if not favorable_card_record:
#             return {"suc": False, "info": f"没有该流水的购买记录,请先进行核,trade_no:{trade_no}"}
#         account_record = dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no,
#                                                                              XcEbikeAccount2.objectId == object_id,
#                                                                              XcEbikeAccount2.amount > 0).first()
#         if not account_record:
#             return {"suc": False, "info": "流水号: {trade_no},没有对应的购买记录数据,请先进行核实".format(trade_no=trade_no)}
#         has_refund_record = account_record.refund_amount or 0
#         if has_refund_record + refund_fee > account_record.amount:
#             could_refund_amount = (account_record.amount - has_refund_record) / 100 if (
#                                                                                                account_record.amount - has_refund_record) > 0 else 0
#             return {"suc": False,
#                     "info": "退款金额超出,当前可退款余额: {could_refund_amount}元".format(could_refund_amount=could_refund_amount)}
#         return {"suc": True, "info": {"favorable_card_record": favorable_card_record, "account_record": account_record}}
#
#     @staticmethod
#     def get_favorable_user(service_id, object_id):
#         favorable_user = dao_session.session().query(XcMieba2FavorableCardUser).filter(
#             XcMieba2FavorableCardUser.service_id == service_id,
#             XcMieba2FavorableCardUser.object_id == object_id).first()
#         return favorable_user
#
#     # 优惠卡信息
#     @staticmethod
#     def get_favorable_card_by_id(favorable_card_id) -> XcMieba2FavorableCard:
#         favorable_card = dao_session.session().query(XcMieba2FavorableCard). \
#             filter(XcMieba2FavorableCard.id == favorable_card_id).first()
#         return favorable_card
#
#     @staticmethod
#     def favorable_card_refund(object_id, refund_fee, trade_no, favorable_card_record, account_record):
#         try:
#             favorable = dao_session.session().query(XcMieba2FavorableCardAccount).filter(
#                 XcMieba2FavorableCardAccount.trade_no == trade_no,
#                 XcMieba2FavorableCardAccount.object_id == object_id).first()
#             favorable_card_info = {}
#             #  需要在表中写to_dict方法 return {c.name: getattr(self, c.name) for c in self.__table__.columns}
#             favorable_card_info['object_id'] = favorable.object_id
#             favorable_card_info['service_id'] = favorable.service_id
#             favorable_card_info['agent_id'] = favorable.agent_id
#             favorable_card_info['card_id'] = favorable.card_id
#             favorable_card_info['price'] = favorable.price
#             favorable_card_info['channel'] = favorable.channel
#             favorable_card_info['trade_no'] = favorable.trade_no
#
#             favorable_card_info['serial_type'] = SERIAL_TYPE.FAVORABLE_CARD_REFUND.value
#             favorable_card_info['price'] = refund_fee * -1
#             favorable_card_info['is_found'] = 1
#             fc = XcMieba2FavorableCardAccount(**favorable_card_info)
#             dao_session.session().add(fc)
#             dao_session.session().flush()
#
#             # XcEbikeAccount2
#             order_info = {
#                 "objectId": object_id,
#                 "amount": refund_fee * -1,
#                 "type": SERIAL_TYPE.FAVORABLE_CARD_REFUND.value,
#                 "channel": account_record.channel,
#                 "trade_no": trade_no,  # refund返回 trade_no
#                 "orderId": fc.id,
#                 "paid_at": datetime.now()
#             }
#             # 插入流水表
#             xc_a = XcEbikeAccount2(**order_info)
#             dao_session.session().add(xc_a)
#             refund_amount = account_record.refund_amount or 0 + refund_fee  # 累计退款金额
#
#             dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no,
#                                                                 XcEbikeAccount2.objectId == object_id,
#                                                                 XcEbikeAccount2.amount > 0).update(
#                 {"refund_amount": refund_amount, "objectId": object_id, "trade_no": trade_no})
#             favorable.is_found = 1
#             dao_session.session().commit()
#         except Exception as e:
#             dao_session.session().rollback()
#             dao_session.session().close()
#             logger.info(f"depost card refund error: {e}")
#             return False
#         return True
#
#     @staticmethod
#     def favorable_card_record(object_id, favorable_card_id, trade_no, serial_type, channel, time_end, amount):
#         """
#         @param object_id:
#         @param favorable_card_id:
#         @param trade_no:
#         @param serial_type:
#         @param channel:
#         @param time_end:
#         @param amount:
#         @return:
#         """
#         favorable_card_dict = {
#             "object_id": object_id,
#             "favorable_card_id": favorable_card_id,
#             "trade_no": trade_no,
#             "serial_type": serial_type,
#             "channel": channel,
#             "time_end": time_end,
#             "amount": amount,
#         }
#         logger.info(f'favorable_record send is {favorable_card_dict}')
#         return kafka_client.pay_send(favorable_card_dict, key=PayKey.FAVORABLE_CARD.value)
#
#     # 根据用户购买的优惠卡类型，新建或者更新用户的优惠卡信息
#     @staticmethod
#     def handle_favorable_card(data_dict):
#         """ serial_type, object_id, transaction_id, amount, channel, time_end, favorable_card_id """
#         logger.info(f"enter favorable card consumer, data_dict: {data_dict}")
#         object_id = data_dict.get("object_id", "")
#         favorable_card_id = data_dict.get("favorable_card_id", "")
#         trade_no = data_dict.get("trade_no", "")
#         serial_type = data_dict.get("serial_type", "")
#         channel = data_dict.get("channel", "")
#         amount = data_dict.get("amount", "")
#         try:
#             rd_db_service = PayDBService(object_id)
#             check = PayNotifyNxLock.check_trade_no(PayKey.FAVORABLE_CARD.value, trade_no)
#             if not check["suc"]:
#                 logger.info("重复回调锁")
#                 return
#             is_account = rd_db_service.is_account_by_trade(trade_no)
#             if is_account:
#                 logger.info("重复回调")
#                 return
#             user_res = rd_db_service.get_user()
#             if not user_res:
#                 logger.info("用户不存在: {objectId}".format(objectId=object_id))
#                 return
#             find_favorable_card = FavorableCardService.get_favorable_card_by_id(favorable_card_id)
#             if not find_favorable_card:  # if not find_favorable_card or len(find_favorable_card) < 1
#                 logger.info("优惠卡不存在,serviceId:{} ,serialType:{}".format(user_res.serviceId, serial_type))
#                 return
#             xc_a = XcEbikeAccount2()
#             xc_a.objectId = object_id
#             xc_a.amount = amount
#             xc_a.type = serial_type
#             xc_a.channel = channel
#             xc_a.trade_no = trade_no
#             xc_a.paid_at = datetime.now()
#             xc_a.createdAt = datetime.now()
#             xc_a.updatedAt = datetime.now()
#             # 插入流水表
#             dao_session.session().add(xc_a)
#             dao_session.session().flush()
#
#             # 优惠卡流水记录存储
#             xc_fca = XcMieba2FavorableCardAccount()
#             xc_fca.object_id = object_id
#             xc_fca.service_id = user_res.serviceId or 1
#             xc_fca.agent_id = user_res.agentId or 2
#             xc_fca.card_id = favorable_card_id
#             xc_fca.serial_type = serial_type
#             xc_fca.price = amount
#             xc_fca.channel = channel
#             xc_fca.trade_no = trade_no
#             xc_fca.created_at = datetime.now()
#             xc_fca.updated_at = datetime.now()
#             dao_session.session().add(xc_fca)
#             dao_session.session().flush()
#
#             service_id = find_favorable_card.service_id
#             config_id = find_favorable_card.config_id
#             days = find_favorable_card.card_time
#             favorable_user = FavorableCardService.get_favorable_user(service_id, object_id)
#             if favorable_user:
#                 end_time = favorable_user.end_time
#                 # 用户优惠卡已过期，则从当前时间开始计算过期时间
#                 if datetime.now() > end_time:
#                     favorable_user.end_time = datetime.now() + timedelta(days=days)
#                 # 用户优惠卡未过期，累计骑行卡使用时间
#                 else:
#                     favorable_user.end_time = end_time + timedelta(days=days)
#             else:
#                 # 没有用户购买优惠卡的记录，新建一条用户的购买记录
#                 xc_fcu = XcMieba2FavorableCardUser()
#                 xc_fcu.service_id = service_id
#                 xc_fcu.config_id = config_id
#                 xc_fcu.object_id = object_id
#                 xc_fcu.begin_time = datetime.now()
#                 xc_fcu.end_time = datetime.now() + timedelta(days=days)
#                 xc_fcu.created_at = datetime.now()
#                 xc_fcu.updated_at = datetime.now()
#                 dao_session.session().add(xc_fcu)
#
#             dao_session.session().commit()
#             logger.info(f"favorable card buy success, data_dict:{data_dict}")
#         except Exception as e:
#             logger.error(f"favorable card buy failed，data_dict: {data_dict}, e: {e}")
#             # 插入失败后，将该消息重新插入到队列中
#             dao_session.session().rollback()
#             dao_session.session().close()
#             PayNotifyNxLock().del_pay_notify_lock(PayKey.FAVORABLE_CARD.value, trade_no)
#             raise KafkaRetry()
#
#
# # 优惠卡支付创建
# class FavorableCardCreateService(BusinessFullPayInterface):
#     def __init__(self, info):
#         super().__init__()
#         self.channel, self.object_id, self.amount, self.serial_type, self.favorable_card_id, \
#         self.open_id, self.user_auth_code, self.card_id, self.front_url, self.buyer_id, self.single_split = info
#         self.subject = "购买优惠卡"
#         self.url = "/favorableCard"
#
#     @staticmethod
#     def get_favorable_card_by_id(favorable_card_id):
#         favorable_card = dao_session.session().query(XcMieba2FavorableCard). \
#             filter(XcMieba2FavorableCard.id == favorable_card_id).first()
#         return favorable_card
#
#     def check_favorable_card(self):
#         favorable_verify = FavorableCardService().favorable_verify(self.object_id, self.favorable_card_id)
#         if not favorable_verify['suc']:
#             return favorable_verify
#         total_fee = favorable_verify.get("info", {}).get("price", 0)
#         return {"suc": True, "price": total_fee}
#
#     def _check(self):
#         user_res = PayDBService(self.object_id).get_user()
#         if not user_res:
#             return {"suc": False, "info": '用户不存在'}
#         favorable = self.get_favorable_card_by_id(self.favorable_card_id)
#         if not favorable:
#             return {"suc": False, "info": '该优惠卡的信息有误'}
#         return {"suc": True, "info": ''}
#
#     def wx_pay(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             return favorable_verify
#         total_fee = favorable_verify['price']
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "favorableCardId": self.favorable_card_id
#         })
#         pay_params = WePayService().wx_pay(
#             self.subject, attach, total_fee, PayHelper.rand_str_32(), PayHelper.rand_str_24(), self.url,
#             RIDING_CHANNEL_TYPE.WEPAY.value)
#         if not pay_params:
#             raise MbException("支付订单生成失败")
#         return pay_params['pay_data']
#
#     def wx_lite(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             logger.info(f"favorable card is error: favorable verify: {favorable_verify}")
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "favorableCardId": self.favorable_card_id
#         })
#         out_trade_no = PayHelper.rand_str_24()
#         pay_params = WeLiteService().wx_pay(
#             self.subject, attach, total_fee, PayHelper.rand_str_32(), out_trade_no, self.url,
#             RIDING_CHANNEL_TYPE.WXLITE.value, self.open_id
#         )
#         logger.info(f"favorable card wxlite pay params: {pay_params}")
#         if pay_params:
#             pre_value = {
#                 "object_id": self.object_id,
#                 "serial_type": self.amount,
#                 "amount": self.amount,
#                 "trade_no": out_trade_no,
#                 "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
#                 "favorable_card_id": self.favorable_card_id,
#                 "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
#                 "app_id": cfg.get("wx_lite", {}).get("appid", "")
#             }
#             WeLiteService().set_info(pay_type="favorable_card",
#                                      value={json.dumps(pre_value): str(time.time()).split('.')[0]})
#             return pay_params['pay_data']
#         else:
#             raise MbException("支付订单生成失败")
#
#     def ali_pay(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         passback_params = {"objectId": self.object_id, "serialType": self.serial_type,
#                            "favorableCardId": self.favorable_card_id}
#         biz_countent = {
#             "subject": self.subject,
#             "out_trade_no": PayHelper.rand_str_32(),
#             "timeout_express": '2h',
#             "product_code": 'QUICK_MSECURITY_PAY',
#             "passback_params": passback_params,
#             "total_amount": round(total_fee / 100, 2)
#         }
#         sign_status = AliService().sign(self.url, biz_countent)
#         logger.info("alipay buy favorable card, {},{},{}".format(
#             self.object_id, RIDING_CHANNEL_TYPE.ALIPAY.value, self.amount))
#         return sign_status
#
#     def ali_lite(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         out_trade_no = PayHelper.rand_str_24
#         passback_params = {"objectId": self.object_id, "serialType": self.serial_type,
#                            "favorableCardId": self.favorable_card_id}
#         pay = AliLiteService().pay(self.buyer_id, out_trade_no, total_fee, self.url, self.subject, passback_params)
#         if not pay:
#             raise MbException("支付订单生成失败")
#         return pay
#
#     def union_pay_app(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         attach = json.dumps({"objectId": self.object_id, "serialType": self.serial_type,
#                              "favorableCardId": self.favorable_card_id})
#         if not total_fee:
#             raise MbException("支付订单生成失败")
#         pay_params = UnionPayForApp().pay(total_fee, attach, PayHelper.rand_str_24(), self.url)
#         if not pay_params:
#             logger.info('union app 支付订单生成失败')
#             raise MbException("支付订单生成失败")
#         logger.info('union app create_order success')
#         return pay_params
#
#     def union_pay_code(self):
#         favorable_verify = self.check_favorable_card()
#         if not favorable_verify['suc']:
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         uc = UnionPayForCode()
#         out_trade_no = PayHelper.rand_str_24()
#         get_union_user_id = uc.get_unionpay_app_user_id(out_trade_no, self.user_auth_code, self.card_id)
#         if not get_union_user_id.get('suc'):
#             raise MbException("获取用户认证失败")
#         order_id = get_union_user_id.get('order_id')
#         app_user_id = get_union_user_id.get('app_user_id')
#
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "favorable_card_id": self.card_id,
#             "backFrontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("normal_front_url")
#         })
#         pay_params = UnionPayForCode().pay(order_id, self.card_id, app_user_id, total_fee, attach)
#         if not pay_params:
#             raise MbException("订单生成失败")
#         return pay_params
#
#     def union_pay_lite(self):
#         favorable_verify = self.check_favorable_card()
#         user = PayDBService(self.object_id).get_user()
#         service_id = user.serviceId
#         if not favorable_verify['suc']:
#             raise MbException(favorable_verify['info'])
#         total_fee = favorable_verify['price']
#         Unionpay_WXlite = SubAccount("favorableCard", service_id=service_id,
#                                      single=self.single_split).get_payment_config()
#         body = "{}-购买优惠卡".format(Unionpay_WXlite.get('mchName'))
#         notify_url = "{}/favorableCard/unionpayNotify".format(
#             Unionpay_WXlite.get("notify_url_head").replace("ebike", "anfu"))
#         trade_no = PayHelper.rand_str_24()
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "favorableCardId": self.favorable_card_id
#         })
#         pay_params = UnionPayForWXLite().pay(
#             self.open_id, total_fee, body, notify_url, trade_no, PayHelper.rand_str_32(), attach, Unionpay_WXlite)
#         if not pay_params.get('suc'):
#             raise MbException("订单生成失败")
#         return pay_params.get('data')
    pass


# 优惠卡支付回调
class FavorableCardNotifyService(BusinessFullPayInterface, NotifyMixing):
    #
    # def __init__(self, channel):
    #     super().__init__()
    #     self.subject = "购买优惠卡"
    #     self.url = "/favorableCard"
    #     self.channel = channel
    #
    # @staticmethod
    # def get_favorable_card_by_id(favorable_card_id):
    #     favorable_card = dao_session.session().query(XcMieba2FavorableCard.id == favorable_card_id).first()
    #     return favorable_card
    pass

    def wx_pay(self, xml) -> str:
        wx_notify = self.get_notify_func()(xml)
        if wx_notify['suc']:
            attach_json = wx_notify.get("attach_json")
            notify = wx_notify.get("notify")
            if attach_json and attach_json.get('objectId') and attach_json.get('serialType') and attach_json.get(
                    'favorableCardId') and notify:
                object_id = attach_json.get('objectId')
                serial_type = attach_json.get('serialType')
                favorable_card_id = attach_json.get('favorableCardId')
                time_end = notify.get('time_end')
                transaction_id = notify.get('transaction_id')
                out_trade_no = notify.get('out_trade_no')
                total_fee = notify.get('total_fee')
            else:
                logger.info(f"wx_pay_notify is failed, attach_json:{attach_json}, notify:{notify}")
                return WX_FAILED_XML
        else:
            logger.info(f"favorable card send message is error, wx_notify: {wx_notify}")
            return WX_FAILED_XML
        amount = total_fee
        state = FavorableCardService.favorable_card_record(object_id, favorable_card_id,
                                                           transaction_id, serial_type, self.channel, time_end, amount)
        if not state:
            logger.info(f"favorable card send message is error, state: {state}")
            return WX_ERROR_XML
        logger.info(f"{self.channel} is success, "
                    f"object_id:{object_id}, favorable_card_id:{favorable_card_id}, total_fee:{total_fee}")
        pre_value = {
            "object_id": object_id,
            "serial_type": serial_type,
            "amount": total_fee,
            "trade_no": out_trade_no,
            "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "favorable_card_id": favorable_card_id,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        if self.channel == RIDING_CHANNEL_TYPE.WXLITE.value:
            WeLiteService.del_info("favorable_card", json.dumps(pre_value))
        return WX_SUCCESS_XML

    def wx_lite(self, xml) -> str:
        return self.wx_pay(xml)

    def ali_pay(self, notify):
        res = self.get_notify_func()(notify)
        if not res['suc']:
            return ALI_FAILED_RESP
        total_amount = notify.get('total_amount')
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        params = json.loads(notify.get("passback_params"))
        object_id = params.get('objectId')
        serial_type = params.get('serialType')
        favorable_card_id = params.get('favorableCardId')
        amount = int(float(total_amount) * 100)
        favorable = FavorableCardService.favorable_card_record(
            object_id, favorable_card_id, trade_no, serial_type, self.channel, gmt_payment, amount)
        if not favorable:
            logger.info('信息写入失败，ali_lite_notify is failed')
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def ali_lite(self, notify) -> dict:
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return ALI_FAILED_RESP
        total_amount = notify.get('total_amount')
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        out_trade_no = notify.get('out_trade_no')
        serial_type = notify.get('serialType')
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        favorable_card_id = params.get('favorableCardId')
        channel = RIDING_CHANNEL_TYPE.ALIPAYLITE.value
        amount = int(float(total_amount) * 100)
        favorable = FavorableCardService.favorable_card_record(
            object_id, favorable_card_id, trade_no, serial_type, channel, gmt_payment, amount)
        if not favorable:
            logger.info('优惠卡信息写入失败，ali_lite_complete_order is failed')
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def union_pay_app(self, notify) -> dict:
        query_id = notify.get("queryId")
        logger.info(f'union app notify query_id:{query_id}')
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return UNION_FAILED_RESP
        attach = decode_notify.get('info')
        object_id = attach.get('objectId')
        serial_type = attach.get('serialType')
        favorable_card_id = attach.get('favorableCardId')
        txn_time = notify.get('txnTime')
        txn_amt = notify.get('txnAmt')
        user = PayDBService(object_id).get_user()
        if not user:
            return UNION_FAILED_RESP
        amount = int(txn_amt)
        state = FavorableCardService.favorable_card_record(object_id, favorable_card_id,
                                                           query_id, serial_type, self.channel, txn_time, amount)
        if not state:
            logger.info('优惠卡信息写入失败')
            return UNION_FAILED_RESP
        return UNION_SUCCESS_RESP

    def union_pay_code(self, notify):
        return self.union_pay_app(notify)

    # unionpay wxlite回调入口
    def union_pay_lite(self, xml):
        wx_notify = UnionPayForWXLite().notify(xml)
        if wx_notify['suc']:
            attach_json = wx_notify.get("attach_json")
            notify = wx_notify.get("notify")
            # out_trade_no = notify.get('out_trade_no')
            if attach_json and attach_json.get('objectId') and attach_json.get("serialType") and attach_json.get(
                    "favorableCardId"):
                object_id = attach_json.get('objectId')
                serial_type = attach_json.get("serialType")
                favorable_card_id = attach_json.get("favorableCardId")
                time_end = notify['time_end']
                transaction_id = notify['transaction_id']
                total_fee = notify['total_fee']
            else:
                logger.info("回调信息中未找到objectId、serialType、favorableCardId")
                return WX_FAILED_XML
            # if get_attach_out_trade_no != out_trade_no:
            #     logger.info("前后生成的订单单号不一致")
            #     return WX_FAILED_XML
        else:
            logger.info(f"favorable card union_pay_lite is error, wx_notify:{wx_notify}")
            return WX_ERROR_XML
        channel = RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value
        state = FavorableCardService.favorable_card_record(object_id, favorable_card_id,
                                                           transaction_id, serial_type, channel, time_end, total_fee)
        if not state:
            logger.info('优惠卡信息写入失败')
            return WX_ERROR_XML
        return WX_SUCCESS_XML


# 优惠卡退款
class FavorableCardRefundService(BusinessFullPayInterface):
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
    #                 XcEbikeAccount2.type == SERIAL_TYPE.FAVORABLE_CARD_REFUND.value).first()
    #     if refund_account:
    #         return {"suc": False, "info": "已退款"}
    #     account = dao_session.session().query(XcEbikeAccount2) \
    #         .filter(XcEbikeAccount2.trade_no == self.trade_no,
    #                 XcEbikeAccount2.objectId == self.object_id,
    #                 XcEbikeAccount2.type == SERIAL_TYPE.FAVORABLE_CARD_ADD_PAY.value).first()
    #     if account and (account.channel != self.channel or account.amount < self.refund_fee):
    #         return {"suc": False, "info": "退款失败0"}
    #     else:
    #         self.total_fee = account.amount
    #         return {"suc": True, "info": ""}
    #
    # @staticmethod
    # def get_favorable_card_by_id(favorable_card_id):
    #     favorable_card = dao_session.session().query(XcMieba2FavorableCard.id == favorable_card_id).first()
    #     return favorable_card
    #
    # @staticmethod
    # def get_favorable_card_account(trade_no):
    #     favorable_card = dao_session.session().query(XcMieba2FavorableCardAccount.trade_no == trade_no).first()
    #     return favorable_card
    #
    # def favorable_refund(self, refund_func):
    #     f = FavorableCardService()
    #     refund_verify = f.user_can_refund_favorable_card(self.object_id, self.trade_no, self.refund_fee)
    #     if not refund_verify.get('suc'):
    #         return refund_verify
    #
    #     verify_data = refund_verify.get("info")
    #     res = refund_func(self.object_id, self.trade_no, self.refund_fee, self.total_fee)
    #     if res["suc"]:
    #         is_refund = f.favorable_card_refund(self.object_id, self.refund_fee, self.trade_no,
    #                                             verify_data.get("favorable_card_record"),
    #                                             verify_data.get("account_record"))
    #         if is_refund:
    #             return {"suc": True, "info": "退款成功"}
    #     return {"suc": False, "info": "退款失败"}
    pass

    def wx_pay(self):
        return self.favorable_refund(WePayService().refund)

    def wx_lite(self):
        return self.favorable_refund(WeLiteService().refund)

    def ali_pay(self):
        return self.favorable_refund(AliService().refund)

    def ali_lite(self):
        return self.favorable_refund(AliLiteService().refund)

    def union_pay_app(self):
        return self.favorable_refund(UnionPayForApp().refund)

    def union_pay_code(self):
        return self.favorable_refund(UnionPayForCode().refund)

    def union_pay_lite(self):
        return self.favorable_refund(UnionPayForWXLite().refund)
