import json
import logging
import time

from model.all_model import *
from service.kafka import KafkaRetry, PayKey
from service.kafka.producer import kafka_client
from service.payment import PayDBService, UserDBService
from service.payment import PayNotifyNxLock
from service.payment import PayHelper
from service.payment.pay_utils.wxlitepay import WeLiteService
from service.payment.payment_interface import BusinessFullPayInterface, NotifyMixing
from service.payment.pay_utils.alilitepay import AliLiteService
from service.payment.pay_utils.alipay import AliService
from service.payment.pay_utils.pay_constant import *
from service.payment.pay_utils.sub_account import SubAccount
from service.payment.pay_utils.unionpay_app import UnionPayForApp
from service.payment.pay_utils.unionpay_code import UnionPayForCode
from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
from service.payment.pay_utils.wepay import WePayService
from service.super_riding_card.internal import InternalService
from mbutils import dao_session, logger, cfg, MbException
#  骑行卡业务重复部分，共用函数
from utils.constant.account import RIDING_CHANNEL_TYPE, SERIAL_TYPE
from utils.constant.redis_key import ALI_TRADE_INFO, REFUND_RIDINGCARD_LOCK
from utils.constant.user import UserState


class RidingCardService:
#
#     @staticmethod
#     def is_user_get_riding_card(object_id):
#         try:
#             user_riding_card = dao_session.session().query(XcEbike2RidingCard).filter(
#                 XcEbike2RidingCard.objectId == object_id).one()
#             if int(time.mktime(user_riding_card.cardExpiredDate.timetuple())) - int(
#                     time.mktime(datetime.now().timetuple())) <= 0:
#                 return False
#         except Exception as e:
#             logger.error(f"is_user_get_riding_card is error, e:{e}")
#             return False
#         return True
#
#     # 骑行卡创建订单重复部分
#     @staticmethod
#     def ridingcard_create_order(object_id):
#         rd_db_service = PayDBService(object_id)
#         user_res = rd_db_service.get_user()
#         if not user_res:
#             return {"suc": False, "info": '用户不存在'}
#         return {"suc": True, "info": user_res.serviceId}
#
#     """获取骑行卡的配置信息"""
#
#     def get_ridingcard_config(self, config_id):
#         config = dao_session.session().query(XcEbike2RidingConfig).filter(
#             XcEbike2RidingConfig.id == config_id,
#             XcEbike2RidingConfig.state == 1).first()
#         return config
#
#     @staticmethod
#     def user_can_refund_ridingcard(object_id, trade_no, refund_fee):
#         """
#         校验用户能否进行骑行卡退款
#         :param object_id: 用户id
#         :param trade_no: 订单号
#         :param refund_fee: 退款金额
#         :return:
#         """
#         refund_lock = dao_session.redis_session.r.set(REFUND_RIDINGCARD_LOCK.format(object_id=object_id),
#                                                       1, nx=True, px=5000)
#         if not refund_lock:
#             return {"suc": False, "info": "骑行卡退款中,请5s后重试"}
#         refund_fee = int(refund_fee)
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
#         ridingcard_record = dao_session.session().query(XcEbike2RidingcardAccount).filter(
#             XcEbike2RidingcardAccount.trade_no == trade_no).first()
#         if not ridingcard_record:
#             return {"suc": False, "info": "没有该流水的购买记录,请先进行核,trade_no: {trade_no}".format(trade_no=trade_no)}
#         account_record = dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no,
#                                                                              XcEbikeAccount2.objectId == object_id,
#                                                                              XcEbikeAccount2.amount > 0).first()
#         if not account_record:
#             return {"suc": False, "info": "流水号: {trade_no},没有对应的购买记录数据,请先进行核实".format(trade_no=trade_no)}
#
#         has_refund_record = account_record.refund_amount or 0
#         if has_refund_record + refund_fee > account_record.amount:
#             could_refund_amount = (account_record.amount - has_refund_record) / 100 if (account_record.
#                                                                                         amount - has_refund_record) > 0 else 0
#             return {"suc": False,
#                     "info": "退款金额超出,当前可退款余额: {could_refund_amount}元".format(could_refund_amount=could_refund_amount)}
#         return {"suc": True, "info": {"ridingcard_record": ridingcard_record, "account_record": account_record}}
#
#     @staticmethod
#     def refund_modify_db(object_id, trade_no, refund_fee, ridingcard_record, account_record):
#         serial_type = SERIAL_TYPE.RIDING_CARD_REFUND.value
#         account_update_amount = account_record.refund_amount + refund_fee
#         # XcEbike2RidingcardAccount
#         riding_card_record_info = {}
#         riding_card_record_info['objectId'] = ridingcard_record.objectId
#         riding_card_record_info['configId'] = ridingcard_record.configId
#         riding_card_record_info['channel'] = ridingcard_record.channel
#         riding_card_record_info['trade_no'] = ridingcard_record.trade_no
#         riding_card_record_info['content'] = ridingcard_record.content
#         riding_card_record_info['agentId'] = ridingcard_record.agentId
#         riding_card_record_info['serviceId'] = ridingcard_record.serviceId
#         riding_card_record_info['ridingCardName'] = ridingcard_record.ridingCardName
#
#         riding_card_record_info['money'] = refund_fee * -1
#         riding_card_record_info['type'] = serial_type
#         xc_rca = XcEbike2RidingcardAccount(**riding_card_record_info)
#         dao_session.session().add(xc_rca)
#         dao_session.session().flush()
#
#         # XcEbikeAccount2
#         order_info = {
#             "objectId": object_id,
#             "amount": refund_fee * -1,
#             "type": serial_type,
#             "channel": account_record.channel,
#             "trade_no": trade_no,  # refund返回 trade_no
#             "orderId": xc_rca.id,
#             "paid_at": datetime.now()
#         }
#         xc_a = XcEbikeAccount2(**order_info)
#         dao_session.session().add(xc_a)
#
#         # 修改原来的购买流水数据,金额大于0的那一条
#         try:
#             dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.objectId == object_id,
#                                                                 XcEbikeAccount2.trade_no == trade_no,
#                                                                 XcEbikeAccount2.amount > 0) \
#                 .update({"refund_amount": account_update_amount})
#             dao_session.session().commit()
#         except Exception as e:
#             logger.info(f"err: {e}, object_id: {object_id}, refund_fee: {refund_fee}, trade_no: {trade_no}")
#             dao_session.session().rollback()
#             dao_session.session().close()
#             return False
#         return True
#
#     @staticmethod
#     def riding_card_record(serial_type, object_id, trade_no, amount, channel, time_end, riding_card_id):
#         """
#         object_id = data_dict.get("object_id", "")
#         config_id = data_dict.get("config_id", "")
#         trade_no = data_dict.get("trade_no", "")
#         amount = data_dict.get("amount", "")
#         gmt_create = data_dict.get("gmt_create", "")
#         riding_card_type = data_dict.get("riding_card_type", "")
#         channel = data_dict.get("channel", "")
#         """
#         wallet_dict = {
#             "riding_card_type": serial_type,
#             "object_id": object_id,
#             "trade_no": trade_no,
#             "amount": amount,
#             "channel": channel,
#             "gmt_create": time_end,
#             "config_id": riding_card_id,
#         }
#         return kafka_client.pay_send(wallet_dict, key=PayKey.RIDING_CARD.value)
#
#     """
#     根据用户购买的骑行卡类型，新建或者更新用户的骑行卡信息
#     """
#
#     @staticmethod
#     def handle_riding_card(data_dict):
#         object_id = data_dict.get("object_id", "")
#         config_id = data_dict.get("config_id", "")
#         trade_no = data_dict.get("trade_no", "")
#         amount = data_dict.get("amount", "")
#         gmt_create = data_dict.get("gmt_create", "")
#         riding_card_type = data_dict.get("riding_card_type", "")
#         channel = data_dict.get("channel", "")
#         try:
#             check = PayNotifyNxLock.check_trade_no(PayKey.RIDING_CARD.value, trade_no)
#             if not check["suc"]:
#                 logger.info("重复回调锁")
#                 return
#             card_info = dao_session.session().query(XcEbike2RidingcardAccount).filter(
#                 XcEbike2RidingcardAccount.trade_no == trade_no).first()
#             if card_info:
#                 logger.info("重复回调")
#                 return
#             user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
#             if not user_info:
#                 logger.info("riding_card,回调时用户信息获取失败，object_id: {}, trade_no:{}".format(object_id, trade_no))
#                 return
#                 # 获取骑行卡的配置信息, 通过ridingCardId直接定位，nodejs通过serialType，objectId
#             riding_card_config = dao_session.session().query(XcEbike2RidingConfig). \
#                 filter(XcEbike2RidingConfig.id == config_id).first()
#             if not riding_card_config:
#                 logger.info("riding_card,回调时骑行卡信息获取失败，object_id: {}, trade_no:{}".format(object_id, trade_no))
#                 return
#                 # TODO：合并超级骑行卡分支后进行添加
#             riding_card_name = ""
#             if riding_card_type >= 310:
#                 riding_card_name = json.loads(riding_card_config.content).get("ridingCardName", "")
#
#             order_info = {
#                 "objectId": object_id,
#                 "amount": amount,
#                 "type": riding_card_type,
#                 "channel": channel,
#                 "trade_no": trade_no,
#                 "paid_at": gmt_create,
#                 "createdAt": datetime.now(),
#             }
#             # 插入流水表
#             xc_a = XcEbikeAccount2(**order_info)
#             dao_session.session().add(xc_a)
#             info = {
#                 "objectId": object_id,
#                 "configId": config_id,
#                 "type": riding_card_type,
#                 "money": amount,
#                 "channel": channel,
#                 "trade_no": trade_no,
#                 "content": riding_card_config.content,
#                 "agentId": user_info.agentId or 2,
#                 "serviceId": user_info.serviceId or 1,
#                 "ridingCardName": riding_card_name
#             }
#             xc_rca = XcEbike2RidingcardAccount(**info)
#             dao_session.session().add(xc_rca)
#
#             # 一起提交
#             dao_session.session().commit()
#             InternalService().add_card((config_id, object_id))
#             logging.info("回调时骑行卡信息获取成功，object_id: {}, trade_no:{}".format(object_id, trade_no))
#         except Exception as ex:
#             logger.info(f"骑行卡回调失败，ex: {ex}")
#             dao_session.session().rollback()
#             dao_session.session().close()
#             PayNotifyNxLock().del_pay_notify_lock(PayKey.RIDING_CARD.value, trade_no)
#             raise KafkaRetry()
#
#
# class RidingCardCreateService(BusinessFullPayInterface):
#     def __init__(self, info):
#         super().__init__()
#         self.object_id, self.amount, self.channel, self.serial_type, self.riding_card_id, self.open_id, \
#         self.user_auth_code, self.car_id, self.front_url, self.buyer_id, self.single_split = info
#         self.subject = "购买骑行卡"
#         self.url = '/ridingCard'
#
#     def _check(self) -> dict:
#         r = RidingCardService().ridingcard_create_order(self.object_id)
#         if not r['suc']:
#             return {"suc": False, "info": "未发现该骑行卡"}
#         return {"suc": True, "info": ""}
#
#     def wx_pay(self):
#         subject = str(PAY_CONFIG.WEPAY_CFG.get('mchName')) + self.subject
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "ridingCardId": self.riding_card_id
#         })
#         payParams = WePayService().wx_pay(
#             subject, attach, self.amount, PayHelper.rand_str_32(),
#             PayHelper.rand_str_24(), self.url, RIDING_CHANNEL_TYPE.WEPAY.value)
#         if payParams:
#             return payParams['pay_data']
#         else:
#             raise MbException("订单生成失败")
#
#     def wx_lite(self):
#         # wx小程序支付未传商品价格，直接从骑行卡配置里去查找骑行卡价格
#         riding_config = RidingCardService().get_ridingcard_config(self.riding_card_id)
#         if not riding_config:
#             logger.info("config is null")
#             raise MbException("订单生成失败")
#         content = riding_config.content
#         amount = json.loads(content).get("curCost")
#         if not amount:
#             raise MbException("查询不到骑行卡金额")
#
#         subject = str(PAY_CONFIG.WXLITE_CFG.get('mchName')) + self.subject
#         out_trade_no = PayHelper.rand_str_24()
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "ridingCardId": self.riding_card_id
#         })
#         payParams = WeLiteService().wx_pay(subject, attach, amount, PayHelper.rand_str_32(), out_trade_no,
#                                            self.url, RIDING_CHANNEL_TYPE.WXLITE.value, self.open_id)
#         if payParams:
#             pre_value = {
#                 "object_id": self.object_id,
#                 "serial_type": self.serial_type,
#                 "amount": self.amount,
#                 "trade_no": out_trade_no,
#                 "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
#                 "riding_card_id": self.riding_card_id,
#                 "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
#                 "app_id": cfg.get("wx_lite", {}).get("appid", "")
#             }
#             WeLiteService().set_info(pay_type="riding_card",
#                                      value={json.dumps(pre_value): str(time.time()).split('.')[0]})
#             return payParams['pay_data']
#         else:
#             raise MbException("订单生成失败")
#
#     def ali_pay(self):
#         passback_params = {
#             "objectId": self.object_id,
#             "ridingCardId": self.riding_card_id,
#             "serialType": self.serial_type
#         }
#         biz_content = {
#             "subject": self.subject,
#             "out_trade_no": PayHelper.rand_str_32(),
#             "timeout_express": "2h",
#             "product_code": "QUICK_MSECURITY_PAY",
#             "passback_params": json.dumps(passback_params),
#             "total_amount": round(self.amount / 100, 2)
#         }
#         sign_status = AliService().sign(self.url, biz_content)
#         logger.info(
#             "alipay buy favorable card, {},{},{}".format(self.object_id, RIDING_CHANNEL_TYPE.ALIPAY.value, self.amount))
#         return sign_status
#
#     def ali_lite(self):
#         riding_config = RidingCardService().get_ridingcard_config(self.riding_card_id)
#         if not riding_config:
#             logger.info("config is null")
#             raise MbException("订单生成失败")
#         passback_params = {
#             "objectId": self.object_id,
#             "ridingCardId": self.riding_card_id,
#             "serialType": self.serial_type
#         }
#         content = riding_config.content
#         amount = json.loads(content).get("curCost")
#         out_trade_no = PayHelper.rand_str_24()
#         pay = AliLiteService().pay(self.buyer_id, out_trade_no, amount, self.url, self.subject, passback_params)
#         if not pay:
#             raise MbException("订单生成失败")
#         return pay
#
#     def union_pay_app(self):
#         riding_config = RidingCardService().get_ridingcard_config(self.riding_card_id)
#         if not riding_config:
#             logger.info("config is null")
#             raise MbException("订单生成失败")
#         content = riding_config.content
#         total_fee = json.loads(content).get("curCost")
#         if not total_fee:
#             raise MbException("订单生成失败")
#         attach = json.dumps({"objectId": self.object_id})
#         pay_params = UnionPayForApp().pay(total_fee, attach, PayHelper.rand_str_24(), self.serial_type,
#                                           self.riding_card_id)
#         if not pay_params:
#             raise MbException("订单生成失败")
#         return pay_params
#
#     def union_pay_code(self):
#         riding_config = RidingCardService().get_ridingcard_config(self.riding_card_id)
#         if not riding_config:
#             logger.info("config is null")
#             raise MbException("订单生成失败")
#         content = riding_config.content
#         total_fee = json.loads(content).get("curCost")
#         if not total_fee:
#             raise MbException("获取骑行卡支付金额失败")
#
#         uc = UnionPayForCode()
#         get_union_user_id = uc.get_unionpay_app_user_id(PayHelper.rand_str_24(), self.user_auth_code, self.car_id)
#         if not get_union_user_id.get('suc'):
#             raise MbException("获取用户认证失败")
#         order_id = get_union_user_id.get('order_id')
#         app_user_id = get_union_user_id.get('app_user_id')
#
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "ridingCardId": self.riding_card_id,
#             "backFrontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("normal_front_url")
#         })
#         pay_params = UnionPayForCode().pay(order_id, self.car_id, app_user_id, total_fee, attach)
#         if not pay_params:
#             raise MbException("订单生成失败")
#         return pay_params
#
#     def union_pay_lite(self):
#         r = RidingCardService().ridingcard_create_order(self.object_id)
#         service_id = r.get('info')
#         riding_config = RidingCardService().get_ridingcard_config(self.riding_card_id)
#         if not riding_config:
#             logger.info("config is null")
#             raise MbException("订单生成失败")
#         content = riding_config.content
#         total_fee = json.loads(content).get("curCost")
#         Unionpay_WXlite = SubAccount("ridingCard", service_id, self.single_split).get_payment_config()
#         notify_url_head = Unionpay_WXlite.get("notify_url_head").replace("ebike", "anfu")
#         # notify_url_head = "http://denghui.c8612afb6df52455e9ac5a9a87a652908.cn-shenzhen.alicontainer.com/anfu/pay"  # 测试使用
#         body = "{}-购买骑行卡".format(Unionpay_WXlite.get('mchName'))
#         notify_url = "{}/ridingCard/unionpayNotify".format(notify_url_head)
#         out_trade_no = PayHelper.rand_str_24()
#         attach = json.dumps({
#             "objectId": self.object_id,
#             "serialType": self.serial_type,
#             "ridingCardId": self.riding_card_id
#         })
#         pay_params = UnionPayForWXLite().pay(self.open_id, total_fee, body, notify_url, out_trade_no,
#                                              PayHelper.rand_str_32(), attach, Unionpay_WXlite)
#         if not pay_params["suc"]:
#             raise MbException('订单生成失败')
#         return pay_params["data"]
#
#
# class RidingCardNotifyService(BusinessFullPayInterface, NotifyMixing):
#     def __init__(self, channel):
#         super().__init__()
#         self.subject = "购买骑行卡"
#         self.url = '/ridingCard'
#         self.channel = channel
#
#     def wx_pay(self, xml) -> str:
#         wx_notify = self.get_notify_func()(xml)
#         if not wx_notify['suc']:
#             return WX_FAILED_XML
#         attach_json = wx_notify.get("attach_json")
#         notify = wx_notify.get("notify")
#         if not (attach_json.get('objectId') and attach_json.get('serialType') and attach_json.get('ridingCardId')):
#             return WX_FAILED_XML
#         object_id = attach_json.get('objectId')
#         serial_type = attach_json.get('serialType')
#         riding_card_id = attach_json.get('ridingCardId')
#         time_end = notify.get('time_end')
#         transaction_id = notify.get('transaction_id')
#         out_trade_no = notify.get('out_trade_no')
#         total_fee = notify.get('total_fee')
#         set_ridingcard_data = RidingCardService.riding_card_record(serial_type, object_id, transaction_id, total_fee,
#                                                                    self.channel, time_end,
#                                                                    riding_card_id)
#         if not set_ridingcard_data:
#             logger.info('骑行卡信息写入失败')
#             return WX_ERROR_XML
#         pre_value = {
#             "object_id": object_id,
#             "serial_type": serial_type,
#             "amount": total_fee,
#             "trade_no": out_trade_no,
#             "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
#             "riding_card_id": riding_card_id,
#             "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
#             "app_id": cfg.get("wx_lite", {}).get("appid", "")
#         }
#         if self.channel == RIDING_CHANNEL_TYPE.WXLITE.value:
#             WeLiteService.del_info("riding_card", json.dumps(pre_value))
#         return WX_SUCCESS_XML
#
#     def wx_lite(self, xml):
#         return self.wx_pay(xml)
#
#     def ali_pay(self, notify: dict):
#         decode_notify = self.get_notify_func()(notify)
#         if not decode_notify['suc']:
#             return ALI_FAILED_RESP
#
#         ali = AliService()
#         sign_status = ali.validate_verify(notify)
#         if not sign_status['suc']:
#             logger.error('pay/ridingCard/alipayNotify ', sign_status['info'])
#             return ALI_FAILED_RESP
#
#         # passback_params ={"objectId": objectId, "serialType": serialType, "ridingCardId": ridingCardId}
#         passback_params = json.loads(notify["passback_params"]) if isinstance(notify["passback_params"], str) else \
#             notify["passback_params"]
#         objectId = passback_params["objectId"]
#         serialType = passback_params['serialType']
#         ridingCardId = passback_params['ridingCardId']
#         trade_no = notify["trade_no"]
#         total_amount = notify["total_amount"]
#         gmt_create = notify["gmt_create"]
#         set_ridingcard_data = RidingCardService.riding_card_record(serialType, objectId, trade_no, total_amount,
#                                                                    RIDING_CHANNEL_TYPE.ALIPAY.value, gmt_create,
#                                                                    ridingCardId)
#         if not set_ridingcard_data:
#             logger.info('骑行卡信息写入失败')
#             return ALI_FAILED_RESP
#         return ALI_SUCCESS_RESP
#
#     def ali_lite(self, notify) -> dict:
#         decode_notify = self.get_notify_func()(notify)
#         if not decode_notify['suc']:
#             return ALI_FAILED_RESP
#         total_amount = int(float(notify.get('total_amount')) * 100)
#         trade_no = notify.get('trade_no')
#         gmt_payment = notify.get('gmt_payment')
#         out_trade_no = notify.get('out_trade_no')
#         params = json.loads(notify.get("passback_params"))
#         object_id = params.get("objectId")
#         riding_card_id = params.get("ridingCardId")
#         set_ridingcard_data = RidingCardService.riding_card_record(type, object_id, trade_no, total_amount,
#                                                                    RIDING_CHANNEL_TYPE.ALIPAYLITE.value, gmt_payment,
#                                                                    riding_card_id)
#         if not set_ridingcard_data:
#             logger.info('骑行卡信息写入失败')
#             return ALI_FAILED_RESP
#         return ALI_SUCCESS_RESP
#
#     def union_pay_app(self, notify) -> dict:
#         query_id = notify.get("queryId")
#         decode_notify = self.get_notify_func()(notify)
#         if not decode_notify['suc']:
#             return UNION_FAILED_RESP
#         attach = decode_notify.get('info')
#         object_id = attach.get('objectId')
#         type = attach.get('serialType')
#         riding_card_id = attach.get('ridingCardId')
#         txn_time = notify.get('txnTime')
#         txn_amt = notify.get('txnAmt')
#         user = PayDBService(object_id).get_user()
#         if not user:
#             return UNION_FAILED_RESP
#
#         set_ridingcard_data = RidingCardService.riding_card_record(type, object_id, query_id, txn_amt,
#                                                                    self.channel, txn_time,
#                                                                    riding_card_id)
#         if not set_ridingcard_data:
#             logger.info('骑行卡信息写入失败')
#             return UNION_FAILED_RESP
#         return UNION_SUCCESS_RESP
#
#     def union_pay_code(self, notify):
#         return self.union_pay_app(notify)
#
#     def union_pay_lite(self, xml):
#         wx_notify = UnionPayForWXLite().notify(xml)
#         if wx_notify['suc']:
#             attach_json = wx_notify.get("attach_json")
#             notify = wx_notify.get("notify")
#             # out_trade_no = notify.get('out_trade_no')
#             if attach_json and attach_json.get('objectId') and attach_json.get("serialType") and attach_json.get(
#                     "ridingCardId"):
#                 object_id = attach_json.get('objectId')
#                 serial_type = attach_json.get("serialType")
#                 riding_card_id = attach_json.get("ridingCardId")
#                 # get_attach_out_trade_no = attach_json.out_trade_no
#                 time_end = notify['time_end']
#                 transaction_id = notify['transaction_id']
#                 total_fee = notify['total_fee']
#             else:
#                 logger.info("回调信息中未找到objectId、serialType、ridingCardId、out_trade_no")
#                 return WX_FAILED_XML
#         else:
#             return WX_FAILED_XML
#
#         set_ridingcard_data = RidingCardService.riding_card_record(serial_type, object_id, transaction_id, total_fee,
#                                                                    RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value, time_end,
#                                                                    riding_card_id)
#         if not set_ridingcard_data:
#             logger.info('骑行卡信息写入失败')
#             return WX_ERROR_XML
#         return WX_SUCCESS_XML
    pass

class RidingCardRefundService(BusinessFullPayInterface):
    # def __init__(self, object_id, trade_no, refund_fee, channel: int):
    #     self.object_id = object_id
    #     self.trade_no = trade_no
    #     self.refund_fee = int(refund_fee)
    #     self.total_fee = 0
    #     self.channel = channel
    #
    # def _check(self):
    #     """
    #     判断交易单号是否存在，退款金额是否大于购买金额，用户是否存在，交易类型是否一致，需要做参数校验
    #     @return:
    #     """
    #     # TODO user_can_refund_ridingcard
    #     refund_account = dao_session.session().query(XcEbikeAccount2) \
    #         .filter(XcEbikeAccount2.trade_no == self.trade_no,
    #                 XcEbikeAccount2.objectId == self.object_id,
    #                 XcEbikeAccount2.type == SERIAL_TYPE.RIDING_CARD_REFUND.value).first()
    #     if refund_account:
    #         return {"suc": False, "info": "已退款"}
    #     account = dao_session.session().query(XcEbikeAccount2) \
    #         .filter(XcEbikeAccount2.trade_no == self.trade_no,
    #                 XcEbikeAccount2.objectId == self.object_id,
    #                 XcEbikeAccount2.type != SERIAL_TYPE.RIDING_CARD_REFUND.value).first()
    #     if account and (account.channel != self.channel or account.amount < self.refund_fee):
    #         return {"suc": False, "info": "退款失败0"}
    #     else:
    #         self.total_fee = account.amount
    #         return {"suc": True, "info": ""}
    #
    # def riding_refund(self, refund_func):
    #     logger.info("riding card is refund, ", refund_func)
    #     r = RidingCardService()
    #     refund_verify = r.user_can_refund_ridingcard(self.object_id, self.trade_no, self.refund_fee)
    #     if not refund_verify.get('suc'):
    #         return refund_verify
    #     verify_data = refund_verify.get('info')
    #     res = refund_func(self.object_id, self.trade_no, self.refund_fee, self.total_fee)
    #     if res["suc"]:
    #         is_refund = r.refund_modify_db(self.object_id, self.trade_no, self.refund_fee,
    #                                        verify_data.get("ridingcard_record"), verify_data.get("account_record"))
    #         if is_refund:
    #             return {"suc": True, "info": "退款成功"}
    #     return {"suc": False, "info": "退款记录失败"}
    pass

    def wx_pay(self):
        return self.riding_refund(WePayService().refund)

    def wx_lite(self):
        return self.riding_refund(WeLiteService().refund)

    def ali_pay(self):
        return self.riding_refund(AliService().refund)

    def ali_lite(self):
        return self.riding_refund(AliLiteService().refund)

    def union_pay_app(self):
        return self.riding_refund(UnionPayForApp().refund)

    def union_pay_code(self):
        return self.riding_refund(UnionPayForCode().refund)

    def union_pay_lite(self):
        return self.riding_refund(UnionPayForWXLite().refund)
