# import random
# import string
# import time
# from datetime import datetime
#
# from sqlalchemy import and_
#
# from model.all_model import *
# from mbutils import dao_session
#
#
# class PayDBService():
#     def __init__(self, objectId=None):
#         self.objectId = objectId
#
#     def get_user(self):
#         user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == self.objectId).first()
#         return user
#
#     def is_user_get_riding_card(self):
#         try:
#             user_riding_card = dao_session.session().query(XcEbike2RidingCard).filter(
#                 XcEbike2RidingCard.objectId == self.objectId).one()
#             if int(time.mktime(user_riding_card.cardExpiredDate.timetuple())) - int(
#                     time.mktime(datetime.now().timetuple())) <= 0:
#                 return False
#         except Exception as e:
#             return False
#         return True
#
#     def get_one_riding_card(self):
#         user_riding_card = dao_session.session().query(XcEbike2RidingCard).filter(
#             XcEbike2RidingCard.objectId == self.objectId).first()
#         return user_riding_card
#
#     # 创建订单号
#     def create_trade_number(self):
#         return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=24))
#
#     # 创建退款单号
#     def create_refund_number(self):
#         return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=40))
#
#     def create_noncestr_number(self):
#         # hashlib.md5(''.join(random.sample(string.ascii_letters + string.digits, 30)).encode('utf-8')).hexdigest().upper()
#         return ''.join(random.sample(string.ascii_letters + string.digits, 32))
#
#     # 判断骑行卡是否存在
#     def get_riding_config(self, serviceId, state, type):
#         riding_config = dao_session.session().query(XcEbike2RidingConfig).filter(
#             and_(XcEbike2RidingConfig.serviceId == serviceId),
#             (XcEbike2RidingConfig.state == state),
#             (XcEbike2RidingConfig.type == type)).all()
#         return riding_config
#
#     def is_account_by_trade(self, transaction_id) -> bool:
#         return bool(
#             dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == transaction_id).one())
#
#     # 优惠卡信息
#     def get_favorablecard_by_id(self, favorableCardId):
#         favorable_card = dao_session.session().query(XcMieba2FavorableCard.id == favorableCardId).first()
#         return favorable_card
#
#     def get_favorable_user(self, serviceId, objectId):
#         favorable_user = dao_session.session().query(and_(XcMieba2FavorableCardUser.service_id == serviceId),
#                                                      (XcMieba2FavorableCardUser.object_id == objectId)).first()
#         return favorable_user
#
#     def get_deposit_card_config(self, depositcard_id):
#         deposit_config = dao_session.session().query(XcEbike2DepositConfig).filter(
#             XcEbike2DepositConfig.id == depositcard_id,
#             XcEbike2DepositConfig.type == 0,
#             XcEbike2DepositConfig.state == 1).first()
#         return deposit_config
#
#     def is_deposit_card(self, trade_no) -> bool:
#         return bool(
#             dao_session.session().query(XcEbike2DepositCard).filter(XcEbike2DepositCard.trade_no == trade_no).one())
#
#     def is_user_order(self, objectId) -> bool:
#         return bool(dao_session.session().query(XcEbikeUserOrder).filter(XcEbikeUserOrder.userId == objectId,
#                                                                          XcEbikeUserOrder.isPaid == 0).one())
#
#
# class PayNotifyNxLock():
#     """
#         设置支付回调锁
#         @param {string} payType 支付类型参数
#         @param {string} tradeNo 流水号
#     """
#
#
# class UserDBService:
#
#     #
#     @staticmethod
#     def get_user_state(agentId, objectId):
#         user_state = dao_session.redis_session.r.get(
#             "xc_ebike_{agentId}_userState_{objectId}".format(agentId=agentId, objectId=objectId))
#         return user_state
#
