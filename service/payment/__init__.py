import random
import string
import time

from sqlalchemy import and_

from model.all_model import *
from mbutils import dao_session, logger
from mbutils.constant import MbEnum
from utils.constant.redis_key import USER_STATE_COUNT, USER_STATE, PAY_TYPE_TRADE_NO


class BusinessType(MbEnum):
    """ 支付相关的key集合 """
    DEPOSIT = "deposit"
    DEPOSIT_CARD = "deposit_card"
    FAVORABLE_CARD = "favorable_card"
    RIDING_CARD = "riding_card"
    WALLET = "wallet"


class PayHelper:

    def __init__(self):
        pass

    @staticmethod
    def rand_str_24() -> str:
        """
        @return: 24位随机数
        """
        return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=24))

    @staticmethod
    def rand_str_40() -> str:
        """
        @return: 48位随机数
        """
        return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=40))

    @staticmethod
    def rand_str_32() -> str:
        """
        @return: 32位随机数
        """
        # return ''.join(random.sample(string.ascii_letters + string.digits, 32))
        result = hex(random.randint(0, 16 ** 32)).replace('0x', '').upper()
        if (len(result) < 32):
            result = '0' * (32 - len(result)) + result
        return result


class PayDBService():
    def __init__(self, object_id=None):
        self.object_id = object_id

    def get_user(self):
        user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == self.object_id).first()
        return user

    def get_one_riding_card(self):
        user_riding_card = dao_session.session().query(XcEbike2RidingCard).filter(
            XcEbike2RidingCard.objectId == self.object_id).first()
        return user_riding_card

    # 创建订单号
    def create_trade_number(self):
        return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=24))

    # 创建退款单号
    def create_refund_number(self):
        return datetime.now().strftime('%Y%m%d') + ''.join(random.choices(string.digits, k=40))

    def create_noncestr_number(self):
        # hashlib.md5(''.join(random.sample(string.ascii_letters + string.digits, 30)).encode('utf-8')).hexdigest().upper()
        # return ''.join(random.sample(string.ascii_letters + string.digits, 32))
        result = hex(random.randint(0, 16 ** 32)).replace('0x', '').upper()
        if (len(result) < 32):
            result = '0' * (32 - len(result)) + result
        return result

    # 判断骑行卡是否存在
    def get_riding_config(self, service_id, state, type):
        riding_config = dao_session.session().query(XcEbike2RidingConfig).filter(
            and_(XcEbike2RidingConfig.serviceId == service_id),
            (XcEbike2RidingConfig.state == state),
            (XcEbike2RidingConfig.type == type)).all()
        return riding_config

    def is_account_by_trade(self, transaction_id) -> bool:
        return bool(
            dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == transaction_id).first())

    def get_deposit_card_config(self, depositcard_id):
        deposit_config = dao_session.session().query(XcEbike2DepositConfig).filter(
            XcEbike2DepositConfig.id == depositcard_id,
            XcEbike2DepositConfig.type == 0,
            XcEbike2DepositConfig.state == 1).first()
        return deposit_config

    def is_deposit_card(self, trade_no) -> bool:
        return bool(
            dao_session.session().query(XcEbike2DepositCard).filter(XcEbike2DepositCard.trade_no == trade_no).first())

    def exists_unpaid_order(self, object_id) -> bool:
        return bool(dao_session.session().query(XcEbikeUserOrder).filter(XcEbikeUserOrder.userId == object_id,
                                                                         XcEbikeUserOrder.isPaid == 0).first())

    def set_deposited_info(self, object_id, deposited, deposited_info, deposited_mount=None, refund_info=None):
        user = self.get_user()
        deposited = deposited if deposited != None else user.deposited
        deposited_info = deposited_info or user.depositedInfo
        deposited_mount = deposited_mount if deposited_mount != None else user.depositedMount
        refund_info = refund_info or user.refundInfo
        # 默认成功
        dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).update(
            {"deposited": deposited, "deposited_info": deposited_info, "deposited_mount": deposited_mount,
             "refund_info": refund_info})
        dao_session.session().commit()


class PayNotifyNxLock():
    """
        设置支付回调锁
        @param {string} payType 支付类型参数
        @param {string} tradeNo 流水号
    """

    def set_pay_notify_lock(self, pay_type, trad_no, value, time=12):
        return dao_session.redis_session.r.set(PAY_TYPE_TRADE_NO.format(payType=pay_type, tradNo=trad_no),
                                               value, ex=time, nx=True)

    """获取支付回调锁"""

    def get_pay_notify_lock(self, pay_type, trad_no):
        return dao_session.redis_session.r.get(PAY_TYPE_TRADE_NO.format(payType=pay_type, tradNo=trad_no))

    """删除支付回调锁"""

    def del_pay_notify_lock(self, pay_type, trad_no):
        return dao_session.redis_session.r.delete(PAY_TYPE_TRADE_NO.format(payType=pay_type, tradNo=trad_no))

    @staticmethod
    def check_trade_no(pay_type, trad_no):
        nx_lock = PayNotifyNxLock().set_pay_notify_lock(pay_type, trad_no, trad_no)
        if not nx_lock:
            return {"suc": False, "info": "重复回调"}
        return {"suc": True, "info": "重复回调"}


class UserDBService:

    #
    @staticmethod
    def get_user_state(object_id):
        user_state = dao_session.redis_session.r.get(
            USER_STATE.format(user_id=object_id))
        return user_state

    # 设置用户状态
    @staticmethod
    def set_user_state(object_id, origin_state, cur_state):
        # 和db操作一起提交
        dao_session.redis_session.r.srem(USER_STATE_COUNT.format(state=origin_state),  # agentId 默认给2
                                         object_id)
        dao_session.redis_session.r.set(USER_STATE.format(user_id=object_id), cur_state)
        dao_session.redis_session.r.sadd(USER_STATE_COUNT.format(state=cur_state),  # agentId 默认给2
                                         object_id)
        logger.info(f"user: {object_id}, user_state_change: {origin_state} --> {cur_state}")

