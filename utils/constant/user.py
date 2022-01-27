from . import MbEnum
from utils.constant.redis_key import *


# 用户状态
from mbutils import logger


class UserState(MbEnum):
    SIGN_UP = 1  # 未实名认证
    AUTHED = 2  # 已实名认证
    READY = 3  # 已缴纳诚信金或者会员卡等有用车资格
    BOOKING = 4  # 预定
    LEAVING = 5  # 停停
    RIDING = 6  # 骑行中
    TO_PAY = 7  # 有未支付订单

    @staticmethod
    def no_riding_users():
        """
        无骑行资格用户
        :return:
        """
        return [1, 2]

    @staticmethod
    def have_riding_users():
        """
        有骑行资格用户
        :return:
        """
        return [3, 4, 5, 6, 7]

    @staticmethod
    def set_state(r, u_id, dst_status, origin_status=None):
        current_state = r.get(USER_STATE_KEY.format(u_id))
        if origin_status and int(current_state) != origin_status:
            return
        # 用户分类从一个到另一个了
        r.srem(USER_STATE_COUNT.format(state=current_state), u_id)
        r.set(USER_STATE_KEY.format(u_id), dst_status)
        logger.info(f"user_id: {u_id}, user_state change: {origin_status} --> {dst_status}")
        r.sadd(USER_STATE_COUNT.format(state=dst_status), u_id)


class DepositedState(MbEnum):
    """押金状态"""
    NO_DEPOSITED = 0  # 未缴纳押金
    DEPOSITED = 1  # 押金已缴纳
    REFUNDED = 2  # 押金已退款
    FROZEN = 3  # 押金已冻结


class UserFreeOrderType(MbEnum):
    """
    用户免单
    """

    ADD_FREE_ORDER = 1  # 添加用户免单
    use_FREE_ORDER = 2  # 使用用户免单


class DiscountsUserType(MbEnum):
    """
    用户折扣
    """

    ADD_DISCOUNT = 1  # 添加用户折扣
    USE_DISCOUNT = 2  # 使用用户折扣
