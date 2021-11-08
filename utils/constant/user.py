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
    NO_DEPOSITED = 0  # 未缴纳押金
    DEPOSITED = 1  # 已缴纳押金
    REFUNDING = 2  # 押金退款流程中

# const XC_EBIKE_DEVICE_STATE = {
#   READY: '1', // 可使用
#   RIDING: '2', // 骑行中
#   BROKEN: '3', // 设备报修
#   BOOKING: '4', // 设备被预约
#   OFFLINE: '5', // 设备离线
#   CHANGING_BATTERY: '6', // 处于工人换电池阶段  【虚拟状态】
#   DRAG_BACK: '7', // 设备无法现场维修，置为拖回状态
#   LOW_BATTERY: '8', // 新增低电量类型车辆，和拖回状态分开
#   MOVE_CAR: '8', // 挪车 【虚拟状态】
#   // 空。设备login但未上架
# };
