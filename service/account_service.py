"""
总流水相关方法
"""
import json
from model.all_model import *
from mbutils import dao_session, MbException
from utils.constant.account import SERIAL_TYPE, RIDING_CHANNEL_TYPE
from utils.constant.redis_key import USER_STATE_KEY
from utils.constant.user import UserState, DepositedState


class AccountService:
    #
    # @staticmethod
    # def get_channel(trade_no, object_id):
    #     one = dao_session.session().query(XcEbikeAccount2.channel) \
    #         .filter_by(trade_no=trade_no, objectId=object_id).first()
    #     if one:
    #         return one.channel
    #     else:
    #         raise MbException("获取不到用户的支付渠道")
    #
    # @staticmethod
    # def get_deposit_refund_info(object_id):
    #     one = dao_session.session().query(XcEbikeUsrs2.deposited, XcEbikeUsrs2.depositedInfo).filter_by(
    #         id=object_id).first()
    #     user_state = int(dao_session.redis_session.r.get(USER_STATE_KEY.format(object_id)))
    #     if not one:
    #         raise MbException("用户不存在")
    #     if one.deposited == DepositedState.NO_DEPOSITED.value:
    #         raise MbException("您未支付诚信金")
    #     elif one.deposited == DepositedState.REFUNDING.value:
    #         raise MbException("您已在退款进程中")
    #     elif user_state == UserState.SIGN_UP.value:
    #         raise MbException("请先实名认证")
    #     elif user_state == UserState.BOOKING.value:
    #         raise MbException('您已经预约了一辆车，当前状态您不能申请退还诚信金')
    #     elif user_state in [UserState.LEAVING.value, UserState.RIDING.value, UserState.TO_PAY.value]:
    #         raise MbException("您有未结束的行程，当前状态您不能申请退还诚信金")
    #     elif user_state != UserState.READY.value:
    #         raise MbException("当前状态您不能申请退还诚信金")
    #     try:
    #         deposited_info = json.loads(one.depositedInfo)
    #         """
    #         {
    #             "channel": RIDING_CHANNEL_TYPE.UNIONPAY_CODE.value,
    #             "out_trade_no": order_id,
    #             "transaction_id": '',
    #             "total_fee": total_fee,
    #         }
    #         """
    #     except Exception as ex:
    #         raise MbException("订单记录异常，暂时无法退还押金。请联系客服处理")
    #     trade_no, refund_fee, channel = deposited_info["transaction_id"], deposited_info["total_fee"], deposited_info[
    #         "channel"]
    #     return trade_no, refund_fee, channel
    pass