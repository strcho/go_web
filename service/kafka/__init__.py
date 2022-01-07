from mbutils.constant import MbEnum
from mbutils import AGENT_NAME
# AGENT_NAME = "qiyiqi"


class KafkaRetry(Exception):
    """ kafka重传异常类 """
    pass


class PayKey(MbEnum):
    """ 支付相关的key集合 """
    DEPOSIT = "deposit"
    DEPOSIT_CARD = "deposit_card"
    FAVORABLE_CARD = "favorable_card"
    RIDING_CARD = "riding_card"
    WALLET = "wallet"
    ORDER = "order"


class ChannelType(MbEnum):
    PLATFORM = 0  # 平台 (活动相关，平台修改的都属于平台)

    ALIPAY = 1  # 支付宝APP支付
    ALIPAY_LITE = 2  # 支付宝小程序支付

    WEPAY = 11  # 微信APP支付
    WXLITE = 12  # 微信小程序支付
    WEPAY_H5 = 13  # 微信H5支付

    UNIONPAY_WXLITE = 21  # 云闪付=>微信小程序支付
    UNIONPAY_CODE = 22  # 云闪付-二维码支付
    UNIONPAY_APP = 23  # 云闪付-APP支付


class TransactionType(MbEnum):
    BOUGHT = 1  # 充值（购买）
    REFUND = 2  # 退款
    PLATFORM_BOUGHT = 3  # 平台充值
    PLATFORM_REFUND = 4  # 平台退款

    ITINERARY = 101  # 行程支付
    ITINERARY_REFUND = 102  # 行程退款
    REPORT_PENALTY = 103  # 举报罚金
    IMPUNITY_PENALTY = 104  # 还车申请免罚罚金

    REGULAR_ACTIVITY = 201  # 固定活动赠送
    RECHARGE_ACTIVITY = 202  # 充值赠送
    TARGET_USER = 203  # 指定用户奖励
    CUSTOM_ACTIVITY = 204  # 营销活动自定义活动赠送
    VOUCHER_USER = 205  # 兑换券
    INVITE_ACTIVITY = 206  # 邀请有礼


class KafkaTopic(MbEnum):
    """ python端收发的topic """
    PAYMENT = f"{AGENT_NAME}_PAY"


# 每个端的消费者组
CONSUMER_GROUP_ID = f"{AGENT_NAME}_python"
