from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils import (
    DefaultMaker,
    ARG_DEFAULT,
)
from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import (
    ReqBaseDeserializer,
    BaseSchema,
)


class GetDepositDeserializer(ReqBaseDeserializer):
    """
    获取用户押金信息
    """

    pin = fields.String(required=True, description="用户标识")


class UserDepositSerializer(BaseSchema):
    """
    用户押金序列化器
    """

    pin = fields.String(description="用户标识")
    deposited_mount = fields.String(description="押金金额")
    deposited_stats = fields.String(description="押金状态")


class UpdateDepositDeserializer(ReqBaseDeserializer):
    """
    更新用户押金信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态")

    type = fields.Integer(load_default=None, description="""支付类型 
    1  # 充值（购买）
    2  # 退款 
    3  # 平台充值
    4  # 平台退款
    101  # 行程支付
    102  # 行程退款
    103  # 举报罚金
    104  # 还车申请免罚罚金
    201  # 固定活动赠送
    202  # 充值赠送
    203  # 指定用户奖励
    204  # 营销活动自定义活动赠送
    205  # 兑换券
    206  # 邀请有礼""")
    channel = fields.String(load_default=None, description="""支付渠道
    0  # 平台 (活动相关，平台修改的都属于平台)
    1  # 支付宝APP支付
    2  # 支付宝小程序支付
    11  # 微信APP支付
    12  # 微信小程序支付
    13  # 微信H5支付
    21  # 云闪付=>微信小程序支付
    22  # 云闪付-二维码支付
    23  # 云闪付-APP支付
    """)
    sys_trade_no = fields.String(load_default=ARG_DEFAULT, description="系统订单号")
    merchant_trade_no = fields.String(load_default=ARG_DEFAULT, description="外部支付渠道订单号")



class BusUpdateDepositDeserializer(ContextDeserializer):
    """
    更新用户押金信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态")


class DepositToKafkaSerializer(ContextDeserializer):

    pass