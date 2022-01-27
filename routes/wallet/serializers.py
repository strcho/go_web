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


class GetWalletDeserializer(ReqBaseDeserializer):
    """
    获取用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")


class BusGetWalletDeserializer(ContextDeserializer):
    """
    获取用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")


class CliGetWalletDeserializer(ContextDeserializer):
    """
    获取用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")


class GetWalletListDeserializer(ReqBaseDeserializer):
    """获取用户钱包信息"""

    pin_list = fields.List(fields.String(required=True), required="pin 列表")


class UserWalletSerializer(BaseSchema):
    """
    用户钱包序列化器
    """

    pin = fields.String(description="用户标识")
    balance = fields.Integer(description="余额")
    recharge = fields.Integer(description="充值余额")
    present = fields.Integer(description="增送余额")
    deposited_mount = fields.Integer(description="押金金额")
    deposited_stats = fields.Integer(description="押金状态  0 未缴纳 1已缴纳  2  已退还  3 已冻结")


class CliUserWalletSerializer(BaseSchema):
    """
    C端用户钱包序列化器
    """

    pin = fields.String(description="用户标识")
    balance = fields.Integer(description="余额")
    recharge = fields.Integer(description="充值余额")
    present = fields.Integer(description="增送余额")
    deposited_mount = fields.Integer(description="押金金额")
    deposited_stats = fields.Integer(description="押金状态")
    can_refund_amount = fields.Integer(description="可退金额")


class UpdateWalletDeserializer(ReqBaseDeserializer):
    """
    更新用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的赠送金额*100")
    # change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    # deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态 0 未缴纳 1缴纳  2 退还  3 冻结")

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


class BusUpdateWalletDeserializer(ContextDeserializer):
    """
    B端更新用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的赠送金额*100")
    change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态")

    type = fields.Integer(load_default=None, description="""支付类型 
    1  # 充值（购买）
    2  # 退款 
    3  # 平台充值 4  # 平台退款 101  # 行程支付 102  # 行程退款
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


class DeductionBalanceDeserializer(ReqBaseDeserializer):
    """
    扣减用户余额
    """

    pin = fields.String(required=True, description="用户标识")
    deduction_amount = fields.Integer(required=True, description="扣减的余额*100")

    type = fields.Integer(load_default=None, description="""支付类型 
    1  # 充值（购买）
    2  # 退款 
    3  # 平台充值 4  # 平台退款 101  # 行程支付 102  # 行程退款
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


class WalletToKafkaSerializer(ReqBaseDeserializer):
    created_pin = fields.String(required=True, description="创建用户标识")
    pin_id = fields.String(required=True, description="用户标识")
    type = fields.Integer(required=True, description="支付类型")
    channel = fields.String(required=True, description="支付渠道")
    sys_trade_no = fields.String(required=True, description="系统订单号")
    merchant_trade_no = fields.String(required=True, description="外部支付渠道订单号")
    recharge_amount = fields.Integer(required=True, description="充值金额")
    present_amount = fields.Integer(required=True, description="充值赠送金额")
