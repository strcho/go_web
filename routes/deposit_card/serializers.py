from marshmallow import (
    fields,
    Schema,
)

from mbutils import (
    ARG_DEFAULT,
)
from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import (
    ReqBaseDeserializer,
    BaseSchema,
)
from utils.ebike_fields import EDateTime


class GetDepositCardDeserializer(ReqBaseDeserializer):
    """
    用户信息
    """

    pin = fields.String(required=True, description="用户标识")
    # service_id = fields.Integer(required=True, description="服务区")


class ModifyDepositCardDeserializer(ReqBaseDeserializer):
    """
    修改用户押金卡信息
    """

    pin = fields.String(required=True, description="用户标识")
    # service_id = fields.Integer(required=True, description="服务区")
    duration = fields.Integer(required=True, description="押金卡时长")


class SendDepositCardDeserializer(ReqBaseDeserializer):
    """
    添加用户押金卡
    """

    pin = fields.String(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")

    config_id = fields.Integer(required=True, description='押金卡ID')
    service_id = fields.Integer(required=True, description="服务区")
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
    channel = fields.Integer(load_default=None, description="""支付渠道
    0  # 平台 (活动相关，平台修改的都属于平台)
    1  # 支付宝APP支付
    2  # 支付宝小程序支付
    11  # 微信APP支付
    12  # 微信小程序支付
    13  # 微信H5支付
    14  # 微信，信用分 
    21  # 云闪付=>微信小程序支付
    22  # 云闪付-二维码支付
    23  # 云闪付-APP支付
    101 # 波兰支付（欧洲）
    """)
    sys_trade_no = fields.String(load_default=ARG_DEFAULT, description="系统订单号")
    merchant_trade_no = fields.String(load_default=ARG_DEFAULT, description="外部支付渠道订单号")
    paid_at = fields.String(load_default=None, description="交易时间戳")


class UserDepositCardSerializer(BaseSchema):
    """
    用户押金卡
    """

    id = fields.Integer(required=True, description="押金卡id")
    pin = fields.String(required=False, load_default=ARG_DEFAULT, description="用户标识")
    expired_date = EDateTime(description="过期时间")


class UserDepositCardDaysSerializer(BaseSchema):
    """
    用户押金卡可用天数
    """

    days = fields.Integer(required=True, description='可用天数')
    expired_date_str = fields.String(required=True, description='到期时间')


class BusModifyDepositCardDeserializer(ContextDeserializer):
    """
    修改用户押金卡信息
    """

    pin = fields.String(required=True, description="用户标识")
    # service_id = fields.Integer(required=True, description="服务区")
    duration = fields.Integer(required=True, description="押金卡时长")


class DepositCardToKafkaSerializer(ContextDeserializer):

    pass
