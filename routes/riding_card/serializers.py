from marshmallow import (
    fields,
    Schema,
)

from mbutils import ARG_DEFAULT
from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import (
    ReqBaseDeserializer,
    BaseSchema,
)


class GetRidingCardDeserializer(ReqBaseDeserializer):
    """
    获取用户骑行卡
    """

    pin = fields.String(required=True, description="用户标识")


class ClientGetRidingCardDeserializer(ContextDeserializer):
    """
    C端 获取用户骑行卡
    """

    pin = fields.String(required=True, description="用户标识")


class EditRidingCardDeserializer(ReqBaseDeserializer):
    """
    编辑用户骑行卡
    """

    card_id = fields.Integer(required=True, description="骑行卡id")
    duration = fields.Integer(required=False, description='剩余天数')
    remain_times = fields.Integer(required=False, description='今日剩余免费次数')


class BusGetRidingCardDeserializer(ContextDeserializer):
    """
    获取用户骑行卡
    """

    pin = fields.String(required=True, description="用户标识")


class BusEditRidingCardDeserializer(ContextDeserializer):
    """
    编辑用户骑行卡
    """

    card_id = fields.Integer(required=True, description="骑行卡id")
    duration = fields.Integer(required=False, description='剩余天数')
    remain_times = fields.Integer(required=False, description='今日剩余免费次数')


class SendRidingCardDeserializer(ReqBaseDeserializer):
    """
    发放骑行卡给用户
    """

    pin = fields.String(required=True, description="用户标识")
    config_id = fields.Integer(required=True, description="骑行卡配置ID（card_id）")

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


class ClientRidingCardInfoSerializer(BaseSchema):
    """
    骑行卡详情
    """

    id = fields.String(description='用户骑行卡id')
    card_expired_date = fields.String(description='骑行卡到期时间')
    card_id = fields.String(description='母卡信息')
    deduction_type = fields.Integer(description='抵扣类型, 1时长卡, 2里程卡, 3减免卡, 4次卡')
    description_tag = fields.String(description='说明标签')
    detail_info = fields.String(description='详情')
    free_distance_meter = fields.Integer(description='免里程, 单位米')
    free_money_cent = fields.Integer(description="免金额, 单位分")
    free_time_second = fields.Integer(description='免时长, 单位秒')
    image_url = fields.String(description='卡图片')
    iz_total_times = fields.Integer(description='是否次卡类')
    name = fields.String(description='卡名')
    promotion_tag = fields.String(description='促销标签')
    rece_times = fields.Integer(description='次卡类, 表示总累计次数; 非次卡类, 表示每日最大次数')
    remain_times = fields.Integer(description='骑行卡剩余使用次数')
    effective_service_ids = fields.String(description="可用服务区")
    effective_service_names = fields.String(description="可用服务区名称")


class RidingCardInfoSerializer(BaseSchema):
    """
    骑行卡详情
    """

    id = fields.Integer(description='用户骑行卡id')
    card_expired_date = fields.String(description='骑行卡到期时间')
    card_id = fields.Integer(description='母卡信息')
    deduction_type = fields.Integer(description='抵扣类型, 1时长卡, 2里程卡, 3减免卡, 4次卡')
    description_tag = fields.String(description='说明标签')
    detail_info = fields.String(description='详情')
    free_distance_meter = fields.Integer(description='免里程, 单位米')
    free_money_cent = fields.Integer(description="免金额, 单位分")
    free_time_second = fields.Integer(description='免时长, 单位秒')
    image_url = fields.String(description='卡图片')
    iz_total_times = fields.Integer(description='是否次卡类')
    name = fields.String(description='卡名')
    promotion_tag = fields.String(description='促销标签')
    rece_times = fields.Integer(description='次卡类, 表示总累计次数; 非次卡类, 表示每日最大次数')
    remain_times = fields.Integer(description='骑行卡剩余使用次数')
    effective_service_ids = fields.String(description="可用服务区")
    effective_service_names = fields.String(description="可用服务区名称")


class RidingCardSerializer(BaseSchema):
    """
    用户骑行卡序列化器
    """

    cost_use = fields.Integer(description="选中的骑行卡")
    used = fields.Nested(RidingCardInfoSerializer, many=True)
    expired = fields.Nested(RidingCardInfoSerializer, many=True)
    rule_info = fields.String()  # todo


class ClientRidingCardSerializer(BaseSchema):
    """
    用户骑行卡序列化器
    """

    cost_use = fields.String(description="选中的骑行卡")
    used = fields.Nested(ClientRidingCardInfoSerializer, many=True)
    expired = fields.Nested(ClientRidingCardInfoSerializer, many=True)
    rule_info = fields.String()  # todo


class CurrentDuringTimeDeserializer(ReqBaseDeserializer):
    """查询当前骑行卡的持续时间参数反序列化"""

    pin = fields.String(required=True, description='用户标识')
    service_id = fields.Integer(required=True, description='服务区ID')


class CurrentDuringTimeSerializer(BaseSchema):
    """
    当前骑行卡的持续时间响应序列化
    """

    free_time = fields.Integer(description="单位秒", dump_default=0)
    free_distance = fields.Integer(description="单位米", dump_default=0)
    free_money = fields.Integer(description="单位分", dump_default=0)


class AddCountHandlerDeserializer(ReqBaseDeserializer):
    """
    骑行卡使用次数加一
    """

    card_id = fields.Integer(required=True, description='骑行卡ID')


class RefundRidingCardDeserializer(ReqBaseDeserializer):

    pin = fields.String(required=True, description="用户标识")
    config_id = fields.Integer(required=True, description='优惠卡卡ID')
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


class RidingCardToKafkaSerializer(ReqBaseDeserializer):

    pass