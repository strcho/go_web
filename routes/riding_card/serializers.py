from marshmallow import (
    fields,
    Schema,
)

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseSerializer


class GetRidingCardDeserializer(ReqBaseSerializer):
    """
    获取用户骑行卡
    """

    pin_id = fields.String(required=True, description="用户标识")


class RidingCardInfoSerializer(Schema):
    """
    骑行卡详情
    """
    card_expired_date = fields.String()
    card_id = fields.Integer()
    deduction_type = fields.Integer()
    description_tag = fields.Integer(description='说明标签')
    detail_info = fields.String(description='详情')
    free_distance_meter = fields.Integer(description='免里程, 单位米')
    free_money_cent = fields.Integer(description="免金额, 单位分")
    free_time_second = fields.Integer(description='免时长, 单位秒')
    image_url = fields.String(description='卡图片')
    is_total_times = fields.Integer(description='是否次卡类')
    name = fields.String(description='卡名')
    promotion_tag = fields.String(description='促销标签')
    rece_times = fields.Integer(description='次卡类, 表示总累计次数; 非次卡类, 表示每日最大次数')
    remain_times = fields.Integer(description='骑行卡剩余使用次数')


class RidingCardSerializer(Schema):
    """
    用户骑行卡序列化器
    """

    cost_use = fields.Integer(description="选中的骑行卡")
    used = fields.Nested(RidingCardInfoSerializer, many=True)
    expired = fields.Nested(RidingCardInfoSerializer, many=True)
    rule_info = fields.String()  # todo


class UpdateWalletDeserializer(ReqBaseSerializer):
    """
    更新用户钱包信息
    """

    pin_id = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=DefaultMaker, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=DefaultMaker, description="变动的赠送金额*100")
    change_deposited_mount = fields.Integer(required=False, load_default=DefaultMaker, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=DefaultMaker, description="押金状态")
