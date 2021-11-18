from marshmallow import (
    fields,
    Schema,
)

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseDeserializer


class GetRidingCardDeserializer(ReqBaseDeserializer):
    """
    获取用户骑行卡
    """

    pin_id = fields.Integer(required=True, description="用户标识")


class EditRidingCardDeserializer(ReqBaseDeserializer):
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

    pin_id = fields.Integer(required=True, description="用户标识")
    config_id = fields.Integer(required=True, description="骑行卡配置ID（card_id）")
    content = fields.String(required=True, description="购卡时候配置信息")


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
    iz_total_times = fields.Integer(description='是否次卡类')
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


class CurrentDuringTimeDeserializer(ReqBaseDeserializer):
    """查询当前骑行卡的持续时间参数反序列化"""

    pin_id = fields.Integer(required=True, description='用户ID')
    service_id = fields.Integer(required=True, description='服务区ID')


class CurrentDuringTimeSerializer(Schema):
    """
    当前骑行卡的持续时间响应序列化
    """
    pass


class AddCountHandlerDeserializer(ReqBaseDeserializer):
    """
    骑行卡使用次数加一
    """

    card_id = fields.Integer(required=True, description='骑行卡ID')

