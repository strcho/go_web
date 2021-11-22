from marshmallow import (
    fields,
    Schema,
)

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseDeserializer


class GetFavorableDeserializer(ReqBaseDeserializer):
    """
    用户信息
    """

    pin = fields.String(required=True, description="用户标识")
    service_id = fields.Integer(required=False, missing=None, load_default=None, description="服务区")


class GetFavorableWithServiceIdDeserializer(ReqBaseDeserializer):
    """
    用户信息
    """

    pin = fields.String(required=True, description="用户标识")
    service_id = fields.Integer(required=True, description="服务区")


class ModifyFavorableCardDeserializer(ReqBaseDeserializer):
    """
    修改用户优惠卡信息
    """

    pin = fields.String(required=True, description="用户标识")
    service_id = fields.Integer(required=True, description="服务区")
    duration = fields.Integer(required=True, description="优惠卡时长")


class SendFavorableCardDeserializer(ReqBaseDeserializer):
    """
    添加用户优惠卡
    """

    pin = fields.String(required=True, description="用户标识")
    service_id = fields.Integer(required=True, description="服务区")
    config_id = fields.Integer(required=True, description="计费配置的id")
    card_time = fields.Integer(required=True, description="优惠卡天数")


class UserFavorableCardSerializer(Schema):
    """
    用户优惠卡
    """

    pin = fields.String(description="用户标识")
    begin_time = fields.DateTime(description="开始时间")
    end_time = fields.DateTime(description="结束时间")
    config_id = fields.Integer(description="计费配置的id")
    service_id = fields.Integer(description="服务区的id")
