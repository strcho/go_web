from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseDeserializer
from utils.constant.user import UserFreeOrderType


class GetUserFreeOrderDeserializer(ReqBaseDeserializer):
    """
    获取用户免单信息
    """

    pin = fields.String(required=True, description='用户标识')


class UserFreeOrderSerializer(Schema):
    """
    用户免单信息序列化
    """

    pin = fields.String(required=True, description='用户标识')
    free_second = fields.Integer(required=True, description='每单的免费时长')
    free_num = fields.Integer(required=True, description='免单次数')


class UpdateUserFreeOrderDeserializer(ReqBaseDeserializer):
    """
    更新用户免单优惠
    """

    pin = fields.String(required=True, description='用户标识')
    tp = fields.Integer(required=True, validate=validate.OneOf(choices=UserFreeOrderType.to_tuple()), description='更新类型')
    free_second = fields.Integer(required=True, description='每单的免费时长')
    free_num = fields.Integer(required=True, description='免单次数')
