from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils.mb_handler import ContextDeserializer
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

    id = fields.Integer(required=True, description="免单卡id")
    pin = fields.String(required=True, description='用户标识')
    free_second = fields.Integer(required=True, description='每单的免费时长')
    free_num = fields.Integer(required=True, description='免单次数')


class UpdateUserFreeOrderDeserializer(ReqBaseDeserializer):
    """
    更新用户免单优惠
    """

    pin = fields.Integer(required=True, description="用户标识")
    tp = fields.Integer(required=True, validate=validate.OneOf(choices=list(UserFreeOrderType.to_tuple())), description='更新类型 1:添加 2:使用')
    free_second = fields.Integer(required=True, description='每单的免费时长')
    free_num = fields.Integer(required=True, description='免单次数')


class BusUpdateUserFreeOrderDeserializer(ContextDeserializer):
    """
    更新用户免单优惠
    """

    pin = fields.Integer(required=True, description="用户标识")
    tp = fields.Integer(required=True, validate=validate.OneOf(choices=list(UserFreeOrderType.to_tuple())), description='更新类型 1:添加 2:使用')
    free_second = fields.Integer(required=True, description='每单的免费时长')
    free_num = fields.Integer(required=True, description='免单次数')
