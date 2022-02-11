from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import (
    ReqBaseDeserializer,
    BaseSchema,
)
from utils.constant.user import (
    DiscountsUserType,
)


class GetUserDiscountDeserializer(ReqBaseDeserializer):
    """
    获取用户折扣信息
    """

    pin = fields.String(required=True, description='用户标识')


class ClientGetUserDiscountDeserializer(ContextDeserializer):
    """
    获取用户折扣信息
    """

    pin = fields.String(required=True, description='用户标识')


class UserDiscountSerializer(BaseSchema):
    """
    用户折扣信息序列化
    """

    id = fields.Integer(required=True, description="折扣卡id")
    pin = fields.String(required=True, description='用户标识')
    discount_rate = fields.Integer(required=True, description='折扣信息 10 表示 1折')


class UpdateUserDiscountDeserializer(ReqBaseDeserializer):
    """
    更新用户折扣优惠
    """

    pin = fields.String(required=True, description='用户标识')
    tp = fields.Integer(required=True, validate=validate.OneOf(choices=list(DiscountsUserType.to_tuple())), description='更新类型 1:添加 2:使用')
    discount_rate = fields.Integer(required=True, description='折扣信息 10 表示 1折')


class BusUpdateUserDiscountDeserializer(ContextDeserializer):
    """
    更新用户折扣优惠
    """

    pin = fields.String(required=True, description='用户标识')
    tp = fields.Integer(required=True, validate=validate.OneOf(choices=list(DiscountsUserType.to_tuple())), description='更新类型 1:添加 2:使用')
    discount_rate = fields.Integer(required=True, description='折扣信息 10 表示 1折')
