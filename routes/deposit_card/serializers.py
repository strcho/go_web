from marshmallow import (
    fields,
    Schema,
)

from mbutils import (
    DefaultMaker,
    ARG_DEFAULT,
)
from utils.base_serializer import ReqBaseDeserializer


class GetDepositDeserializer(ReqBaseDeserializer):
    """
    用户信息
    """

    pin = fields.String(required=True, description="用户标识")


class ModifyDepositCardDeserializer(ReqBaseDeserializer):
    """
    修改用户优惠卡信息
    """

    pin = fields.String(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")


class SendDepositCardDeserializer(ReqBaseDeserializer):
    """
    添加用户优惠卡
    """

    pin = fields.String(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")


class UserDepositCardSerializer(Schema):
    """
    用户优惠卡
    """

    pin = fields.Integer(required=False, load_default=ARG_DEFAULT, description="用户标识")
    expired_date = fields.DateTime(description="过期时间")
