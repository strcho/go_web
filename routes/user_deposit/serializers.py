from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils import (
    DefaultMaker,
    ARG_DEFAULT,
)
from utils.base_serializer import ReqBaseDeserializer


class GetDepositDeserializer(ReqBaseDeserializer):
    """
    获取用户押金信息
    """

    pin = fields.String(required=True, description="用户标识")


class UserDepositSerializer(Schema):
    """
    用户押金序列化器
    """

    pin = fields.String(description="用户标识")
    deposited_mount = fields.String(description="押金金额")
    deposited_stats = fields.String(description="押金状态")


class UpdateDepositDeserializer(ReqBaseDeserializer):
    """
    更新用户押金信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态")
