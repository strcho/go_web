from marshmallow import (
    fields,
    Schema,
    validate,
)

from mbutils import (
    DefaultMaker,
    ARG_DEFAULT,
)
from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import ReqBaseDeserializer


class GetWalletDeserializer(ReqBaseDeserializer):
    """
    获取用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")


class BusGetWalletDeserializer(ContextDeserializer):
    """
    获取用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")


class GetWalletListDeserializer(ReqBaseDeserializer):
    """获取用户钱包信息"""

    pin_list = fields.List(fields.String(required=True), required="pin 列表")


class UserWalletSerializer(Schema):
    """
    用户钱包序列化器
    """

    pin = fields.String(description="用户标识")
    balance = fields.String(description="余额")
    recharge = fields.String(description="充值余额")
    present = fields.String(description="增送余额")
    deposited_mount = fields.String(description="押金金额")
    deposited_stats = fields.String(description="押金状态")


class UpdateWalletDeserializer(ReqBaseDeserializer):
    """
    更新用户钱包信息
    """

    pin = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的赠送金额*100")
    change_deposited_mount = fields.Integer(required=False, load_default=ARG_DEFAULT, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=ARG_DEFAULT, description="押金状态")
