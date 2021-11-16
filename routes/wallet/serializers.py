from marshmallow import (
    fields,
    Schema,
)

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseSerializer


class GetWalletDeserializer(ReqBaseSerializer):
    """
    获取用户钱包信息
    """

    pin_id = fields.String(required=True, description="用户标识")


class UserWalletSerializer(Schema):

    pin_id = fields.String(description="用户id")
    balance = fields.String(description="余额")
    recharge = fields.String(description="充值余额")
    present = fields.String(description="增送余额")
    deposited_mount = fields.String(description="押金金额")
    deposited_stats = fields.String(description="押金状态")


class UpdateWalletDeserializer(ReqBaseSerializer):
    """
    更新用户钱包信息
    """

    pin_id = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=DefaultMaker, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=DefaultMaker, description="变动的赠送金额*100")
    change_deposited_mount = fields.Integer(required=False, load_default=DefaultMaker, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=DefaultMaker, description="押金状态")
