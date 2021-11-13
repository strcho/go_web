from marshmallow import fields

from mbutils import DefaultMaker
from utils.base_serializer import ReqBaseSerializer


class GetWalletDeserializer(ReqBaseSerializer):
    """
    获取用户钱包信息
    """

    pin_id = fields.String(required=True, description="用户标识")


class UpdateWalletDeserializer(ReqBaseSerializer):
    """
    更新用户钱包信息
    """

    pin_id = fields.String(required=True, description="用户标识")
    change_recharge = fields.Integer(required=False, load_default=DefaultMaker, description="变动的充值金额*100")
    change_present = fields.Integer(required=False, load_default=DefaultMaker, description="变动的赠送金额*100")
    change_deposited_mount = fields.Integer(required=False, load_default=DefaultMaker, description="变动的押金金额*100")
    deposited_stats = fields.Integer(required=False, load_default=DefaultMaker, description="押金状态")
