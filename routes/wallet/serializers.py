from marshmallow import fields

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
    change_

