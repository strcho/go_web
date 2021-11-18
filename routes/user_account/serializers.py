from marshmallow import fields

from utils.base_serializer import ReqBaseDeserializer


class UserAccountDeserializer(ReqBaseDeserializer):
    """
    获取用户资产信息
    """
    pass

    pin_id = fields.String(required=True, description="用户标识")
