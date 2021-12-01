from marshmallow import (
    fields,
    Schema,
)

from mbutils import (
    ARG_DEFAULT,
)
from mbutils.mb_handler import ContextDeserializer
from utils.base_serializer import ReqBaseDeserializer
from utils.ebike_fields import EDateTime


class GetDepositCardDeserializer(ReqBaseDeserializer):
    """
    用户信息
    """

    pin = fields.Integer(required=True, description="用户标识")


class ModifyDepositCardDeserializer(ReqBaseDeserializer):
    """
    修改用户押金卡信息
    """

    pin = fields.Integer(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")


class SendDepositCardDeserializer(ReqBaseDeserializer):
    """
    添加用户押金卡
    """

    pin = fields.Integer(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")


class UserDepositCardSerializer(Schema):
    """
    用户押金卡
    """

    pin = fields.Integer(required=False, load_default=ARG_DEFAULT, description="用户标识")
    expired_date = EDateTime(description="过期时间")


class UserDepositCardDaysSerializer(Schema):
    """
    用户押金卡可用天数
    """

    days = fields.Integer(required=True, description='可用天数')
    expired_date_str = fields.String(required=True, description='到期时间')


class BusModifyDepositCardDeserializer(ContextDeserializer):
    """
    修改用户押金卡信息
    """

    pin = fields.Integer(required=True, description="用户标识")
    duration = fields.Integer(required=True, description="押金卡时长")
