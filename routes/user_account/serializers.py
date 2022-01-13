from marshmallow import (
    fields,
    Schema,
)
from marshmallow.schema import BaseSchema

from mbutils.mb_handler import ContextDeserializer
from routes.deposit_card.serializers import UserDepositCardSerializer
from routes.discount.serializers import UserDiscountSerializer
from routes.favorable_card.serializers import UserFavorableCardSerializer
from routes.free_order.serializers import UserFreeOrderSerializer
from routes.riding_card.serializers import RidingCardInfoSerializer
from routes.wallet.serializers import UserWalletSerializer
from utils.base_serializer import ReqBaseDeserializer


class UserAccountDeserializer(ReqBaseDeserializer):
    """
    获取用户资产信息
    """
    pin = fields.String(required=True, description="用户标识")


class BusUserAccountDeserializer(ContextDeserializer):
    """
    获取用户资产信息
    """
    pin = fields.String(required=True, description="用户标识")


class CliUserAccountDeserializer(ContextDeserializer):
    """
    获取用户资产信息
    """
    pin = fields.String(required=True, description="用户标识")


class UserAccountSerializer(BaseSchema):
    """
    用户资产信息序列化
    """

    user_wallet = fields.Nested(UserWalletSerializer(), description='用户钱包')
    user_riding_card = fields.Nested(RidingCardInfoSerializer(), description='用户骑行卡')
    user_deposit_card = fields.Nested(UserDepositCardSerializer(), description='用户押金卡')
    user_favorable_card = fields.Nested(UserFavorableCardSerializer(), description='用户优惠卡')
    user_free_order = fields.Nested(UserFreeOrderSerializer(), description='用户免单')
    user_discount = fields.Nested(UserDiscountSerializer(), description='用户折扣')
