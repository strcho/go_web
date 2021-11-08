from marshmallow import (
    Schema,
    fields,
)


class PaymentWalletDeserializer(Schema):

    channel = fields.String(required=True, description="支付渠道")
    objectId = fields.String(required=True, description="用户id")
    amount = fields.Integer(required=True, description="金额")
    openid = fields.String(missing="", description="")
    activeId = fields.Integer(required=False, missing=0, description="活动id")
    userAuthCode = fields.String(missing="", description="")
    carId = fields.String(missing="", description="车辆id")
    frontUrl = fields.String(missing="", description="")
    buyer_id = fields.String(missing="", description="")
    singleSplit = fields.Boolean(missing=False, description="")
