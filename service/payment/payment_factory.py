from mbutils import MbException
from service.payment import BusinessType
from service.payment.deposit import *
from service.payment.deposit_card import *
from service.payment.favorable_card import *
from service.payment.riding_card import *
from service.payment.wallet import *


class PaymentFactory():
    def __init__(self, business: BusinessType, channel: RIDING_CHANNEL_TYPE):
        self.business = business
        self.channel = channel

    def create_factory(self, *args, **kwargs) -> dict:
        cls = BusinessFullPayInterface
        if self.business == BusinessType.DEPOSIT:
            cls = DepositCreateService
        elif self.business == BusinessType.DEPOSIT_CARD:
            cls = DepositCardCreateService
        elif self.business == BusinessType.FAVORABLE_CARD:
            cls = FavorableCardCreateService
        elif self.business == BusinessType.RIDING_CARD:
            cls = RidingCardCreateService
        elif self.business == BusinessType.WALLET:
            cls = WalletCreateService
        else:
            raise MbException("该支付方式未开通")

        obj = cls(*args, **kwargs)
        res = obj._check()
        if not res["suc"]:
            raise MbException(res["info"])

        # 创建参数是在init初始化的
        if self.channel == RIDING_CHANNEL_TYPE.WEPAY:
            func = obj.wx_pay
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAY:
            func = obj.ali_pay
        elif self.channel == RIDING_CHANNEL_TYPE.WXLITE:
            func = obj.wx_lite
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAYLITE:
            func = obj.ali_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_APP:
            func = obj.union_pay_app
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE:
            func = obj.union_pay_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_CODE:
            func = obj.union_pay_code
        else:
            raise MbException("业务错误")
        return func()

    def callback_factory(self, *args, **kwargs) -> dict:
        cls = BusinessFullPayInterface
        if self.business == BusinessType.DEPOSIT:
            cls = DepositNotifyService
        elif self.business == BusinessType.DEPOSIT_CARD:
            cls = DepositCardNotifyService
        elif self.business == BusinessType.FAVORABLE_CARD:
            cls = FavorableCardNotifyService
        elif self.business == BusinessType.RIDING_CARD:
            cls = RidingCardNotifyService
        elif self.business == BusinessType.WALLET:
            cls = WalletNotifyService
        else:
            return {"suc": False, "data": "该支付方式未开通"}

        obj = cls(self.channel.value)
        res = obj._check()
        if not res["suc"]:
            return res

        # 创建参数是在func带入的
        if self.channel == RIDING_CHANNEL_TYPE.WEPAY:
            func = obj.wx_pay
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAY:
            func = obj.ali_pay
        elif self.channel == RIDING_CHANNEL_TYPE.WXLITE:
            func = obj.wx_lite
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAYLITE:
            func = obj.ali_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_APP:
            func = obj.union_pay_app
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE:
            func = obj.union_pay_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_CODE:
            func = obj.union_pay_code
        else:
            return {"suc": False, "data": "业务找不到"}
        return func(*args, **kwargs)

    def refund_factory(self, *args, **kwargs) -> dict:
        cls = BusinessFullPayInterface
        if self.business == BusinessType.DEPOSIT:
            cls = DepositRefundService
        elif self.business == BusinessType.DEPOSIT_CARD:
            cls = DepositCardRefundService
        elif self.business == BusinessType.FAVORABLE_CARD:
            cls = FavorableCardRefundService
        elif self.business == BusinessType.RIDING_CARD:
            cls = RidingCardRefundService
        elif self.business == BusinessType.WALLET:
            cls = WalletRefundService
        else:
            raise MbException("该支付方式未开通")

        obj = cls(*args, **kwargs)
        res = obj._check()
        if not res["suc"]:
            raise MbException(res["info"])

        # 是在init里面初始化的
        if self.channel == RIDING_CHANNEL_TYPE.WEPAY:
            func = obj.wx_pay
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAY:
            func = obj.ali_pay
        elif self.channel == RIDING_CHANNEL_TYPE.WXLITE:
            func = obj.wx_lite
        elif self.channel == RIDING_CHANNEL_TYPE.ALIPAYLITE:
            func = obj.ali_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_APP:
            func = obj.union_pay_app
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE:
            func = obj.union_pay_lite
        elif self.channel == RIDING_CHANNEL_TYPE.UNIONPAY_CODE:
            func = obj.union_pay_code
        else:
            raise MbException("业务找不到")

        res = func()  # eg :{"suc": True, "info": ""}
        if res["suc"]:
            return res["info"]
        else:
            raise MbException(res["info"])
