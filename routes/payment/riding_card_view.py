import json

from tornado.gen import coroutine

from service.account_service import AccountService
from service.payment import BusinessType
from service.payment.favorable_card import FavorableCardService
from service.payment.pay_utils.pay_constant import *
from service.payment.payment_factory import PaymentFactory
from mbutils import mb_async, logger, cfg
from mbutils.constant import ValidType
from utils.constant.account import RIDING_CHANNEL_TYPE
from mbutils.mb_handler import MBHandler


class CreateRidingCardOrderHandler(MBHandler):
    """
    api: {post} /ebike/pay/ridingCard 购买骑行卡
    objectId 用户ID
    @apiParam amount 金额（单位：分）
    @apiParam channel 支付渠道 alipay:支付宝支付， wepay：微信支付， wxlite：微信小程序支付
    @apiParamExample request:
    {
        "objectId": "5a28f9b28d6d810061a37db3",
        "channel": "alipay",
        "ridingCardId": "1",  // 购买的骑行卡ID
        "serialType":"6",     // 交易类型
        "openid": "openid", // 微信小程序支付时需要微信小程序openid
    }
    @apiSuccessExample response:
    {
      "suc":true,
      "data":{
          // order info
      }
    }
    """
    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            ("objectId", ValidType.STR, {"must": True}),
            ("amount", ValidType.INT, {"default": 0}),
            ("channel", ValidType.STR, {"must": True}),
            ("serialType", ValidType.INT, {"must": True}),
            ("ridingCardId", ValidType.INT, {"must": True}),
            ("openid", ValidType.STR, {"default": ''}),
            ("userAuthCode", ValidType.STR, {"default": ''}),
            ("carId", ValidType.INT, {"default": 0}),
            ("frontUrl", ValidType.STR, {"default": ''}),
            ("buyer_id", ValidType.INT, {"default": 0}),
            ("singleSplit", ValidType.BOOL, {"default": False}),
        ])
        logger.info(f"pay/ridingCard valid_data: {valid_data}")
        object_id, amount, channel, serial_type, riding_card_id, openid, user_auth_code, car_id, \
        front_url, buyer_id, single_split = valid_data
        pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.from_str(channel))
        res = yield mb_async(pf.create_factory)(valid_data)
        self.success(res)


class NotifyWepayRindingCardOrderHandler(MBHandler):
    """
    接收微信购买骑行卡支付回调
    api: {post} '/ebike/pay/ridingCard/wepayNotify'
    """
    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/ridingCard/wepayNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.WEPAY)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result['info'])
        except Exception as e:
            self.origin_write(WX_ERROR_XML)


class NotifyWxLitePayRindingCardOrderHandler(MBHandler):
    """
    接收微信小程序购买骑行卡支付回调
    api: {post} '/ebike/pay/ridingCard/wxliteNotify'
    """
    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/ridingCard/wxliteNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        msg = msg.replace("\ufeff", "")
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.WXLITE)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result)
        except Exception as e:
            self.origin_write(WX_ERROR_XML)


class NotifyAliPayRidingCardOrderHandler(MBHandler):
    """
    接收支付宝购买骑行卡支付回调
    api: {post} '/ebike/pay/ridingCard/alipayNotify'
    """
    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            #  ("gmt_payment", ValidType.STR, {"must": True}), node代码中取得是这个参数，但是发现没有这个参数（所以在老的代码逻辑中，支付时间和创建时间都是系统的当前时间）
            ("gmt_create", ValidType.STR, {"must": True}),
            ("seller_email", ValidType.STR, {"must": True}),
            ("notify_time", ValidType.STR, {"must": True}),
            ("subject", ValidType.STR, {"must": True}),
            ("seller_id", ValidType.STR, {"must": True}),
            ("buyer_id", ValidType.STR, {"must": True}),
            ("passback_params", ValidType.STR, {"func": [lambda x: x.find("objectId")]}),
            ("version", ValidType.STR, {"must": True}),
            ("notify_id", ValidType.STR, {"must": True}),
            ("notify_type", ValidType.STR, {"must": True}),
            ("out_trade_no", ValidType.STR, {"must": True}),
            ("total_amount", ValidType.STR, {"must": True}),
            ("trade_status", ValidType.STR, {"func": [lambda x: x == "TRADE_SUCCESS"]}),
            ("refund_fee", ValidType.STR, {"func": [lambda x: x == '0.00']}),
            ("trade_no", ValidType.STR, {"must": True}),
            ("auth_app_id", ValidType.STR, {"must": True}),
            ("gmt_close", ValidType.STR, {"must": True}),
            ("buyer_logon_id", ValidType.STR, {"must": True}),
            ("app_id", ValidType.STR, {"func": [lambda x: x == cfg.get("alipay", {}).get("app_id")]}),
            ("sign_type", ValidType.STR, {"must": True}),
            ("sign", ValidType.STR, {"must": True}),
        ])
        pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.ALIPAY)
        result = yield mb_async(pf.callback_factory)(self.request.body)
        self.success(result)


class NotifyAliLitePayRidingCardOrderHandler(MBHandler):
    """
        接收支付宝小程序购买骑行卡支付回调
        api: {post} '/ebike/pay/ridingCard/alipayliteNotify'
    """
    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(ALI_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.ALIPAYLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'ali_lite notify, error:{e}')
            self.origin_write(ALI_FAILED_RESP)


class NotifyUnionLiteRidingCardOrderHandler(MBHandler):
    """
    接收云闪付=>微信小程序支付购买骑行卡的回调
    api: {post} '/ebike/pay/ridingCard/unionpayNotify'
    """
    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_lite notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionAppRidingCardOrderHandler(MBHandler):
    """
    接收云闪付-APP支付购买骑行卡的回调
    """
    @coroutine
    def post(self):
        logger.info(f'union_app notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_APP)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_app notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionCodeRidingCardOrderHandler(MBHandler):
    """
    接收云闪付-二维码支付购买骑行卡的回调
    """
    @coroutine
    def post(self):
        logger.info(f'union_code notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_CODE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_code notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class RidingCardRefundHandler(MBHandler):
    """
    platform/ridingCard/refund 平台骑行卡退款
    {
      "trade_no": "5a28f9b28d6d810061a37db3",
      "refund_free": xxx
      "fixManId": "xxxx"
      "objectId": 3
    }
    """

    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            ("trade_no", ValidType.STR, {"must": True}),
            ("refund_free", ValidType.INT, {"must": True}),
            ("objectId", ValidType.STR, {"must": True}),
        ])
        #  将用户信息的判断提到前面进行判断，当没有查到用户信息时，直接返回
        trade_no, refund_fee, object_id = valid_data
        channel = yield mb_async(AccountService.get_channel)(trade_no, object_id)
        pf = PaymentFactory(BusinessType.RIDING_CARD, RIDING_CHANNEL_TYPE(int(channel)))
        result = yield mb_async(pf.refund_factory)(object_id, trade_no, refund_fee, channel)
        self.success(result)





