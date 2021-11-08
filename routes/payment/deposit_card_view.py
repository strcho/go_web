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


class CreateDepositCardOrderHandler(MBHandler):
    """
     @api {post} /ebike/pay/depositCard 购买会员卡
     @apiVersion 1.0.0
     @apiGroup App
     @apiDescription 购买会员卡
     @apiParam objectId 用户ID
     @apiParam amount 金额（单位：分）
     @apiParam channel 支付渠道 alipay:支付宝支付， wepay：微信支付， wxlite：微信小程序支付 ， unionpay：云闪付
     @apiParam id 购买的骑行卡ID
     @apiParamExample request:
     {
         "objectId": "5a28f9b28d6d810061a37db3",
         "amount": 100, // 单位：分
         "channel": "alipay",
         "id": "1",  // 购买的骑行卡ID
         "openid": "openid", // 微信小程序支付时需要微信小程序openid/云闪付支付需要微信小程序openid
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
            ("amount", ValidType.STR, {"default": '0'}),
            ("channel", ValidType.STR, {"must": True}),
            ("id", ValidType.INT, {"default": 0}),
            ("openid", ValidType.STR, {"default": ''}),
            ("userAuthCode", ValidType.STR, {"default": ''}),
            ("carId", ValidType.INT, {"default": 0}),
            ("frontUrl", ValidType.STR, {"default": ''}),
            ("buyer_id", ValidType.STR, {"default": ''}),
            ("singleSplit", ValidType.BOOL, {"default": False}),
        ])
        object_id, amount, channel, id, openid, user_auth_code, car_id, front_url, buyer_id, single_split = valid_data
        pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.from_str(channel))
        res = yield mb_async(pf.create_factory)(valid_data)
        self.success(res)


class NotifyWepayDepositCardOrderHandler(MBHandler):
    """
    接收微信购买会员卡支付回调
    api: {post} '/ebike/pay/depositcard/wepayNotify'
    """
    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/depositcard/wepayNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.WEPAY)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result['info'])
        except Exception as e:
            logger.info(f'wx notify, e:{e}')
            self.origin_write(WX_ERROR_XML)


class NotifyWxLitePayDepositCardOrderHandler(MBHandler):
    """
    接收微信小程序购买会员卡支付回调
    api: {post} '/ebike/pay/depositcard/wxliteNotify'
    """
    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/depositcard/wxliteNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        msg = msg.replace("\ufeff", "")
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.WXLITE)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'wx_lite notify, e:{e}')
            self.origin_write(WX_ERROR_XML)


class NotifyAliPayDepositCardOrderHandler(MBHandler):
    """
    接收支付宝购买会员卡支付回调
    api: {post} '/ebike/pay/depositcard/alipayNotify'
    """
    @coroutine
    def post(self):
        # valid_data = self.valid_data_all([
        #     #  ("gmt_payment", ValidType.STR, {"must": True}), node代码中取得是这个参数，但是发现没有这个参数（所以在老的代码逻辑中，支付时间和创建时间都是系统的当前时间）
        #     ("gmt_create", ValidType.STR, {"must": True}),
        #     ("seller_email", ValidType.STR, {"must": True}),
        #     ("notify_time", ValidType.STR, {"must": True}),
        #     ("subject", ValidType.STR, {"must": True}),
        #     ("seller_id", ValidType.STR, {"must": True}),
        #     ("buyer_id", ValidType.STR, {"must": True}),
        #     ("passback_params", ValidType.STR, {"func": [lambda x: x.find("objectId")]}),
        #     ("version", ValidType.STR, {"must": True}),
        #     ("notify_id", ValidType.STR, {"must": True}),
        #     ("notify_type", ValidType.STR, {"must": True}),
        #     ("out_trade_no", ValidType.STR, {"must": True}),
        #     ("total_amount", ValidType.STR, {"must": True}),
        #     ("trade_status", ValidType.STR, {"func": [lambda x: x == "TRADE_SUCCESS"]}),
        #     ("refund_fee", ValidType.STR, {"func": [lambda x: x == '0.00']}),
        #     ("trade_no", ValidType.STR, {"must": True}),
        #     ("auth_app_id", ValidType.STR, {"must": True}),
        #     ("gmt_close", ValidType.STR, {"must": True}),
        #     ("buyer_logon_id", ValidType.STR, {"must": True}),
        #     ("app_id", ValidType.STR, {"func": [lambda x: x == cfg.get("alipay", {}).get("app_id")]}),
        #     ("sign_type", ValidType.STR, {"must": True}),
        #     ("sign", ValidType.STR, {"must": True}),
        # ])
        pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.ALIPAY)
        result = yield mb_async(pf.callback_factory)(self.request.body)
        self.success(result)


class NotifyAliLitePayDepositCardOrderHandler(MBHandler):
    """
    接收支付宝小程序购买会员卡支付回调
    api: {post} '/ebike/pay/depositcard/alipayliteNotify'
    """
    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(ALI_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.ALIPAYLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'ali_lite notify, e:{e}')
            self.origin_write(ALI_FAILED_RESP)


class NotifyUnionLiteDepositCardOrderHandler(MBHandler):
    """
    接收云闪付=>微信小程序支付购买会员卡的回调
    api: {post} '/ebike/pay/depositcard/unionpayNotify'
    """
    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_lite notify, e:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionAppDepositCardOrderHandler(MBHandler):
    """
    接收云闪付-APP支付购买会员卡的回调
    """
    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_APP)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_app notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionCodeDepositCardOrderHandler(MBHandler):
    """
    接收云闪付-二维码支付购买会员卡的回调
    """
    @coroutine
    def post(self):
        logger.info(f'union_code deposit_card notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE.UNIONPAY_CODE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_code notify, e:{e}')
            self.origin_write(UNION_FAILED_RESP)


class RefundDepositCardHandler(MBHandler):
    """
     * @apiParam objectId 用户ID
     * @apiParam trade_no 交易订单
     * @apiParam refund_fee 退款的金额
     * @apiParam fixManId 操作人员ID  做记录使用
     * @apiParamExample request:
     * {
     *     "trade_no": "5a28f9b28d6d810061a37db3",
     *     "refund_free": xxx
     *     "fixManId": "xxxx"
     *     "objectId": 3
     * }
    """
    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            ("channel", ValidType.INT, {"default": 0}),
            ("trade_no", ValidType.STR, {"must": True}),
            ("refund_free", ValidType.STR, {"must": True}),
            ("objectId", ValidType.STR, {"must": True}),
        ])
        #  将用户信息的判断提到前面进行判断，当没有查到用户信息时，直接返回
        channel, trade_no, refund_fee, object_id = valid_data
        if not channel:
            channel = yield mb_async(AccountService.get_channel)(trade_no, object_id)
        pf = PaymentFactory(BusinessType.DEPOSIT_CARD, RIDING_CHANNEL_TYPE(int(channel)))
        result = yield mb_async(pf.refund_factory)(object_id, trade_no, refund_fee, channel)
        self.success(result)





