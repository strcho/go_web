import json

from tornado.gen import coroutine

from routes.payment.serializers import PaymentWalletDeserializer
from service.big_screen.merchant import *
from service.payment import BusinessType
from service.payment.pay_utils.pay_constant import *
from service.payment.payment_factory import PaymentFactory
from mbutils import mb_async, cfg
from mbutils.constant import ValidType

from utils.arguments import (
    use_args,
)
from utils.constant.account import RIDING_CHANNEL_TYPE
from mbutils.mb_handler import MBHandler


class PaymentWalletHandler(MBHandler):

    @use_args(PaymentWalletDeserializer)
    def post(self, args,):
        """
        钱包支付
        ---
        tags: [钱包]
        summary: 钱包支付
        description:

        parameters:
          - in: body
            schema:
                PaymentWalletDeserializer

        responses:
            200:
                schema:
                    type: object
                    required:
                      - suc
                      - data
                    properties:
                        suc:
                            type: boolean
                        data:
                            type: dict
        """
        channel = args['channel']
        pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.from_str(channel))
        res = yield mb_async(pf.create_factory)(**args)
        logger.info('wallet create_order success')

        self.success(res)


class NotifyWxLiteWalletHandler(MBHandler):
    """
    接收微信购买钱包支付回调
    api: {post} '/ebike/pay/wallet/wxliteNotify'
    """

    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/wallet/wxliteNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        msg = msg.replace("\ufeff", "")
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.WXLITE)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result)
        except Exception as e:
            logger.error("wallet/wxliteNotify is error, {}".format(e))
            self.origin_write(WX_ERROR_XML)


class NotifyAliWalletHandler(MBHandler):
    """
        api:/anfu/pay/wallet/alipayNotify
        钱包支付
        application/x-www-form-urlencoded 转成 json
        gmt_create=2021-07-23+18:58:47&charset=utf-8&seller_email=qiyiqikeji@163.com&subject=骑币充值&sign=kfFjvEu8S7E/ya4xC4KXx5lJvUsa+fxtho7WD+Glxws1+t8QEGt/h/5P05LkDMGrn3gbzmJONVNmDk6LuR7j6Jkj1DgqgQggnfkO0jRg50oZlrr/cAhpc3DVbwpjV9Lx55UGrNIvYuNGRMaVYRrTEqsgyx+c9gHjLYvs2PZGcHEFBEp/dgEGpNgEbPgw+gFRAeRiZlQyfGsRLDjkwcfaByXXzU7zVPOEx11KqE8fo9QbkB9nPgoY1OkyYv+jFGUCAIftX9rr0ZbZPYQfrI5Zy2r0LLuCqZDboFuErG2MjG8YI5vqGsJB3aDlNVjXeQM2qp8b2LGTqUv1UQujiLKwDw==&buyer_id=2088212826576182&invoice_amount=0.01&notify_id=2021072300222185848076181411022901&fund_bill_list=[{"amount":"0.01","fundChannel":"ALIPAYACCOUNT"}]&notify_type=trade_status_sync&trade_status=TRADE_SUCCESS&receipt_amount=0.01&app_id=2019022263253609&buyer_pay_amount=0.01&sign_type=RSA2&seller_id=2088431218818781&gmt_payment=2021-07-23+18:58:48&notify_time=2021-07-23+18:58:48&passback_params={"objectId":"60fa5522f69486000112ef90"}&version=1.0&out_trade_no=20210723389188091214502781970036&total_amount=0.01&trade_no=2021072322001476181451511504&auth_app_id=2019022263253609&buyer_logon_id=158****5754&point_amount=0.00
        :return:
        {
          "suc": true,
          "data": ""
        }
        """

    @coroutine
    def post(self):
        #  TRADE_CLOSED（交易关闭）， TRADE_FINISHED（完结）， TRADE_SUCCESS（成功）， WAIT_BUYER_PAY（创建）
        #  回调这里只接受trade_status为TRADE_SUCCESS（目前待确定，其他的状态是否做日志的储存，或者是特殊的返回）
        #  当refund大于0时，则为退款回调
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
            ("trade_no", ValidType.STR, {"must": True}),
            ("auth_app_id", ValidType.STR, {"must": True}),
            ("buyer_logon_id", ValidType.STR, {"must": True}),
            ("app_id", ValidType.STR, {"func": [lambda x: x == cfg.get("alipay", {}).get("app_id")]}),
            ("sign_type", ValidType.STR, {"must": True}),
            ("sign", ValidType.STR, {"must": True}),
            ("fund_bill_list", ValidType.STR),
            ("receipt_amount", ValidType.STR),
            ("buyer_pay_amount", ValidType.STR),
            ("gmt_payment", ValidType.STR),
            ("point_amount", ValidType.STR),
            ("charset", ValidType.STR),
        ])
        pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.ALIPAY)
        result = yield mb_async(pf.callback_factory)(self.request.body)
        self.success(result)


class NotifyWepayWalletHandler(MBHandler):
    """
    接收微信购买钱包支付回调
    api: {post} '/ebike/pay/wallet/wepayNotify'
    """

    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/wallet/wepayNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.WEPAY)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result['info'])
        except Exception as e:
            self.origin_write(WX_ERROR_XML)


class NotifyAliLIteWalletHandler(MBHandler):
    """
        接收支付宝小程序购买钱包支付回调
        api: {post} '/ebike/pay/wallet/alipayliteNotify'

    """

    @coroutine
    def post(self):
        if not self.request.body:
            self.origin_write(ALI_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.ALIPAYLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            logger.info(f'ali lite notify result: {result}')
            self.origin_write(result)
        except Exception as e:
            logger.info(f'ali lite notify error: {e}')
            self.origin_write(ALI_FAILED_RESP)


class NotifyUnionWxLiteWalletHandler(MBHandler):
    """
    接收云闪付=>微信小程序支付购买钱包的回调
    api: {post} '/ebike/pay/wallet/unionpayNotify'
    """

    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/wallet/unionpayNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        msg = msg.replace("\ufeff", "")
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result)
        except Exception as e:
            logger.error("wallet/unionpayNotify is error, {}".format(e))
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionAPPWalletHandler(MBHandler):
    """
    接收云闪付-APP支付购买钱包的回调
    """

    @coroutine
    def post(self):
        logger.info(f'union_app notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.UNIONPAY_APP)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_app notify, e:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionCodeWalletHandler(MBHandler):
    """
    接收云闪付-二维码支付购买钱包的回调
    """

    @coroutine
    def post(self):
        logger.info(f'union_code wallet notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE.UNIONPAY_CODE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_code notify, e:{e}')
            self.origin_write(UNION_FAILED_RESP)


class PaymentWalletBalanceRefundHandler(MBHandler):
    """
        api:/anfu/pay/wallet/balance/refund
        钱包支付
        :return:
        {
          "suc": true,
          "data": ""
        }
        """

    @coroutine
    def post(self):
        #  TRADE_CLOSED（交易关闭）， TRADE_FINISHED（完结）， TRADE_SUCCESS（成功）， WAIT_BUYER_PAY（创建）
        #  回调这里只接受trade_status为TRADE_SUCCESS（目前待确定，其他的状态是否做日志的储存，或者是特殊的返回）
        #  当refund大于0时，则为退款回调
        valid_data = self.valid_data_all([
            ("channel", ValidType.INT, {"must": True}),
            ("trade_no", ValidType.STR, {"must": True}),
            ("refund_fee", ValidType.INT, {"must": True}),
            ("objectId", ValidType.STR, {"must": True}),
        ])
        #  将用户信息的判断提到前面进行判断，当没有查到用户信息时，直接返回
        channel, trade_no, refund_fee, object_id = valid_data
        pf = PaymentFactory(BusinessType.WALLET, RIDING_CHANNEL_TYPE(int(channel)))
        result = yield mb_async(pf.refund_factory)(object_id, trade_no, refund_fee, channel)
        self.success(result)
