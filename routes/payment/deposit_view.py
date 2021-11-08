import json

from tornado.gen import coroutine

from service.account_service import AccountService
from service.payment import BusinessType
from service.payment.pay_utils.pay_constant import *
from service.payment.payment_factory import PaymentFactory
from mbutils import mb_async, logger, cfg
from mbutils.constant import ValidType
from utils.constant.account import RIDING_CHANNEL_TYPE
from mbutils.mb_handler import MBHandler


class CreateDepositOrderHandler(MBHandler):
    """
     @api {post} /ebike/pay/deposit 押金支付
     @apiName 押金支付
     @apiVersion 1.0.0
     @apiGroup pay
     @apiDescription 支付押金
     @apiParam objectId 用户ID
     @apiParam channel 支付渠道 alipay:支付宝支付， wepay：微信支付， wxlite：微信小程序支付
     @apiParamExample request:
     {
         "objectId": "5a28f9b28d6d810061a37db3",
         "channel": "alipay",
         "openid": "xxxxxxx"
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
            ("channel", ValidType.STR, {"must": True}),
            ("objectId", ValidType.STR, {"must": True}),
            ("openid", ValidType.STR, {"default": ''}),
            ("userAuthCode", ValidType.STR, {"default": ''}),
            ("carId", ValidType.STR, {"default": ''}),
            ("frontUrl", ValidType.STR, {"default": ''}),
            ("singleSplit", ValidType.BOOL, {"default": False}),  # todo singleSplit 传来的类型未知，但要转成bool类型
            ("buyer_id", ValidType.STR, {"default": ""}),
        ])
        channel = valid_data[0]
        pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.from_str(channel))
        res = yield mb_async(pf.create_factory)(valid_data)
        logger.info(f'deposit crete_order success res: {res}')
        self.success(res)


class NotifyWepayDepositHandler(MBHandler):
    """
     {
     "gmt_create":"2021-07-23 07:09:50",
     "charset":"utf-8",
     "seller_email":"qiyiqikeji@163.com",
     "subject":"钱包充值",
     "sign":"jmr333xkGaXktQsnr/ifBy52iQyJlBZMG9Lhq2KyYrjgp0QuH/h3ezXaOAVhujtgGTArGqI/t4tSdZZVO8HKFl9DzTFGs7vXEaABw0uTUJrUEfwuLUA8iS6K9h6MV1p3VfTKTPhSdD+8exEUVXE3HmaafJ/8m3sNDt9tdeRmm8Cufv5OXoNGL0dezTEWB2JO0IFW5zErlrhL8IJT/4WwVQvfeyle2PJ3gNQmbyPIsygxmzKio4v20MnwL73Xozt6DtAtVuYi2im/hFqSEidBSFkLcDEH7dIgQ9BVlHVh8FzbV6lIAx1VcM5le/N510/wgfP/t2vzcPZDQGiQc3Z5gg==",
     "buyer_id":"2088612267704841",
     "invoice_amount":"10.00",
     "notify_id":"2021072300222070953004841419639533",
     "fund_bill_list":"[{\"amount\":\"10.00\",\"fundChannel\":\"ALIPAYACCOUNT\"}]",
     "notify_type":"trade_status_sync",
     "trade_status":"TRADE_SUCCESS",
     "receipt_amount":"10.00",
     "buyer_pay_amount":"10.00",
     "app_id":"2021001169680986",
     "sign_type":"RSA2",
     "seller_id":"2088431218818781",
     "gmt_payment":"2021-07-23 07:09:53",
     "notify_time":"2021-07-23 07:09:53",
     "version":"1.0",
     "out_trade_no":"20210723570285626447857410646522",
     "total_amount":"10.00",
     "trade_no":"2021072322001404841401208664",
     "auth_app_id":"2021001169680986",
     "buyer_logon_id":"153****1621",
     "point_amount":"0.00"
     }
    接收微信购买押金支付回调
    api: {post} '/ebike/pay/deposit/wepayNotify'
    """

    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/deposit/wepayNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.WEPAY)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result['info'])
        except Exception as e:
            self.origin_write(WX_ERROR_XML)


class NotifyWxLitePayDepositOrderHandler(MBHandler):
    """
    接收微信小程序购买押金支付回调
    api: {post} '/ebike/pay/deposit/wxliteNotify'
    """

    @coroutine
    def post(self):
        msg = self.request.body.decode('utf-8')
        logger.info('/pay/deposit/wxliteNotify', msg)
        if not msg:
            self.origin_write(WX_ERROR_XML)
        msg = msg.replace("\ufeff", "")
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.WXLITE)
            result = yield mb_async(pf.callback_factory)(msg)
            self.origin_write(result)
        except Exception as e:
            self.origin_write(WX_ERROR_XML)


class NotifyAliPayDepositOrderHandler(MBHandler):
    """
     {"gmt_create":"2021-07-22 21:38:02",
     "charset":"utf-8",
     "seller_email":"qiyiqikeji@163.com",
     "notify_time":"2021-07-23 09:06:15",
     "subject":"骑币充值",
     "sign":"HnD63WrtX3Ad88RJYEczHWSBhhxzv84DZaBahwIgIAD8by6jovjiuahPmqyELsaa9FzR1ggaRSZnNzJDdDVB6WLITpW32XoET58+9unDF0SmzGpa/8lPjqVF1H7wIIMfK2IweTO1kf3qffl/NTbN4dR+y9oIRxxHqkyKR8Yv/yOf7QpybSwKTX6OBu7+FHL+Fg6FavgdRWVsaD806LCePGoqdfGlj2EYdlaBzEwj1TF1Irppsxa28/bOMuwbZiiyDSLlCx5idbOESFg0fMajJT+8GPLe00KbynyJSldRqqKin/mCldh114rb+mzEo0XGGn3QNMTGhnQmTNIyEvoeEg==",
     "buyer_id":"2088822943668125",
     "passback_params":"objectId=5fdb6340b16fb20001d97bda",
     "version":"1.0",
     "notify_id":"2021072200222233848068121407135793",
     "notify_type":"trade_status_sync",
     "out_trade_no":"20210722618016562262155336898160",
     "total_amount":"2.00",
     "trade_status":"TRADE_CLOSED",
     "refund_fee":"0.00",
     "trade_no":"2021072222001468121449204824",
     "auth_app_id":"2019022263253609",
     "gmt_close":"2021-07-22 23:38:48",
     "buyer_logon_id":"178****9363",
     "app_id":"2019022263253609",
     "sign_type":"RSA2",
     "seller_id":"2088431218818781"}
    接收支付宝购买押金支付回调
    api: {post} '/ebike/pay/deposit/alipayNotify'
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
        pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.ALIPAY)
        result = yield mb_async(pf.callback_factory)(self.request.body)
        self.success(result)


class NotifyAliLitePayDepositOrderHandler(MBHandler):
    """
    接收支付宝小程序购买押金支付回调
    api: {post} '/ebike/pay/deposit/alipayliteNotify'
    """

    @coroutine
    def post(self):
        logger.info(f'ali_lite deposit notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(ALI_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.ALIPAYLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'ali_lite notify, error:{e}')
            self.origin_write(ALI_FAILED_RESP)


class NotifyUnionLiteDepositOrderHandler(MBHandler):
    """
    接收云闪付=>微信小程序支付购买押金的回调
    api: {post} '/ebike/pay/deposit/unionpayNotify'
    """

    @coroutine
    def post(self):
        logger.info(f'union_lite deposit notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_lite notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionAppDepositOrderHandler(MBHandler):
    """
    接收云闪付-APP支付购买押金的回调
    api: {post} '/ebike/pay/deposit/unionpayAppNotify'
    """

    @coroutine
    def post(self):
        logger.info(f'union_app deposit notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.UNIONPAY_APP)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            logger.info(f"union_app deposit result: {result}")
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_app deposit notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class NotifyUnionCodeDepositOrderHandler(MBHandler):
    """
    接收云闪付-二维码支付购买会员卡的回调
    api: {post} '/ebike/pay/deposit/unionpayCodeNotify'
    """

    @coroutine
    def post(self):
        logger.info(f'union_code deposit notify, body: {self.request.body}')
        if not self.request.body:
            self.origin_write(UNION_FAILED_RESP)
        try:
            pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE.UNIONPAY_CODE)
            result = yield mb_async(pf.callback_factory)(self.request.body)
            self.origin_write(result)
        except Exception as e:
            logger.info(f'union_code deposit notify, error:{e}')
            self.origin_write(UNION_FAILED_RESP)


class RefundDepositHandler(MBHandler):
    """
        @api {post} /ebike/pay/deposit/refund 押金退款
        @apiVersion 1.0.0
        @apiGroup pay
        @apiDescription 押金退款
        @apiParam objectId 用户ID
        @apiParamExample request:
        {
            "objectId": "5a28f9b28d6d810061a37db3",
        }
        @apiSuccessExample response:
        {
            "suc":false,
            "error":{
                // order info
            refundtype  = 2  退押金失败，请联系客服
            refundtype  = 3  订单未支付无法退押金
            refundtype  = 4  业务错误导致退款失败
            }
        }
    """

    @coroutine
    def post(self):
        valid_data = self.valid_data_all([
            ("objectId", ValidType.STR, {"must": True}),
        ])
        #  将用户信息的判断提到前面进行判断，当没有查到用户信息时，直接返回
        object_id = valid_data[0]
        trade_no, refund_fee, channel = yield mb_async(AccountService.get_deposit_refund_info)(object_id)  # 押金需要自行获取退款信息
        pf = PaymentFactory(BusinessType.DEPOSIT, RIDING_CHANNEL_TYPE(int(channel)))
        result = yield mb_async(pf.refund_factory)(object_id, trade_no, int(refund_fee), int(channel))
        self.success(result)

# class CompleteWepayDepositOrderHandler(MBHandler):
#     """
#     接收微信购买押金支付回调
#     api: {post} '/ebike/pay/deposit/wepayNotify'
#     """
#     @coroutine
#     def post(self, request):
#         msg = request.body.decode('utf-8')
#         logger.info('/pay/ridingCard/wxliteNotify', msg)
#         if not msg:
#             self.origin_write("""<xml>
#                                 <return_code><![CDATA[FAIL]]></return_code>
#                                 <return_msg><![CDATA[INPUT_PARAMS_MISS]]></return_msg>
#                             </xml>""")
#         msg = msg.replace("\ufeff", "")
#         try:
#             result = yield mb_async(DepositViewService().wx_complete_order)(msg)
#             self.origin_write(result)
#         except Exception as e:
#             self.origin_write(ERROR_NOTIFY_XML)
