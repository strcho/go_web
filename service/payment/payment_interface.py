class BusinessFullPayInterface():
    """ 业务实现需要的支付接口 """

    def _check(self) -> dict:
        return {"suc": True, "info": ''}

    def wx_pay(self, *args, **kwargs):
        pass

    def ali_pay(self, *args, **kwargs):
        pass

    def wx_lite(self, *args, **kwargs):
        pass

    def ali_lite(self, *args, **kwargs):
        pass

    def union_pay_app(self, *args, **kwargs):
        pass

    def union_pay_lite(self, *args, **kwargs):
        pass

    def union_pay_code(self, *args, **kwargs):
        pass


class NotifyMixing():

    def __init__(self):
        self.channel = ""

    """ 单独不能工作, 需要其他的地方提供channel """

    def get_notify_func(self):
        from service.payment.pay_utils.alilitepay import AliLiteService
        from service.payment.pay_utils.alipay import AliService
        from service.payment.pay_utils.unionpay_app import UnionPayForApp
        from service.payment.pay_utils.unionpay_code import UnionPayForCode
        from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
        from service.payment.pay_utils.wepay import WePayService
        from service.payment.pay_utils.wxlitepay import WeLiteService
        from utils.constant.account import RIDING_CHANNEL_TYPE
        table = {
            RIDING_CHANNEL_TYPE.WEPAY.value: WePayService().wx_notify,
            RIDING_CHANNEL_TYPE.WXLITE.value: WeLiteService().wx_notify,
            RIDING_CHANNEL_TYPE.ALIPAY.value: AliService().notify,
            RIDING_CHANNEL_TYPE.ALIPAYLITE.value: AliLiteService().notify,
            RIDING_CHANNEL_TYPE.UNIONPAY_CODE.value: UnionPayForCode().notify,
            RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value: UnionPayForWXLite().notify,
            RIDING_CHANNEL_TYPE.UNIONPAY_APP.value: UnionPayForApp().notify
        }
        return table[self.channel]
