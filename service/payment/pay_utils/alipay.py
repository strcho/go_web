from urllib import parse
import requests
from service.payment import PayDBService, PayHelper
from service.payment.pay_utils.pay_constant import PAY_CONFIG
from mbutils import logger, cfg
import json
import random
import urllib.request
from datetime import datetime
from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15
from Crypto.PublicKey import RSA
from urllib.parse import quote_plus
from base64 import b64decode, encodebytes


class AliService():
    def __init__(self):
        """
        {'amount': '1',
        'channel': 'alipay',
        'activeId': 528, '
        objectId': '60fa5522f69486000112ef90'}
        """
        super().__init__()
        alipay_config = cfg.get("alipay", {})
        self.app_id = alipay_config.get("app_id", '')
        self.charset = alipay_config.get("charset", '')
        self.sign_type = alipay_config.get("sign_type", '')
        self.version = alipay_config.get("version", '')
        self.gateway = alipay_config.get("gateway", '')
        self.privateKey = alipay_config.get("privateKey", '')
        self.publicKey = alipay_config.get("publicKey", '')
        self.notify_url_head = alipay_config.get("notify_url_head", '').replace("ebike", "anfu")
        self.notify_url_method = "/alipayNotify"

    @staticmethod
    def ordered_data(data):
        complex_keys = []
        for key, value in data.items():
            if isinstance(value, dict):
                complex_keys.append(key)
        # 将字典类型的数据dump出来
        for key in complex_keys:
            data[key] = json.dumps(data[key], separators=(',', ':'))
        return sorted([(k, v) for k, v in data.items()])

    # def sign(self, params: dict, url_method: str, method='alipay.trade.app.pay'):
    def sign(self, url: str, biz_countent: dict, method='alipay.trade.app.pay'):
        sign_params = {
            "app_id": self.app_id,
            "method": method,
            "charset": self.charset,
            "sign_type": self.sign_type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "version": self.version,
            "notify_url": self.notify_url_head + url + self.notify_url_method,
            "biz_content": biz_countent
        }
        logger.info(f"xxx ali sign sign_params: {sign_params}")
        # 排序后的字符串
        unsigned_items = self.ordered_data(sign_params)
        unsigned_string = "&".join("{0}={1}".format(k, v) for k, v in unsigned_items)
        # signer = pkcs1_15.new(self.privateKey)
        signature = pkcs1_15.new(RSA.import_key(self.privateKey)).sign(SHA256.new(unsigned_string.encode("utf-8")))
        # base64 编码，转换为unicode表示并移除回车
        sign = encodebytes(signature).decode("utf8").replace("\n", "")
        ordered_items = self.ordered_data(sign_params)
        logger.info(f"ali pay ordered_items:{ordered_items}")
        quoted_string = "&".join("{0}={1}".format(k, quote_plus(v)) for k, v in ordered_items)
        signed_string = quoted_string + "&sign=" + quote_plus(sign)
        return signed_string

    def _verify(self, raw_content: str, signature: str):
        # 开始计算签名
        # logger.info("_verify:参数", raw_content, signature)
        signer = pkcs1_15.new(RSA.import_key(self.publicKey))
        digest = SHA256.new()
        try:
            # json字符串格式
            # 总是做一下去空格, 验证的时候不能有空格
            digest.update(json.dumps(json.loads(raw_content), separators=(',', ':')).encode("utf8"))
        except Exception:
            # {}={}&{}={}格式
            digest.update(raw_content.encode("utf8"))
        try:
            signer.verify(digest, b64decode(signature))
            # logger.info("ali pay verify is success")
        except Exception as e:
            logger.info("ali pay verify is error,", e)
            return False
        return True

    def validate_verify(self, notify):
        logger.info("decodeNotify", notify)
        total_amount = notify["total_amount"]
        app_id = notify["app_id"]
        trade_status = notify["trade_status"]
        trade_no = notify["trade_no"]
        gmt_payment = notify["gmt_payment"]
        passback_params = notify["passback_params"]
        refund_fee = ["refund_fee"]
        out_trade_no = ["out_trade_no"]

        if app_id != PAY_CONFIG.ALI_CFG.get("app_id"):
            return {"suc": False, "info": "app_id不符"}
        if trade_status != "TRADE_SUCCESS":
            return {"suc": False, "info": "非交易成功通知"}
        if refund_fee:
            return {"suc": False, "info": "订单部分退款回调"}

        is_account = PayDBService().is_account_by_trade(trade_no)
        if is_account:
            return {"suc": False, "info": "重复的支付回调"}
        return {"suc": True, "info":""}

    def notify(self, notify):
        if not self.check_notify_sign(notify):
            return {"suc": True, "info": '验签失败'}
        return {"suc": True, "info": "成功"}

    def check_notify_sign(self, notify: dict):
        logger.info("check_notify_sign:", notify)
        sign_str = notify.get('sign')
        if not (self.publicKey or sign_str):
            return False
        sign_args = notify
        del sign_args["sign"]
        del sign_args["sign_type"]
        unsigned_items = self.ordered_data(sign_args)
        # unsigned_string = "&".join("u{}={}".format(k, v) for k, v in unsigned_items)
        unsigned_string = "&".join(u"{}={}".format(k, v) for k, v in unsigned_items)
        res = self._verify(unsigned_string, sign_str)
        logger.info("check_notify_sign:", res)
        return res

    def check_response_sign(self, sign_dict: dict, sign_str: str):
        if not self.privateKey:
            return True
        if not sign_dict:
            return False
        validate_str = json.dumps(sign_dict)
        return bool(self._verify(validate_str, sign_str))

    def refund(self, order_id, trade_no, refund_fee, total_fee):
        out_request_no = PayHelper.rand_str_24()
        biz_content = {
            "trade_no": trade_no,
            "refund_amount": refund_fee / 100,
            "out_request_no": out_request_no
        }
        sign_str = self.sign("", biz_content, "alipay.trade.refund")
        response = requests.post("{}?{}".format(cfg.get("alipay", {}).get("gateway", ""), sign_str))
        response = response.text
        """
        {"alipay_trade_refund_response":
        {"code":"10000",
        "msg":"Success",
        "buyer_logon_id":"151******06",
        "buyer_user_id":"2088802762786839",
        "fund_change":"N",
        "gmt_refund_pay":"2021-07-29 11:35:37",
        "out_trade_no":"20210729299150549905685115121425",
        "refund_fee":"0.01","send_back_fee":"0.00",
        "trade_no":"2021072922001486831458264940"},
        "sign":"AJkLxEEN7pAFFo+/zqNPciiyJBxSkC7QKgkMYtpQH9u9Is+fJIo3LrqvBnnRwPIuBN+g3rDTwHCWrhcEERvMSw0t5Dl4TqUqY8UmcW0gAC7iBIai+q8SWAf26vke6VnkG5cd/47xrNaLHZWHBYPEWJ0o0+qpkgcjNIpy0bC/Dd8RJP1pAYzzWHtzASiJXk6DO9d0y8Z8GlLtFBcrvImsrQoYjkA+2PSrND4ruciDanJPxCigPBeYM8RvHcq/bal6rd4KV81RCbSnsOKztWnNLRk5keQu6YYg0amo6dB7RoQQbmaEzZZdxGCC1vQ+igvA5LDRgIrSa/F5LIl1u9Ou2A=="}
        """
        logger.info("支付宝退款信息:", response)
        # 这里返回的是一个json,和notify格式不一样
        res = json.loads(response)
        response = res["alipay_trade_refund_response"]
        sign_str = res["sign"]
        if not self.check_response_sign(response, sign_str):
            logger.info("验签失败, response: {response}".format(response=response))
            return {"suc": False, "info": "验签失败"}

        if response.get("code") != "10000":
            logger.info("返回码错误, response: {response}".format(response=response))
            if response.get("sub_code") == "ACQ.TRADE_HAS_FINISHED":
                logger.info("交易已结束, response: {response}".format(response=response))
            return {"suc": False, "info": response.get("sub_msg")}
        logger.info("user refund suc, response: {response}".format(response=response))
        return {"suc": True, "info": response.get("sub_msg")}
