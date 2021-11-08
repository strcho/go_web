import json
from base64 import encodebytes, b64decode
from datetime import datetime
from urllib.parse import quote_plus

import requests
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5

from service.payment import PayHelper
from mbutils import cfg, logger


class AliLiteService():

    def __init__(self):
        alilite_config = cfg.get("alipaylite", {})
        self.app_id = alilite_config.get("app_id", '')
        self.charset = alilite_config.get("charset", '')
        self.sign_type = alilite_config.get("sign_type", "RSA2")
        self.version = alilite_config.get("version", '')
        self.gateway = alilite_config.get("gateway", '')
        self.privateKey = alilite_config.get("privateKey", '')
        self.publicKey = alilite_config.get("publicKey", '')
        self.notify_url_head = alilite_config.get("notify_url_head", '').replace("ebike", "anfu")
        self.notify_url_method = "/alipayliteNotify"

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

    def check_response_sign(self, sign_dict: dict, sign_str: str):
        if not self.privateKey:
            return True
        if not sign_dict:
            return False
        validate_str = json.dumps(sign_dict)
        return bool(self._verify(validate_str, sign_str))

    def sign(self, params, url, method):
        sign_params = {
            "app_id": self.app_id,
            "method": method,
            "charset": self.charset,
            "sign_type": self.sign_type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "version": self.version,
            "notify_url": self.notify_url_head + url + self.notify_url_method,
            "biz_content": params
        }
        logger.info(f'xxxxx ali_lite sign_params: {sign_params}')
        # 排序后的字符串
        unsigned_items = self.ordered_data(sign_params)
        unsigned_string = "&".join("{0}={1}".format(k, v) for k, v in unsigned_items)
        signature = PKCS1_v1_5.new(RSA.import_key(self.privateKey)).sign(SHA256.new(unsigned_string.encode("utf-8")))
        # base64 编码，转换为unicode表示并移除回车
        sign = encodebytes(signature).decode("utf8").replace("\n", "")
        ordered_items = self.ordered_data(sign_params)
        quoted_string = "&".join("{0}={1}".format(k, quote_plus(v)) for k, v in ordered_items)
        signed_string = quoted_string + "&sign=" + quote_plus(sign)
        return signed_string

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

    def pay(self, buyer_id, out_trade_no, total_amount, url, subject, passback_params,
            method="alipay.trade.create"):  # method = 'alipay.trade.create'
        """
        :param buyer_id: buyer_id
        :param out_trade_no: 商户订单号。由商家自定义，64个字符以内，仅支持字母、数字、下划线且需保证在商户端不重复。
        :param total_amount: 订单总金额。单位为元，精确到小数点后两位，取值范围：[0.01,100000000]
        :param url: 支付回调表示那个商品部分的url example："/ridingCard"
        :param subject: 订单标题。注意：不可使用特殊字符，如 /，=，& 等
        :param method: 接口名称 example:"alipay.trade.create"
        :return:
        """
        biz_content = {
            "out_trade_no": out_trade_no,
            "total_amount": round(int(total_amount) / 100, 2),
            "subject": subject,
            "buyer_id": buyer_id,
            "timeout_express": '2h',
            "passback_params": json.dumps(passback_params),
        }
        logger.info(f'ali lite notify create_sign param: {biz_content}')
        sign_str = self.sign(json.dumps(biz_content), url, method)
        gateway = cfg.get('alipaylite').get('gateway')
        try:
            response = requests.post(gateway + "?" + sign_str)
            response = json.loads(response.content)
            logger.info(f'ali_lite create_order response:{response}')
            res_key = method.replace('.', "_") + '_response'
            res = response.get(res_key)
            if not res:
                return {}
            if self.check_response_sign(res, response.get('sign')):
                logger.info(f'ali lite res: {res}')
                if res['code'] and res['code'] == '10000':
                    return res
                else:
                    return {}
            else:
                return {}
        except Exception as e:
            logger.info(f'ali_lite create_order fail error:{e}')
            return {}

    def notify(self, notify):
        if not self.check_notify_sign(notify):
            return {"suc": False, "info": '验签失败'}
        return {"suc": True, "info": "成功"}

    def check_notify_sign(self, notify: dict):
        logger.info("check_notify_sign:", notify)
        sign_str = notify.get('sign')
        if not (self.publicKey or sign_str):
            return False
        # sign_type = notify.get('sign_type') or self.sign_type or 'RSA2'
        sign_args = notify
        del sign_args["sign"]
        del sign_args["sign_type"]
        unsigned_items = self.ordered_data(sign_args)
        unsigned_string = "&".join("{0}={1}".format(k, v) for k, v in unsigned_items)
        logger.info("check_notify_sign:", unsigned_string)
        res = self._verify(unsigned_string, sign_str)
        logger.info("check_notify_sign:", res)
        return res

    def refund(self, order_id, trade_no, refund_fee, total_fee):
        out_request_no = PayHelper.rand_str_24()
        biz_content = {
            "trade_no": trade_no,
            "refund_amount": refund_fee / 100,
            "out_request_no": out_request_no
        }
        logger.info(f'ali lite refund biz_content: {biz_content}')
        method = "alipay.trade.refund"
        sign_str = self.sign(biz_content, '', method)

        get_way = cfg.get('alipaylite').get('gateway')
        try:
            response = requests.post(get_way + "?" + sign_str)
            res = json.loads(response.content)
            logger.info(f'ali lite refund response1:{res}')
            res_key = method.replace('.', "_") + '_response'
            response = res[res_key]
            logger.info(f'ali lite refund response2:{response}')
            sign_str = res["sign"]
            if self.check_response_sign(response, sign_str):
                if response['code'] and response['code'] == '10000':
                    return {"suc": True, "info": response.get('sub_msg')}
                else:
                    return {"suc": False, "info": response.get("sub_msg")}
            else:
                return {"suc": False, "info": response.get("sub_msg")}
        except Exception as e:
            logger.info(f'ali_lite refund fail error:{e}')
            return {"suc": False, "info": e}
