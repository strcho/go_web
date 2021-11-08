import base64
import hashlib
import json
from datetime import datetime
from urllib import parse

# import crypto
import requests
from OpenSSL import crypto

from service.payment import PayHelper
from service.payment.pay_utils.pay_constant import PAY_CONFIG
from mbutils import logger, cfg


class UnionPayForApp():

    def __init__(self):
        pass

    def get_read_pfx(self, flag):
        pfx = PAY_CONFIG.UNIONPAY_CONFIG.get("despostEncryptPfx") if flag else PAY_CONFIG.UNIONPAY_CONFIG.get(
            "encryptPfx")
        if not pfx:
            return False
        certs = crypto.load_certificate(crypto.FILETYPE_PEM, pfx.get('cert'))
        key = crypto.load_privatekey(crypto.FILETYPE_PEM, pfx.get('key'))
        cert_id = certs.get_serial_number()
        return key, cert_id

    def create_sign(self, params: dict, flag=False):
        filter_params = {}
        for key in params:
            if isinstance(key, str):
                filter_params[key] = params[key].strip() if isinstance(params[key], str) else params[key]
        sign_method = filter_params.get('signMethod')
        version = filter_params.get('version')
        if not sign_method:
            logger.info('signMethod must not null ', filter_params)
            # return False
        if not version:
            logger.info('version must not null ', filter_params)
            # return False
        key, cert_id = self.get_read_pfx(flag)  # 加密数据
        params['certId'] = cert_id
        sha256 = hashlib.sha256(self.build_sign_str(params).encode("utf-8")).hexdigest()
        private = crypto.sign(key, sha256, "sha256")
        return str(base64.b64encode(private), encoding="utf-8")

    def build_sign_str(self, params):
        req = []
        for k in sorted(params.keys()):
            if params[k] and k not in ('pfx', 'partner_key', 'sign', 'key'):
                req.append(f"{k}={params[k]}")
        return '&'.join(req)

    def parse_arguments(self, content):
        data = {}
        qs_params = parse.parse_qs(str(content))
        for name in qs_params.keys():
            data[name] = qs_params.get(name)[-1]
        return data

    def unionpay_post(self, req, url, flag):
        sign = self.create_sign(req, flag)
        logger.info(f'union_app sign: {sign}, url: {url}')
        req['signature'] = sign
        resp = requests.post(url=url, data=req, headers={
            "content-type": "application/x-www-form-urlencoded"}
                             )
        logger.info(f'unionpay_app resp resp: {resp.content.decode("utf-8")}')
        if resp.status_code != requests.codes.ok:
            return {"suc": False}
        else:
            content = self.parse_arguments(resp.content.decode("utf-8"))
            if content.get("respCode", "") != "00":
                logger.info(f'union_app content error {content}')
                return {"suc": False}
            else:
                # todo node上有payUrl这个参数，unipay_app官方文档（手机支付控件（含安卓pay）） 是没有的
                logger.info(f'union_app content success {content}')
                return {"suc": True, "tn": content.get("tn"), "payUrl": content.get("payUrl")}

    def get_merid(self, flag):
        return PAY_CONFIG.UNIONPAY_CONFIG.get('despostMerId') if flag else PAY_CONFIG.UNIONPAY_CONFIG.get('merId')

    def pay(self, total_fee, attach, order_id, url, flag=False):
        params = {
            "version": "5.1.0",
            "encoding": "utf-8",
            "bizType": "000000",
            "txnTime": datetime.now().strftime("%Y%m%d%H%M%S"),
            "backUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("notify_url_head").replace("ebike",
                                                                                 "anfu") + url + "/unionpayAppNotify",
            "currencyCode": "156",
            "txnAmt": str(total_fee),  # 交易金额, 分
            "txnType": "01",
            "reqReserved": attach,
            "txnSubType": "01",
            "accessType": "0",
            "signMethod": "01",
            "channelType": "08",
            "merId": self.get_merid(flag),
            "orderId": order_id
        }
        url = PAY_CONFIG.UNIONPAY_CONFIG.get("backTransUrlOfAppOrder")
        resp = self.unionpay_post(params, url, flag)
        if not resp.get('suc'):
            return {'suc': False}
        return resp

    def notify(self, notify):
        logger.info(f'union app notify notify:{notify}')
        resp_code = notify.get('respCode')
        req_reserved = notify.get("reqReserved")
        if resp_code != '00':
            logger.info('支付失败,respCode={resp_code}'.format(resp_code=resp_code))
            return {"suc": False, "info": "返回支付状态码错误"}

        validate_result = self.validate(notify)
        logger.info(f'union_app notify, validate_result: {validate_result}')
        if not validate_result:
            logger.info('支付失败,respCode={resp_code},validateResult={validate_result}'.format(
                resp_code=resp_code, validate_result=validate_result))
            return {"suc": False, "info": "签名校验错误"}
        attach = json.loads(req_reserved)
        if not attach.get('objectId'):
            logger.info('notify')
            return {"suc": False, "info": "用户信息查询失败"}
        logger.info(f'union_app notify, success attach: {attach}')
        return {"suc": True, "info": attach}

    # 解析返回域信息
    def get_string_kv(self, req_reserved):
        attach = {}
        data_list = req_reserved.split('?')
        req_reserved_params = data_list[0]
        front_params = data_list[1]  # 重定向frontUrl的参数
        req_data_list = req_reserved_params.split('&')
        for i in req_data_list:
            key_values = i.split('=')
            if key_values[0] == 'backFrontUrl':
                attach.update({key_values[0]: "{}?{}".format(key_values[1], front_params)})
            else:
                attach.update({key_values[0]: key_values[1]})
        return attach

    def validate(self, notify):
        signature = notify.pop('signature')
        link_string = self.build_sign_str(notify)
        digest = hashlib.sha256(bytes(link_string, encoding="utf-8")).hexdigest()
        signature = base64.b64decode(signature)
        sign_pubkey_cert = notify.get("signPubKeyCert", None)
        try:
            x509_ert = crypto.load_certificate(crypto.FILETYPE_PEM, sign_pubkey_cert)
            crypto.verify(x509_ert, signature, digest, 'sha256')
            return True
        except Exception as exc:
            logger.info(f'union app validate fail: {exc}')
            return False

    def refund(self, object_id, trade_no, refund_fee, total_fee, flag=False):
        """
        :param order_id: 退货的订单号
        :param trade_no: 原始的流水号
        :param txn_amt: 退款金额
        :param flag:
        :return:
        """
        trade_no, txn_amt = trade_no, refund_fee
        order_id = PayHelper.rand_str_24()
        params = {
            "version": '5.1.0',
            "encoding": 'utf-8',
            "signMethod": '01',
            "txnType": '04',
            "txnSubType": '00',
            "bizType": '000000',
            "channelType": '07',
            "backUrl": cfg.get('UnionpayConfig', '').get('notify_url_head').replace("ebike",
                                                                                    "anfu") + "/queryOrder/refund",
            # todo: 退款使用异步，还是同步操作
            "accessType": '0',
            "merId": self.get_merid(flag),
            "txnTime": datetime.now().strftime("%Y%m%d%H%M%S"),
            "orderId": order_id,  # 退货的订单号
            "origQryId": trade_no,  # 原始的流水号
            "txnAmt": txn_amt,  # 退款金额
        }
        url = PAY_CONFIG.UNIONPAY_CONFIG.get("backTransUrlOfAppRefound")
        resp = self.unionpay_post(params, url, False)
        logger.info(f'union refund resp: {resp}')
        if not resp.get('suc'):
            logger.info(f'union_app refund failed')
            return {'suc': False}
        logger.info(f'union_app refun success')
        return resp
