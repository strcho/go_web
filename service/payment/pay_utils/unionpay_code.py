import base64
import hashlib
from datetime import datetime
from urllib import parse

import requests
from OpenSSL import crypto

from service.payment.pay_utils.pay_constant import PAY_CONFIG
from mbutils import cfg, logger


class UnionPayForCode():

    def __init__(self):
        pass

    def get_encrypt_cert(self, flag):
        cert_str = PAY_CONFIG.UNIONPAY_CONFIG.get("despostEncryptCert") if flag else PAY_CONFIG.UNIONPAY_CONFIG.get("encryptCert")
        # todo  c.readCertPEM(certStr);
        #     let hSerial = c.getSerialNumberHex();
        #     cert.certId = new jsrsasign.BigInteger(hSerial, 16).toString();
        #     cert.key = jsrsasign.X509.getPublicKeyFromCertPEM(certStr);
        key = ''
        cert_id = ''
        return key, cert_id

    def get_merid(self, flag):
        return PAY_CONFIG.UNIONPAY_CONFIG.get('despostMerId') if flag else PAY_CONFIG.UNIONPAY_CONFIG.get('merId')

    def create_link_string(self, params):
        return "&".join(["{}={}".format(i, params[i]) for i in sorted(params) if
                  params[i] and i not in ('pfx', 'partner_key', 'sign', 'key')])

    def get_read_pfx(self, flag):
        pfx = PAY_CONFIG.UNIONPAY_CONFIG.get("despostEncryptPfx") if flag else PAY_CONFIG.UNIONPAY_CONFIG.get("encryptPfx")
        if not pfx:
            return False

        certs = crypto.load_certificate(crypto.FILETYPE_PEM, pfx.get('cert'))
        key = crypto.load_privatekey(crypto.FILETYPE_PEM, pfx.get('key'))
        cert_id = certs.get_serial_number()
        return key, cert_id

    def build_sign_str(self, params):
        req = []
        for k in sorted(params.keys()):
            if params[k] and k not in ('pfx', 'partner_key', 'sign', 'key'):
                req.append(f"{k}={params[k]}")
        return '&'.join(req)

    def create_sign(self, params, flag=False):
        logger.info(f'union_code params: {params}')
        # 将传入对象的空字符串剔除
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

    @staticmethod
    def get_termid(car_id):
        if PAY_CONFIG.UNIONPAY_CONFIG.get("defaultTermId"):
            return PAY_CONFIG.UNIONPAY_CONFIG.get("defaultTermId")
        if not car_id:
            return "00600003"

        term_id = []
        for i in range(len(car_id)):
            if len(term_id) == 8:
                break
            term_id.append(car_id[i])
        if len(term_id) < 8:
            term_id.append('0')
        return ''.join(term_id)

    def parse_arguments(self, content):
        data = {}
        qs_params = parse.parse_qs(str(content))
        for name in qs_params.keys():
            data[name] = qs_params.get(name)[-1]
        return data

    def unionpay_post(self, req, flag):
        sign = self.create_sign(req, flag)
        logger.info(f'union_code sign: {sign}')
        req['signature'] = sign
        resp = requests.post(url=PAY_CONFIG.UNIONPAY_CONFIG.get("backTransUrlOfAppOrder"), data=req, headers={
            "content-type": "application/x-www-form-urlencoded"}
                             )
        logger.info('unionpay_code resp status_code')
        if resp.status_code != requests.codes.ok:
            return {"suc": False}
        else:
            content = self.parse_arguments(resp.content.decode("utf-8"))
            logger.info(f"union_code content error {content}")
            return {"suc": True, "info": content}

    def get_unionpay_app_user_id(self, out_trade_no, user_auth_code, car_id, flag=False):
        params = {
            "version": "5.1.0",
            "encoding": "utf-8",
            "signMethod": "01",
            "txnType": "00",
            "txnSubType": "10",
            "bizType": "000000",
            "channelType": "08",
            "accessType": "0",
            "merId": self.get_merid(flag),
            "orderId": out_trade_no,
            "txnTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),  # moment().format('%Y%m%d%H%M%S')
            "userAuthCode": user_auth_code,  # 用户标识
            "appUpIdentifier": "UnionPay/1.0 CloudPay",  # 银联支付标识, 固定填写
            "termId": self.get_termid(car_id),
        }
        logger.info(f'union code get_app_user_id params:{params}')
        resp = self.unionpay_post(params, flag)
        logger.info(f'****** resp, {resp}, resp_type: {type(resp)}')
        order_id = resp.get('info').get('orderId')
        app_user_id = resp.get('info').get('app_user_id')
        if not resp:
            return {"suc": False, "info": ''}
        logger.info('***** union_code get_unionpay_app_user_id success')
        return {"suc": True, "order_id": order_id, "app_user_id": app_user_id}

    def pay(self, order_id, car_id, app_user_id, total_fee, attach, flag=False):
        key, cert_id = self.get_encrypt_cert(flag)  # 加密数据
        order_params = {
            "version": '5.1.0',
            "encoding": 'utf-8',
            "signMethod": '01',
            "txnType": '01',
            "txnSubType": '01',
            "bizType": '000000',
            "channelType": '08',
            "tradeType": 'mobileWeb',
            "accessType": '0',
            "currencyCode": '156',
            "merId": self.get_merid(flag),
            "orderId": order_id,
            "appUserId": app_user_id,
            "txnTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),  # moment().format('%Y%m%d%H%M%S')
            "txnAmt": total_fee,  # 交易金额, 分
            "encryptCertId": cert_id,
            "termId": self.get_termid(car_id),
            "reqReserved": attach,
            "backUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("notify_url_head") + "/ridingCard/unionpayCodeNotify",
            "frontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("notify_url_head") + '/queryOrder',  # queryOrder
            "qrCodeType": '0'
        }

        resp = self.unionpay_post(order_params, flag)
        parse_resp = self.parse_string(resp)
        if not (parse_resp or parse_resp.get('respCode') != '00' or parse_resp.get("payUrl")):
            logger.info("")
            return {}
        return {"suc": True, "data": {"suc": True, "tn": parse_resp.get('tn'), "payUrl": parse_resp.get('payUrl')}}

    def validate(self, notify):
        sign_method = notify.get('signMethod')
        version = notify.get('version')
        signature = notify.get('signature')
        if not (sign_method or version or signature):
            logger.info('signMethod,version,signature must not null', notify)
            return False
        del notify['signature']
        prestr = self.create_link_string(notify)  # 签名字符串
        prestr = hashlib.sha256(prestr.encode('utf-8')).hexdigest()  # 签名摘要

        # todo let key = unionPayUtils.verifyAndGetVerifyKey(reqBody.signPubKeyCert);
        #     if (!key) {
        #       logger.info(`no cert was found by signPubKeyCert: ${reqBody.signPubKeyCert}`);
        #       return false;
        #     } else {
        #       signature = jsrsasign.b64tohex(signature);
        #       let sig = new jsrsasign.Signature({ alg: 'SHA256withRSA' });
        #       sig.init(key);
        #       sig.updateString(prestr);
        #       result = sig.verify(signature);
        #     }
        #     return result;

    # 解析返回域信息
    @staticmethod
    def get_string_kv(req_reserved):
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

    def notify(self, notify):
        resp_code = notify.get('respCode')
        req_reserved = notify.get("reqReserved")
        if resp_code != '00':
            logger.info('支付失败,respCode={resp_code}'.format(resp_code=resp_code))
            return {"suc": False, "info": "返回支付状态码错误"}

        validate_result = self.validate(notify)
        if not validate_result:
            logger.info('支付失败,respCode={resp_code},validateResult={validate_result}'.format(resp_code=resp_code, validate_result=validate_result))
            return {"suc": False, "info": "签名校验错误"}

        attach = self.get_string_kv(req_reserved)
        if not attach.get('objectId'):
            logger.info('notify')
            return {"suc": False, "info": "用户信息查询失败"}
        return {"suc": True, "info": attach}


    def refund(self, object_id, trade_no, refund_fee, total_fee, flag=False):
        """

        :param order_id: 退货的订单号
        :param trade_no: 原始的流水号
        :param txn_amt: 退款金额
        :param flag:
        :return:
        """
        order_id, trade_no, txn_amt = object_id, trade_no, refund_fee
        params = {
            "version": '5.1.0',
            "encoding": 'utf-8',
            "signMethod": '01',
            "txnType": '04',
            "txnSubType": '00',
            "bizType": '000000',
            "channelType": '07',
            "backUrl": cfg.get('UnionpayConfig', '').get('notify_url_head').replace("ebike", "anfu") + "/queryOrder/refund",  # todo: 退款使用异步，还是同步操作
            "accessType": '0',
            "merId": self.get_merid(flag),  #Todo, 在config中读取是否开启双证书, 是否使用另一套证书的标志位
            "txnTime": datetime.now().strftime("%Y%m%d%H%M%S"),
            "orderId": order_id,  # 退货的订单号
            "origQryId": trade_no,  # 原始的流水号
            "txnAmt": txn_amt,  # 退款金额
        }
        resp = self.unionpay_post(params, False)
        parse_resp = self.parse_string(resp)
        if not (parse_resp or parse_resp.get('respCode') != '00' or parse_resp.get("payUrl")):
            logger.info("")
            return False
        return parse_resp




