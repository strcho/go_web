import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta

from service.payment import PayHelper
import requests
import xmltodict

# from service.payment.pay_utils.payment_db_service import PayDBService
from service.payment import PayDBService
from mbutils import cfg, logger
from utils.constant.account import RIDING_CHANNEL_TYPE


class WePayService():

    def __init__(self):
        wepay_config = cfg.get("wepay", {})
        self.wx_wepaykey = wepay_config.get("key", "")
        self.wx_appID = wepay_config.get("appID", "")
        self.wx_mch_id = wepay_config.get("mch_id", "")
        self.wx_notify_url = wepay_config.get("notify_url_head", "").replace("ebike", "anfu")
        self.wx_notify_url_method = "/wepayNotify"
        self.wx_trade_type = "APP"

    def create_sign(self, order_info):
        string_a = ""
        for i in sorted(order_info):
            string_a += (i + "=" + str(order_info[i]) + "&") if i not in ('pfx', 'partner_key', 'sign', 'key') else ''
        string_sign_temp = string_a + 'key={}'.format(self.wx_wepaykey)
        hash_md5 = hashlib.md5(string_sign_temp.encode('utf8'))
        sign = hash_md5.hexdigest().upper()
        return sign

    """输入全部支付要素，返回用于前端的签名"""

    def wx_pay(self, body, attach, total_fee, nonce_str, out_trade_no, url, channel: int, openid=None):
        """
        :param STR body: 商品简单描述
        :param STR attach: 附加数据，在查询API和支付通知中原样返回，可作为自定义参数使用
        :param INT total_fee: 订单总金额，单位为分
        :param STR nonce_str: 随机字符串，长度要求在32位以内
        :param STR out_trade_no: 商户系统内部订单号，要求32个字符内，只能是数字、大小写字母_-|* 且在同一个商户号下唯一
        :param STR url: 支付回调表示那个商品部分的url example："/ridingCard"
        :param INT channel: 支付渠道 example: wxlite渠道为3
        :param STR openid: trade_type=JSAPI时（即JSAPI支付,wxlite渠道支付）此参数必传，此参数为微信用户在商户对应appid下的唯一标识
        :return:
        """
        """
        2021/07/26 
        由于测试时发现wx用oldwepay的key创建订单时加密会失败
        咨询过杨东，oldwepay时两年前的内容，现在可以暂时不考虑
        """
        appid = self.wx_appID
        mch_id = self.wx_mch_id
        notify_url = self.wx_notify_url
        notify_url_method = self.wx_notify_url_method
        trade_type = self.wx_trade_type
        time_start = datetime.now().strftime("%Y%m%d%H%M%S")
        time_expire = (datetime.now() + timedelta(hours=2)).strftime("%Y%m%d%H%M%S")
        order_info = {
            "appid": appid,
            "mch_id": mch_id,
            "nonce_str": nonce_str,
            "body": body,  # 不同途径构建不同body信息， exp:{mchName}-购买骑行卡"
            "attach": attach,
            "out_trade_no": out_trade_no,
            "total_fee": total_fee,
            "notify_url": notify_url + url + notify_url_method,
            "spbill_create_ip": "127.0.0.1",
            "time_start": time_start,
            "time_expire": time_expire,
            "trade_type": trade_type
        }
        if openid:
            order_info['openid'] = openid
        logger.info(f"{channel} pay orderinfo: {order_info}")
        sign = self.create_sign(order_info)
        order_info['sign'] = sign
        param = {'root': order_info}
        xml = xmltodict.unparse(param)
        logger.info(f"{channel} pay param: {param}, xml: {xml}")
        response = requests.post(url="https://api.mch.weixin.qq.com/pay/unifiedorder", data=xml.encode('utf-8'),
                                 headers={'Content-Type': 'text/xml'})
        xml_msg = xmltodict.parse(response.text)
        logger.info(f'{channel} Service.js resp: {xml_msg}')

        if "xml" not in xml_msg:
            return {}
        if "return_code" not in xml_msg.get('xml'):
            return {}
        prepay_id = xml_msg.get('xml', {}).get('prepay_id', "")
        if not prepay_id:
            logger.info(f'WxliteService createOrder no prepayId returned, check the resp. xml: {xml_msg}')
            return {}

        if channel == RIDING_CHANNEL_TYPE.WXLITE.value:
            data = self.create_lite_pay_params(prepay_id, appid)
        else:  # wepay
            data = self.create_pay_params(prepay_id, appid, mch_id)
        logger.info(f"pay_data: {data}, prepay_id: {prepay_id}")
        return {"pay_data": data, "prepay_id": prepay_id, "time_start": time_start, "time_expire": time_expire}

    def create_pay_params(self, prepay_id, appid, mch_id):
        data = {
            "appid": appid,
            "partnerid": mch_id,  # mch_id
            "prepayid": prepay_id,
            "package": "Sign=WXPay",
            "noncestr": PayDBService().create_noncestr_number(),
            "timestamp": str(int(datetime.now().timestamp())),
        }
        pay_sign = self.create_sign(data)
        data["paySign"] = pay_sign  # 加入签名
        return data

    def create_lite_pay_params(self, prepay_id, appid):
        data = {
            "appId": appid,
            "package": "prepay_id={}".format(prepay_id),
            "signType": "MD5",
            "nonceStr": PayDBService().create_noncestr_number(),
            "timeStamp": int(datetime.now().timestamp()),
        }
        pay_sign = self.create_sign(data)
        data["paySign"] = pay_sign  # 加入签名
        return data

    def is_validated_sign(self, params):
        sign = params['sign']
        del params['sign']
        return sign == self.create_sign(params)

    """微信/小程序 支付回调公共部分"""

    def valida_sign(self, xml):
        notify = xmltodict.parse(xml)
        notify = notify.get('xml')
        if not self.is_validated_sign(notify):
            return {"suc": False}
        result_code = notify.get('result_code')
        err_code = notify.get('err_code')
        err_code_des = notify.get('err_code_des')
        attach = notify.get('attach')
        if result_code != "SUCCESS":
            logger.info("参数错误",
                        {"suc": False, "info": "支付结果失败，err_code: {} err_code_des: {}".format(err_code, err_code_des)})
            return {"suc": False}
        try:
            attach_json = json.loads(attach)
        except Exception as e:
            logger.error(f'completePurchaseRidingCardOrder JSON.parse(passback_params) error: {e}, attach: {attach}')
            return {"suc": False}
        logger.info(f"notify: {notify}, attachJSON: {attach_json}")
        return {"suc": True, "notify": notify, "attach_json": attach_json}

    def wx_notify(self, xml):
        """
        :param xml:
        :return:
        """
        return self.valida_sign(xml)

    """微信退款公共部分"""

    def refund(self, object_id, trade_no, refund_fee, total_fee):
        """
        :param info: (appid, mch_id, total_fee, nonce_str, out_trade_no, out_refund_no, refund_desc, transaction_id)
        :param STR appid 微信支付分配的公众账号ID
        :param STR mch_id 微信支付分配的商户号
        :param INT total_fee 订单总金额，单位为分
        :param STR nonce_str 随机字符串，长度要求在32位以内
        :param STR out_trade_no 商户系统内部订单号，要求32个字符内，只能是数字、大小写字母_-|* 且在同一个商户号下唯一
        :param STR out_refund_no 商户系统内部的退款单号，商户系统内部唯一，只能是数字、大小写字母_-|*@ ，同一退款单号多次请求只退一笔。
        :param STR refund_desc 若商户传入，会在下发给用户的退款消息中体现退款原因
        :param STR transaction_id 微信生成的订单号，在支付通知中有返回
        :return:
        """
        nonce_str = PayHelper.rand_str_32()
        out_refund_no = PayHelper.rand_str_40()
        refund_desc = "退款"
        transaction_id = trade_no
        appid = self.wx_appID
        mch_id = self.wx_mch_id

        refund_info = {
            "appid": appid,
            "mch_id": mch_id,
            "nonce_str": nonce_str,
            "out_refund_no": out_refund_no,
            "total_fee": int(total_fee),
            "refund_fee": int(refund_fee),
            "refund_desc": refund_desc
        }
        if transaction_id:
            refund_info['transaction_id'] = transaction_id
        refund_info['sign'] = self.create_sign(refund_info)
        param = {'root': refund_info}
        xml = xmltodict.unparse(param)
        url = "https://api.mch.weixin.qq.com/secapi/pay/refund"

        # 需要将apiclient_cert.p12 转为pem格式， 默认将pem保存在xcmieba文件夹下
        if not os.path.exists("./apiclient_cert.pem"):
            logger.info('apiclient_cert.pem not exists')
            shell = os.system(
                "openssl pkcs12 -in ./cert/apiclient_cert.p12 -out apiclient_cert.pem -nodes -password pass:{}".format(
                    mch_id))
            logger.info(f'sehll: {shell}, pem:{os.path.exists("./apiclient_cert.pem")}')
        try:
            response = requests.post(url, data=xml.encode('utf-8'), cert="./apiclient_cert.pem")
            logger.info(f'wx refund response: {response}')
            resp = xmltodict.parse(response.text)
            logger.info('wepayService.js/refundOrder: refundInfo: {}, resp: {}'.format(
                refund_info, resp))
            resp_info = resp.get('xml')
            if resp_info.get('return_code') == "SUCCESS" and resp_info.get('result_code') == "SUCCESS":
                logger.info(f'refund success, resp_info:{resp_info}')
                return {"suc": True, "info": resp_info}
            else:
                logger.info('refund err: {}'.format(resp))
                return {"suc": False, "info": {"return_msg": resp_info.get('return_msg'),
                                               "err_code": resp_info.get('err_code'),
                                               "err_code_des": resp_info.get('err_code_des')},
                        "refundInfo": refund_info}
        except Exception as ex:
            logger.info("微信支付解析错误:", ex)
            return {"suc": False, "info": "解析错误"}
