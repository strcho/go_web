import hashlib
import json
from datetime import datetime, timedelta
from mbutils import dao_session
import requests
import xmltodict

from service.payment import PayHelper
from mbutils import logger, cfg
from service.payment.pay_utils.sub_account import SubAccount

class UnionPayForWXLite():

    def __init__(self):
        self.notify_url_method = "unionpayNotify"
        self.unionpay_lite = cfg.get('Unionpay_WXlite', {})
        self.unionpay_lite_single = cfg.get('Unionpay_WXlite_Single', {})

    def pay(self, open_id, total_fee, body, notify_url, out_trade_no, nonce_str, attach, Unionpay_WXlite):
        time_start = datetime.now().strftime("%Y%m%d%H%M%S")
        time_expire = (datetime.now() + timedelta(hours=2)).strftime("%Y%m%d%H%M%S")
        order_info = {
            "service": "pay.weixin.jspay",
            "version": Unionpay_WXlite.get('version'),
            "sign_type": Unionpay_WXlite.get("sign_type"),
            "mch_id": Unionpay_WXlite.get("mch_id"),
            "is_raw": 1,
            "is_minipg": 1,
            "out_trade_no": out_trade_no,  # 商户订单号
            "body": body,
            "sub_openid": open_id,  # 前端传入
            "sub_appid": Unionpay_WXlite.get("appId"),
            "total_fee": int(total_fee),  # 前端传入, 分, 该部分进行校验, 总金额
            "mch_create_ip": '127.0.0.1',  # 终端IP, 生成订单的机器Ip
            "notify_url": notify_url,  # 支付回调地址
            "nonce_str": nonce_str,  # 随机字符串
            "time_start": time_start,
            "time_expire": time_expire,
            "attach": attach
        }
        logger.info("unionpay to wxlite pay orderInfo: ", order_info)

        sign = self.create_sign(order_info, Unionpay_WXlite.get('key'))
        order_info['sign'] = sign
        param = {"xml": order_info}
        logger.info(f"UnionPayForWXLite order_info:{param}")
        xml = xmltodict.unparse(param)
        logger.info(f"UnionPayForWXLite xml:{xml}")
        response = requests.post(url=Unionpay_WXlite.get('url'), data=xml.encode('utf-8'),
                                 headers={'Content-Type': 'text/xml'})
        xmlmsg = xmltodict.parse(response.text)
        logger.info(f"UnionPayForWXLite pay xmlmsg:{xmlmsg}")
        if xmlmsg.get('xml').get("status") not in ('0', 0) or xmlmsg.get('xml').get("result_code") not in ('0', 0):
            return {"suc": False}
        try:
            get_params = json.loads(xmlmsg.get('xml').get("pay_info"))
        except Exception as e:
            return {"suc": False}
        logger.info(f"UnionPayForWXLite pay get_params:{get_params}")
        for k in list(get_params.keys()):
            if k not in ('appId', 'timeStamp', 'nonceStr', 'signType', 'package', 'paySign'):
                del get_params[k]
        #  return {"suc": True, "data": get_params} 多加参数是否有影响
        return {"suc": True, "data": get_params, "time_start": time_start, "time_expire": time_expire}

    @staticmethod
    def create_sign(orderInfo, key):
        stringA = ""
        for i in sorted(orderInfo):
            stringA += (i + "=" + str(orderInfo[i]) + "&") if i not in ('pfx', 'partner_key', 'sign', 'key') else ''
        string_sign_temp = stringA + 'key={}'.format(key)
        hash_md5 = hashlib.md5(string_sign_temp.encode('utf8'))
        sign = hash_md5.hexdigest().upper()
        return sign

    def get_all_union_secret(self) -> list:
        """ 获取所有分账的账户,用于验证账户"""
        result = [self.unionpay_lite.get("key")]
        # 某些服务区按照某个分法
        if self.unionpay_lite_single:
            result.append(self.unionpay_lite_single.get('key'))
        sub_accounts = self.unionpay_lite.get("sub_account", [])
        sub_accounts_1 = self.unionpay_lite_single.get("sub_account", [])
        if isinstance(sub_accounts, list) and isinstance(sub_accounts_1, list):
            sub_accounts.extend(sub_accounts_1)
            for sub_account in sub_accounts:
                if sub_account.get('key'):
                    result.append(sub_account.get('key'))
        return result

    def is_validated_sign(self, params):
        sign = params['sign']
        del params['sign']
        secret_key_list = self.get_all_union_secret()
        found = False
        idx = 0
        while idx < len(secret_key_list) and not found:
            if sign == self.create_sign(params, secret_key_list[idx]):
                found = True
            idx += 1
        return found

    def notify(self, xml):
        notify = xmltodict.parse(xml)
        logger.info(f"UnionPayForWXLite.notify notify:{notify}")
        notify = notify.get('xml')
        attach = notify.get('attach')
        result_code = notify.get("result_code")
        pay_result = notify.get("pay_result")
        s_code = (0, '0')

        # 1.校验返回回调结果中，根据status和result_code值来判定回调是否支付成功
        if notify.get("status") not in s_code or result_code not in s_code or pay_result not in s_code:
            return {"suc": False, "info": f"支付结果失败，result_code: {result_code}, pay_result: {pay_result}"}
        # 2.校验签名信息
        if not self.is_validated_sign(notify):
            return {"suc": False, "info": "签名失败. notify: {notify}".format(notify=notify)}
        # 3.解析返回回调的attach参数，校验附加信息,校验out_trade_no
        try:
            attach_json = json.loads(attach)
        except Exception as e:
            logger.error('completePurchaseRidingCardOrder JSON.parse(passback_params) error: {e}'.format(e=e))
            return {"suc": False,
                    "info": "completePurchaseRidingCardOrder JSON.parse(passback_params) error: {e}".format(e=e)}
        logger.info("attach_json: {attach_json}".format(attach_json=attach_json))
        return {"suc": True, "notify": notify, "attach_json": attach_json}

    def refund_unionpay_order(self, out_trade_no, transaction_id, out_refund_no, total_fee, refund_fee, union_pay_wx_lite):
        """
        @param out_trade_no: 交易单号
        @param transaction_id:
        @param out_refund_no: 退款单号
        @param total_fee: 订单总金额
        @param refund_fee: 退款总金额
        @param union_pay_wx_lite: union pay wx lite 配置（version， sign_type， mch_id， appId， key, url）
        @return:
        """
        refund_info = {
            "service": 'unified.trade.refund',
            "version": union_pay_wx_lite.get("version", ""),
            "sign_type": union_pay_wx_lite.get("sign_type", ""),
            "mch_id": union_pay_wx_lite.get("mch_id", ""),
            "transaction_id": transaction_id,
            "out_refund_no": out_refund_no,  # 返回的退款单号
            "total_fee": total_fee,  # 订单总金额
            "refund_fee": refund_fee,  # 退款总金额
            "op_user_id": union_pay_wx_lite.get("appId", ""),
            "nonce_str": PayHelper.rand_str_32(),  # 随机字符串
        }
        sign = self.create_sign(refund_info, union_pay_wx_lite.get("key", ""))
        refund_info["sign"] = sign
        param = {"rootName": refund_info}
        xml = xmltodict.unparse(param)
        logger.info(f"refund_unionpay_order data_xml:{xml}")
        response = requests.post(url=union_pay_wx_lite.get("url", ""), data=xml.encode('utf-8'),
                                 headers={'Content-Type': 'text/xml'})
        xmlmsg = xmltodict.parse(response.text)
        logger.info(f"refund_unionpay_order xmlmsg:{xmlmsg}")
        if xmlmsg.get('xml').get("status") not in (0, '0') or xmlmsg.get('xml').get("result_code") not in (0, '0'):
            return {"suc": False, "info": "退款失败"}
        try:
            out_transaction_id = xmlmsg.get('xml').get("out_transaction_id")  # 验签通过
            logger.info(f"refund_unionpay_order out_transaction_id:{out_transaction_id}")
            return {"suc": True, "info": "退款成功"}
        except Exception as e:
            logger.info(f"refund_unionpay_order e:{e}")
            return {"suc": False, "info": "退款失败"}

    # 分账退款
    def refund(self, object_id, trade_no, refund_fee, total_fee):
        """
        * 多支付号,使用多次尝试退款操作
        :param trade_no: 交易单号
        :param transaction_id:
        :param out_refund_no: 退款单号
        :param total_fee: 订单总金额
        :param refund_fee: 退款总金额
        :param union_pay_wx_lite: 退款分账配置
        :return:
        """
        out_refund_no = PayHelper.rand_str_32()
        transaction_id = trade_no
        out_trade_no = ''  # let out_trade_no = flowRecord.out_trade_no || ''

        # 分账配置获取
        union_pay_wx_lite = SubAccount("wallet").get_refund_config()
        logger.info(f"wallet 退款分账配置, config:{union_pay_wx_lite}")

        found, idx, result = False, 0, None
        # 分账退款一个账户一个账户的试验退款
        while idx < len(union_pay_wx_lite) and not found:
            result = self.refund_unionpay_order(out_trade_no, transaction_id, out_refund_no, total_fee, refund_fee, union_pay_wx_lite[idx])
            if result["suc"]:
                found = True
            idx += 1
        return result
