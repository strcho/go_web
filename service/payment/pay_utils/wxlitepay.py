from service.payment import PayHelper
import requests
import xmltodict

# from service.payment.pay_utils.payment_db_service import PayDBService
from service.payment import PayDBService
from mbutils import cfg, logger, dao_session
from utils.constant.account import RIDING_CHANNEL_TYPE
from utils.constant.redis_key import WXLITE_PAYMENT
from .wepay import WePayService


class WeLiteService(WePayService):

    def __init__(self):
        super().__init__()
        config = cfg.get("wx_lite", {})
        self.wx_wepaykey = config.get("key", "")
        self.wx_appID = config.get("appid", "")
        self.wx_mch_id = config.get("mch_id", "")
        self.wx_notify_url = config.get("notify_url_head", "").replace("ebike", "anfu")
        self.wx_notify_url_method = "/wxliteNotify"
        self.wx_trade_type = config.get("trade_type", "")

    @staticmethod
    def set_info(pay_type: str, value: dict):
        """
        @param pay_type:  wallet, riding_card, favorable_card, deposit_card, deposit
        @param value: {"value": 1}
        @return:
        """
        return dao_session.redis_session.r.zadd(WXLITE_PAYMENT.format(pay_type=pay_type), value)

    @staticmethod
    def del_info(pay_type: str, value: str):
        return dao_session.redis_session.r.zrem(WXLITE_PAYMENT.format(pay_type=pay_type), value)

    @staticmethod
    def range_del_info(pay_type, max_time):
        return dao_session.redis_session.r.zremrangebyscore(
            WXLITE_PAYMENT.format(pay_type=pay_type), min=0, max=max_time)

    @staticmethod
    def range_sel_info(pay_type, min_time, max_time):
        return dao_session.redis_session.r.zrangebyscore(WXLITE_PAYMENT.format(pay_type=pay_type), min_time, max_time)

    def query_order(self, out_trade_no, app_id, mch_id):
        """
        out_trade_no: xitong
        """
        url = "https://api.mch.weixin.qq.com/pay/orderquery"
        order_info = {
            "appid": app_id,
            "mch_id": mch_id,
            "nonce_str": PayHelper.rand_str_32(),
            "out_trade_no": out_trade_no
        }
        sign = self.create_sign(order_info)
        order_info['sign'] = sign
        param = {'root': order_info}
        xml = xmltodict.unparse(param)
        response = requests.post(url=url, data=xml.encode('utf-8'),
                                 headers={'Content-Type': 'text/xml'})
        xml_msg = xmltodict.parse(response.text).get('xml', {})
        logger.info(f"WeLiteService.query_order xml_msg: {xml_msg}")
        if "SUCCESS" == xml_msg.get('return_code', 'FALSE') and "SUCCESS" == xml_msg.get('result_code', 'FALSE') and \
                "SUCCESS" == xml_msg.get('trade_state', 'FALSE'):
            return xml_msg.get('transaction_id', '')
        else:
            return ''
