import datetime
import json
import time
from datetime import timedelta

from service.payment.pay_utils.wxlitepay import WeLiteService
from mbutils import MbException
from model.all_model import *
from service.config import ConfigService
from service.kafka import KafkaRetry, PayKey
from service.kafka.producer import kafka_client
from service.payment.payment_interface import BusinessFullPayInterface, NotifyMixing
from service.payment import PayNotifyNxLock, PayDBService
from service.payment import UserDBService, PayHelper
from service.payment.pay_utils.alilitepay import AliLiteService
from service.payment.pay_utils.alipay import AliService
from service.payment.pay_utils.pay_constant import *
from service.payment.pay_utils.sub_account import SubAccount
from service.payment.pay_utils.unionpay_app import UnionPayForApp
from service.payment.pay_utils.unionpay_code import UnionPayForCode
from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
from service.payment.pay_utils.wepay import WePayService
from mbutils import dao_session, logger
from utils.constant.account import SERIAL_TYPE
from utils.constant.config import ConfigName
from utils.constant.redis_key import USER_DEPOSIT_CARD_EXPIRED
from utils.constant.user import UserState, DepositedState
from utils.constant.account import RIDING_CHANNEL_TYPE


class DepositService():

    def deposit_verify(self, object_id):
        rd_db_service = PayDBService(object_id)
        user_res = rd_db_service.get_user()
        if not user_res:
            return {"suc": False, "info": '用户不存在'}
        user_state = UserDBService.get_user_state(object_id)
        if isinstance(user_state, str):
            user_state = int(user_state)
        if user_state == UserState.SIGN_UP.value:
            return {"suc": False, "info": '请先实名认证'}
        if user_state == UserState.READY.value:
            return {"suc": False, "info": '您已缴纳诚信金'}
        if user_state != UserState.AUTHED.value:
            return {"suc": False, "info": '当前状态，购买不了诚信金！'}
        total_fee = ConfigService().get_router_content(ConfigName.DEPOSIT.value, user_res.serviceId)
        if not total_fee:
            logger.info(f'获取押金金额配置失败, object_id: {object_id}')
            return {"suc": False, "info": '获取押金金额配置失败'}
        return {"suc": True, "info": {"total_fee": total_fee, "serviceId": user_res.serviceId}}

    @staticmethod
    def deposit_order(channel: int, object_id, total_fee: int, out_trade_no, url: str, open_id=None):
        if RIDING_CHANNEL_TYPE.WXLITE.value == channel:
            config = PAY_CONFIG.WXLITE_CFG
            service_cls = WeLiteService
        else:
            config = PAY_CONFIG.WEPAY_CFG
            service_cls = WePayService

        mch_prefix, mch_name = config.get("depositSubject", "诚信金支付"), config.get("mchName", "")
        body = "{}-{}".format(mch_prefix, mch_name)
        attach = json.dumps({"objectId": object_id})
        nonce_str = PayHelper.rand_str_32()
        logger.info(f"deposit_order data, body:{body}, attach:{attach}, total_fee:{total_fee}, nonce_str:{nonce_str}, "
                    f"out_trade_no:{out_trade_no}, url:{url}, channel:{channel}, open_id:{open_id}")
        pay_params = service_cls().wx_pay(body, attach, total_fee, nonce_str, out_trade_no, url, channel, open_id)
        if not pay_params:
            return {"suc": False, "info": "发起支付失败"}
        return pay_params, nonce_str

    @staticmethod
    def deposit_record(object_id, out_trade_no, trade_no, time_end, channel, total_fee=0):
        """
        """
        wallet_dict = {
            "object_id": object_id,
            "trade_no": out_trade_no,
            "transaction_id": trade_no,
            "time_end": time_end,
            "channel": channel,
            "total_fee": total_fee
        }
        logger.info(f'union_app deposit_record send is {wallet_dict}')
        return kafka_client.pay_send(wallet_dict, key=PayKey.DEPOSIT.value)

    @staticmethod
    def handle_deposit(data_dict):
        object_id = data_dict.get("object_id", "")
        out_trade_no = data_dict.get("trade_no", "")
        transaction_id = data_dict.get("transaction_id", "")
        time_end = data_dict.get("time_end", "")
        channel = data_dict.get('channel', '')
        total_fee = data_dict.get('total_fee', '')
        try:
            rd_db_service = PayDBService(object_id)
            is_account = rd_db_service.is_account_by_trade(transaction_id)
            check = PayNotifyNxLock.check_trade_no(PayKey.DEPOSIT.value, out_trade_no)
            if not check["suc"]:
                logger.info("重复回调锁")
                return False
            if is_account:
                logger.info("重复回调")
                return False
            deposit_info = DepositService().deposit_verify(object_id)
            if not deposit_info["suc"]:
                logger.info(f"deposit info is error, deposit_info:{deposit_info}")
                return False
            user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
            # 如果用户有未支付订单,购买押金后,用户状态为待支付

            xc_a = XcEbikeAccount2()
            xc_a.objectId = object_id
            xc_a.amount = total_fee
            xc_a.type = SERIAL_TYPE.DEPOSIT.value
            xc_a.channel = channel
            xc_a.trade_no = transaction_id
            xc_a.paid_at = datetime.now()
            xc_a.createdAt = datetime.now()
            xc_a.updatedAt = datetime.now()
            dao_session.session().add(xc_a)
            dao_session.session().flush()

            order_paid_info = {}
            order_paid_info['transaction_id'] = transaction_id
            order_paid_info['gmt_payment'] = time_end
            order_paid_info['time_expire'] = time_end
            order_paid_info['time_start'] = time_end
            order_paid_info['channel'] = channel
            order_paid_info['out_trade_no'] = out_trade_no
            order_paid_info['total_fee'] = total_fee
            user.deposited = 1
            user.depositedInfo = json.dumps(order_paid_info)
            user.depositedMount = total_fee

            dao_session.session().commit()
            # redis操作放在db操作之后
            u = UserDBService()  # 更新用户信息
            user_state = u.get_user_state(object_id)
            if isinstance(user_state, str):
                user_state = int(user_state)
            logger.info(f"handle_deposit, user_state:{user_state}")
            if user_state in (UserState.RIDING.value, UserState.LEAVING.value):
                logger.info(f"callback after user riding: {object_id}, user_state: {user_state}")
            else:
                UserDBService().set_user_state(object_id, user_state, UserState.READY.value)
            is_unpaid_order = PayDBService().exists_unpaid_order(object_id)
            if is_unpaid_order:
                if user_state in (UserState.RIDING.value, UserState.LEAVING.value):  # 如果用户有未支付订单,购买押金后,用户状态为待支付
                    logger.info(f"call back user riding user_id: {object_id}, user_state: {user_state}")
                else:
                    u.set_user_state(object_id, user_state, UserState.TO_PAY.value)

            # 将会员卡过期集合中的用户删除
            dao_session.redis_session.r.srem(USER_DEPOSIT_CARD_EXPIRED, object_id)
            logger.info("更新用户的押金信息成功")
            return True
        except Exception as e:
            logger.info(f"更新用户的押金信息失败, err: {e}")
            dao_session.session().rollback()
            dao_session.session().close()
            PayNotifyNxLock().del_pay_notify_lock(PayKey.DEPOSIT.value, out_trade_no)
            raise KafkaRetry()

    @staticmethod
    def refund_verify(object_id):
        user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
        user_state = UserDBService.get_user_state(object_id)
        if isinstance(user_state, str):
            user_state = int(user_state)
        if not (user_state or user):
            return {"suc": False, "info": "获取当前用户信息失败,无法进行退款操作"}
        if user_state == UserState.TO_PAY.value:
            return {'suc': False, "info": f"当前用户有未支付订单不能退押金 objectId: {object_id}, user_state: {user_state}"}
        if user_state != UserState.READY.value or user.deposited != DepositedState.DEPOSITED.value:
            return {"suc": False, "info": f"当前状态下不能退押金 objectId: {object_id}, user_state: {user_state}"}
        return {"suc": True, "info": "已判断"}

    @staticmethod
    def refund_modify_db(object_id, refund_fee, trade_no, channel):
        try:
            one = dao_session.session().query(XcEbikeUsrs2).filter_by(
                id=object_id).first()
            deposited_info = json.loads(one.depositedInfo)
        except Exception as e:
            logger.info(f"订单记录异常，暂时无法退还押金。请联系客服处理 objectId: {object_id}, error: {e}")
            return False

        try:
            deposited_info['refund_fee'] = int(refund_fee)
            deposited_info['gmt_refund_pay'] = int(datetime.now().timestamp())
            # 更新退款后user信息
            one.deposited = DepositedState.NO_DEPOSITED.value
            one.depositedInfo = json.dumps({})
            one.depositedMount = int(deposited_info['total_fee']) - deposited_info['refund_fee']
            one.refundInfo = json.dumps(deposited_info)
            dao_session.session().add(one)

            order_info = {
                "objectId": object_id,
                "amount": int(deposited_info.get('refund_fee')) * -1,
                "type": SERIAL_TYPE.UNDEPOSIT.value,
                "channel": channel,
                "trade_no": deposited_info.get('transaction_id'),
                "paid_at": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now()
            }
            # 插入流水表
            xc_a = XcEbikeAccount2(**order_info)
            dao_session.session().add(xc_a)

            dao_session.session().commit()
            # redis操作必须放在mysql操作之后,保证一致性质
            user_state = UserDBService.get_user_state(object_id)
            UserDBService().set_user_state(object_id, user_state, UserState.AUTHED.value)

        except Exception as e:
            dao_session.session().rollback()
            dao_session.session().close()
            logger.info(f"refundDeposit JSON.parse(user.personInfo), user: {one.phone}, err: {e}")
            return False
        return True


class DepositCreateService(BusinessFullPayInterface):
    def __init__(self, info):
        super().__init__()
        self.channel, self.object_id, self.open_id, self.user_auth_code, self.car_id, self.frount_url, self.single_split, self.buyer_id = info
        self.subject = '购买诚信金'
        self.url = "/deposit"

    def wx_pay(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify["suc"]:
            raise MbException(deposit_verify["info"])
        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        out_trade_no = PayHelper.rand_str_24()
        deposit_order, nonce_str = d.deposit_order(RIDING_CHANNEL_TYPE.WEPAY.value, self.object_id, total_fee,
                                                   out_trade_no, self.url)
        return deposit_order.get('pay_data')

    def wx_lite(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify["info"])
        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        out_trade_no = PayHelper.rand_str_24()
        deposit_order, nonce_str = d.deposit_order(RIDING_CHANNEL_TYPE.WXLITE.value, self.object_id,
                                                   total_fee, out_trade_no, self.url, self.open_id)
        pre_value = {
            "object_id": self.object_id,
            "out_trade_no": nonce_str,
            "total_fee": total_fee,
            "trade_no": out_trade_no,
            "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        WeLiteService().set_info(pay_type="deposit", value={json.dumps(pre_value): str(time.time()).split('.')[0]})
        return deposit_order.get('pay_data')

    def ali_pay(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify["info"])
        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        out_trade_no = PayHelper.rand_str_24()
        # deposit_set = ConfigService().get_router_content("depositSet", service_id)
        biz_content = {
            "subject": self.subject,
            "out_trade_no": out_trade_no,
            "timeout_express": '2h',
            "product_code": 'QUICK_MSECURITY_PAY',
            "passback_params": {"objectId": self.object_id},
            "total_amount": round(float(total_fee) / 100, 2)
        }
        sign_status = AliService().sign(self.url, biz_content)
        logger.info("alipay buy favorable card, {},{},{}".format(self.object_id, RIDING_CHANNEL_TYPE.ALIPAY.value,
                                                                 total_fee))
        return sign_status

    def ali_lite(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify["info"])
        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        out_trade_no = PayHelper.rand_str_32()
        passback_params = {"objectId": self.object_id}
        pay = AliLiteService().pay(self.buyer_id, out_trade_no, total_fee, self.url, self.subject, passback_params)
        if not pay:
            raise MbException("订单生成失败")
        return pay

    def union_pay_app(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify["info"])
        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        attach = json.dumps({'objectId': self.object_id})
        pay_params = UnionPayForApp().pay(total_fee, attach, PayHelper.rand_str_24(), self.url)
        if not pay_params:
            raise MbException("订单生成失败")
        return pay_params

    def union_pay_lite(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify)

        total_fee = deposit_verify.get("info", {}).get('total_fee', 0)
        service_id = deposit_verify.get('serviceId')
        union_pay_wx_lite = SubAccount("deposite", service_id, self.single_split).get_payment_config()
        body = "{}-押金支付".format(union_pay_wx_lite.get('mchName'))
        notify_url = "{}/deposit/unionpayNotify".format(
            union_pay_wx_lite.get("notify_url_head").replace("ebike", "anfu"))
        out_trade_no = PayHelper.rand_str_24()
        attach = json.dumps({"objectId": self.object_id})
        pay_params = UnionPayForWXLite().pay(self.open_id, total_fee, body, notify_url, out_trade_no,
                                             PayHelper.rand_str_32(), attach, union_pay_wx_lite)
        if not pay_params["suc"]:
            raise MbException("订单生成失败")
        return pay_params["data"]

    def union_pay_code(self):
        d = DepositService()
        deposit_verify = d.deposit_verify(self.object_id)
        if not deposit_verify['suc']:
            raise MbException(deposit_verify)
        total_fee = deposit_verify.get('total_fee')
        deposited_info = json.loads(deposit_verify.get("deposited_info"))
        time_expire = deposit_verify.get('time_expire')

        uc = UnionPayForCode()
        get_union_user_id = uc.get_unionpay_app_user_id(PayHelper.rand_str_24(), self.user_auth_code, self.car_id)
        if not get_union_user_id.get('suc'):
            raise MbException("获取用户认证失败")
        order_id = get_union_user_id.get('order_id')
        app_user_id = get_union_user_id.get('app_user_id')

        attach = json.dumps({
            "objectId": self.object_id,
            "orderId": order_id,
            "backFrontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("normal_front_url")
        })
        pay_params = UnionPayForCode().pay(order_id, self.car_id, app_user_id, total_fee, attach)
        if not pay_params:
            raise MbException("订单生成失败")

        order_paid_info = {
            "channel": RIDING_CHANNEL_TYPE.UNIONPAY_CODE.value,
            "out_trade_no": order_id,
            "transaction_id": '',
            "total_fee": total_fee,
            "time_start": datetime.now().strftime("%Y%m%d%H%M%S"),
            "time_expire": (datetime.now() + timedelta(hours=2)).strftime("%Y%m%d%H%M%S")
        }
        if not deposited_info or deposited_info.get('gmt_payment') or deposited_info.get(
                'channel') != RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value or time_expire < datetime.now().strftime(
            "%Y%m%d%H%M%S"):
            dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == self.object_id).update(
                {"depositedInfo": json.dumps(order_paid_info), "depositedMount": 0})
            dao_session.session().commit()

        return pay_params


class DepositNotifyService(BusinessFullPayInterface, NotifyMixing):
    def __init__(self, channel):
        super().__init__()
        self.subject = '购买诚信金'
        self.url = "/deposit"
        self.channel = channel

    def wx_pay(self, xml) -> str:
        wx_notify = self.get_notify_func()(xml)
        if not wx_notify['suc']:
            return WX_FAILED_XML
        attach_json = wx_notify.get("attach_json")
        notify = wx_notify.get("notify")
        if not attach_json.get('objectId'):
            return WX_FAILED_XML
        object_id = attach_json.get('objectId')
        time_end = notify.get('time_end')
        transaction_id = notify.get('transaction_id')
        out_trade_no = notify.get('out_trade_no')
        total_fee = notify.get('total_fee')
        res = DepositService.deposit_record(object_id, out_trade_no, transaction_id, time_end, self.channel, total_fee)
        if not res:
            return WX_FAILED_XML
        pre_value = {
            "object_id": object_id,
            "out_trade_no": out_trade_no,
            "total_fee": total_fee,
            "trade_no": transaction_id,
            "channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        if self.channel == RIDING_CHANNEL_TYPE.WXLITE.value:
            WeLiteService.del_info("deposit", json.dumps(pre_value))
        return WX_SUCCESS_XML

    def ali_pay(self, notify):
        total_amount = notify.get('total_amount')
        trade_no = notify.get('trade_no')
        out_trade_no = notify.get('out_trade_no')
        gmt_payment = notify.get('gmt_payment')
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        amount = int(float(total_amount) * 100)
        res = DepositService.deposit_record(object_id, out_trade_no, trade_no, gmt_payment, self.channel, amount)
        if not res:
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def wx_lite(self, xml):
        return self.wx_pay(xml)

    def ali_lite(self, notify) -> dict:
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return ALI_FAILED_RESP
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        out_trade_no = notify.get('out_trade_no')
        total_amount = int(float(notify.get('total_amount')) * 100)
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        if not object_id:
            return ALI_FAILED_RESP
        res = DepositService.deposit_record(object_id, out_trade_no, trade_no, gmt_payment, self.channel, total_amount)
        if not res:
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def union_pay_app(self, notify) -> dict:
        query_id = notify.get("queryId")
        logger.info(f'deposit union_app notify {notify}')
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return UNION_FAILED_RESP
        attach = decode_notify.get('info')
        object_id = attach.get('objectId')
        txn_time = notify.get('txnTime')
        out_trade_no = notify.get('orderId')
        txn_amt = notify.get('txnAmt')
        res = DepositService.deposit_record(object_id, out_trade_no, query_id, txn_time, self.channel, txn_amt)
        if not res:
            logger.error('押金信息写入失败')
            return UNION_FAILED_RESP
        return UNION_SUCCESS_RESP

    def union_pay_lite(self, xml):
        return self.wx_pay(xml)

    def union_pay_code(self, notify):
        return self.union_pay_app(notify)


class DepositRefundService(BusinessFullPayInterface):
    def __init__(self, object_id, trade_no, refund_fee, channel: int):
        self.object_id = object_id
        self.trade_no = trade_no
        self.refund_fee = int(refund_fee)
        self.total_fee = 0
        self.channel = channel

    def _check(self):
        """
        判断交易单号是否存在，退款金额是否大于购买金额，用户是否存在，交易类型是否一致，需要做参数校验
        @return:
        """
        refund_account = dao_session.session().query(XcEbikeAccount2) \
            .filter(XcEbikeAccount2.trade_no == self.trade_no,
                    XcEbikeAccount2.objectId == self.object_id,
                    XcEbikeAccount2.type == SERIAL_TYPE.UNDEPOSIT.value).first()
        if refund_account:
            return {"suc": False, "info": "已退款"}
        account = dao_session.session().query(XcEbikeAccount2) \
            .filter(XcEbikeAccount2.trade_no == self.trade_no,
                    XcEbikeAccount2.objectId == self.object_id,
                    XcEbikeAccount2.type == SERIAL_TYPE.DEPOSIT.value).first()
        if account and (account.channel != self.channel or account.amount < self.refund_fee):
            return {"suc": False, "info": "退款失败0"}
        else:
            self.total_fee = account.amount
            return {"suc": True, "info": ""}

    def deposit_refund(self, refund_func):
        logger.info("deposit is refund, ", refund_func)
        # 参数验证已在 get_deposit_refund_info 做了
        res = refund_func(self.object_id, self.trade_no, self.refund_fee, self.total_fee)
        if res["suc"]:
            is_refund = DepositService.refund_modify_db(self.object_id, self.refund_fee, self.trade_no, self.channel)
            if is_refund:
                return {"suc": True, "info": "退款成功"}
        return {"suc": False, "info": "退款失败"}

    def wx_pay(self):
        return self.deposit_refund(WePayService().refund)

    def wx_lite(self):
        return self.deposit_refund(WeLiteService().refund)

    def ali_pay(self):
        return self.deposit_refund(AliService().refund)

    def ali_lite(self):
        return self.deposit_refund(AliLiteService().refund)

    def union_pay_app(self):
        return self.deposit_refund(UnionPayForApp().refund)

    def union_pay_code(self):
        return self.deposit_refund(UnionPayForCode().refund)

    def union_pay_lite(self):
        return self.deposit_refund(UnionPayForWXLite().refund)
