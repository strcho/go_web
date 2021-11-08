import json
import time

from model.all_model import *
from service.kafka import KafkaRetry, PayKey
from service.kafka.producer import kafka_client
from service.payment.pay_utils.wxlitepay import WeLiteService
from service.payment.payment_interface import BusinessFullPayInterface, NotifyMixing
from service.payment import PayDBService, PayNotifyNxLock, UserDBService
from service.payment import PayHelper
from service.payment.pay_utils.alilitepay import AliLiteService
from service.payment.pay_utils.alipay import AliService
from service.payment.pay_utils.pay_constant import *
from service.payment.pay_utils.sub_account import SubAccount
from service.payment.pay_utils.unionpay_app import UnionPayForApp
from service.payment.pay_utils.unionpay_code import UnionPayForCode
from service.payment.pay_utils.unionpay_wxlite import UnionPayForWXLite
from service.payment.pay_utils.wepay import WePayService
from mbutils import cfg, MbException
from mbutils import dao_session, logger
from utils.constant.account import RIDING_CHANNEL_TYPE, DEPOSIT_CONFIG_TYPE, DEPOSIT_CHANNEL_TYPE
from utils.constant.account import SERIAL_TYPE
from utils.constant.redis_key import ALI_TRADE_INFO, REFUND_RIDINGCARD_LOCK, REFUND_DEPOSITCARD_LOCK, USER_STATE
from utils.constant.redis_key import DEPOSIT_CARD_USER_ID, USER_DEPOSIT_CARD_EXPIRED
from utils.constant.user import UserState
from datetime import timedelta, datetime


# 会员卡支付重复部分，共用函数
class DepositCardService():
    DEPOSIT_CONFIG_TYPE = {
        "depositCard": 0,  # 押金卡配置
        "zhima": 1  # 芝麻信用免押金配置
    }

    def user_verify(self, object_id, deposit_card_id):
        rd_db_service = PayDBService(object_id)
        user_res = rd_db_service.get_user()
        if not user_res:
            return {"suc": False, "info": '用户不存在'}
        user_state = UserDBService.get_user_state(object_id)
        if isinstance(user_state, str):
            user_state = int(user_state)
        if user_state == UserState.SIGN_UP.value:
            return {"suc": False, "info": '请先实名认证'}
        deposit_config = rd_db_service.get_deposit_card_config(deposit_card_id)
        if not deposit_config:
            return {"suc": False, "info": '没有会员卡配置'}
        return {"suc": True, "info": deposit_config.content, "service_id": user_res.serviceId}

    @staticmethod
    def user_can_refund_deposit_card(object_id, trade_no, refund_fee):
        """
        校验用户能否进行会员卡退款
        :param object_id: 用户id
        :param trade_no: 订单号
        :param refund_fee: 退款金额
        :return:
        """
        refund_lock = dao_session.redis_session.r.set(REFUND_DEPOSITCARD_LOCK.format(object_id=object_id), trade_no,
                                                      nx=True, px=5000)
        if not refund_lock:
            return {"suc": False, "info": "会员卡退款中,请5s后重试"}
        refund_fee = int(refund_fee)
        if refund_fee <= 0:
            return {"suc": False, "info": "请输入正整数的退款金额"}

        user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
        user_state = UserDBService.get_user_state(object_id)
        if isinstance(user_state, str):
            user_state = int(user_state)
        if not (user_state or user):
            return {"suc": False, "info": "获取当前用户信息失败,无法进行退款操作"}
        if user_state == UserState.LEAVING.value or user_state == UserState.RIDING.value:
            return {"suc": False, "info": "用户正在使用中,请不要进行退款"}
        if user_state == UserState.TO_PAY.value:
            return {"suc": False, "info": "用户有未完结的订单,请用户完结后进行退款"}

        depositcard_record = dao_session.session().query(XcEbike2DepositCard).filter(
            XcEbike2DepositCard.trade_no == trade_no, XcEbike2DepositCard.objectId == object_id,
            XcEbike2DepositCard.money > 0).first()
        if not depositcard_record:
            return {"suc": False, "info": "没有会员卡的购买记录,请先进行核,trade_no: {trade_no}".format(trade_no=trade_no)}
        account_record = dao_session.session().query(XcEbikeAccount2).filter(
            XcEbikeAccount2.trade_no == trade_no).first()
        if not account_record:
            return {"suc": False, "info": "流水号: {trade_no},没有对应的购买记录数据,请先进行核实".format(trade_no=trade_no)}

        has_refund_record = account_record.refund_amount or 0
        if has_refund_record + refund_fee > account_record.amount:
            could_refund_amount = (account_record.amount - has_refund_record) / 100 if (account_record.
                                                                                        amount - has_refund_record) > 0 else 0
            return {"suc": False,
                    "info": "退款金额超出,当前可退款余额: {could_refund_amount}元".format(could_refund_amount=could_refund_amount)}

        if account_record.channel not in RIDING_CHANNEL_TYPE.get_all_apy_channel():
            return {'suc': False, "info": f"支付类型无效,channel: {account_record.channel}"}

        info = {"has_refund_record": has_refund_record, "account_record": account_record,
                "depositcard_record": depositcard_record}
        return {"suc": True, "info": info}

    @staticmethod
    def refund_modify_db(object_id, trade_no, refund_fee, depositcard_record, account_record):
        logger.info(f'depositcard_record :{depositcard_record}, account_record: {account_record}')
        try:
            refund_fee = int(refund_fee)
            deposit_card_info = {}
            deposit_card_info['configId'] = depositcard_record.configId
            deposit_card_info['days'] = depositcard_record.days
            deposit_card_info['expiredDate'] = depositcard_record.expiredDate
            deposit_card_info['content'] = depositcard_record.content
            deposit_card_info['agentId'] = depositcard_record.agentId
            deposit_card_info['serviceId'] = depositcard_record.serviceId
            deposit_card_info['objectId'] = object_id
            deposit_card_info['trade_no'] = ""
            deposit_card_info['money'] = refund_fee * -1
            deposit_card_info['channel'] = DEPOSIT_CHANNEL_TYPE.PLATFORM_REFUND.value
            deposit_card_info['type'] = RIDING_CHANNEL_TYPE.PLATFORM.value
            dc = XcEbike2DepositCard(**deposit_card_info)
            dao_session.session().add(dc)
            dao_session.session().flush()

            # XcEbikeAccount2
            order_info = {
                "objectId": object_id,
                "amount": refund_fee * -1,
                "type": SERIAL_TYPE.DEPOSIT_CARD_REFUND.value,
                "channel": account_record.channel,
                "trade_no": trade_no,  # refund返回 trade_no
                "orderId": dc.id,
                "paid_at": datetime.now()
            }
            # 插入流水表
            xc_a = XcEbikeAccount2(**order_info)
            dao_session.session().add(xc_a)

            refund_amount = account_record.refund_amount or 0 + refund_fee  # 累计退款金额
            dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no,
                                                                XcEbikeAccount2.objectId == object_id,
                                                                XcEbikeAccount2.amount > 0).update(
                {"refund_amount": refund_amount, "objectId": object_id, "trade_no": trade_no})
            dao_session.session().commit()
        except Exception as e:
            dao_session.session().rollback()
            dao_session.session().close()
            logger.info(f"depost card refund error: {e}")
            return False
        return True

    @staticmethod
    def deposit_card_record(object_id, deposit_card_id, amount, trade_no, gmt_create, account_channel, deposit_channel):
        """
        @param object_id: 用户id
        @param deposit_card_id: 会员卡id
        @param amount: 变化的金额
        @param trade_no: 交易流水单号
        @param gmt_create: 支付时间
        @param account_channel: 交易渠道(根据不同的回调，传不同的值)
        @param deposit_channel: 押金卡交易渠道(根据不同的回调，传不同的值)
        @return:
        """
        deposit_cardd_dict = {
            "object_id": object_id,
            "deposit_card_id": deposit_card_id,
            "amount": amount,
            "trade_no": trade_no,
            "gmt_create": gmt_create,
            "account_channel": account_channel,
            "deposit_channel": deposit_channel,
        }
        logger.info(f'deposit_card_record send is {deposit_cardd_dict}')
        return kafka_client.pay_send(deposit_cardd_dict, key=PayKey.DEPOSIT_CARD.value)

    @staticmethod
    def handle_deposit_card(data_dict):
        """ object_id, deposit_card_id, amount, trade_no, gmt_create, account_channel, channel """
        # TODO type和channel都少传了
        object_id = data_dict.get("object_id", "")
        deposit_card_id = data_dict.get("deposit_card_id", "")
        total_fee = data_dict.get("amount", "")
        account_channel = data_dict.get("account_channel", "")
        deposit_channel = data_dict.get("deposit_channel", "")
        trade_no = data_dict.get("trade_no", "")
        time_end = data_dict.get("gmt_create", "")
        deposit_card_type = SERIAL_TYPE.DEPOSIT_CARD.value
        try:
            check = PayNotifyNxLock.check_trade_no(PayKey.DEPOSIT_CARD.value, trade_no)
            if not check["suc"]:
                logger.info("重复回调锁")
                return False
            rd_db_service = PayDBService(object_id)
            deeposit_card = rd_db_service.is_deposit_card(trade_no)
            if deeposit_card:
                logger.info(f"重复的微信回调, trade_no:{trade_no}")
                return
            user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == object_id).first()
            if not user_info:
                logger.info("回调时用户信息获取失败，{}".format(object_id))
                return
            deposit_config = rd_db_service.get_deposit_card_config(deposit_card_id)
            if not deposit_config:
                logger.info("没有会员卡配置")
                return
            content = json.loads(deposit_config.content)
            agent_id = user_info.agentId
            days = content.get('cardDurationDays', 0)

            # 购买会员卡
            xc_dc = XcEbike2DepositCard()
            xc_dc.objectId = object_id
            xc_dc.configId = deposit_card_id
            xc_dc.type = DEPOSIT_CONFIG_TYPE.DEPOSIT_CARD.value
            xc_dc.channel = deposit_channel
            xc_dc.money = total_fee
            xc_dc.days = days
            xc_dc.trade_no = trade_no
            xc_dc.expiredDate = datetime.now()
            xc_dc.content = deposit_config.content
            xc_dc.agentId = agent_id or 2
            xc_dc.serviceId = user_info.serviceId or 1
            xc_dc.createdAt = datetime.now()
            xc_dc.updatedAt = datetime.now()
            dao_session.session().add(xc_dc)
            dao_session.session().flush()

            # 插入流水表
            xc_a = XcEbikeAccount2()
            xc_a.objectId = object_id
            xc_a.amount = total_fee
            xc_a.type = deposit_card_type
            xc_a.channel = account_channel
            xc_a.trade_no = trade_no
            xc_a.paid_at = datetime.now()
            xc_a.createdAt = datetime.now()
            xc_a.updatedAt = datetime.now()
            xc_a.orderId = xc_dc.id
            dao_session.session().add(xc_a)
            dao_session.session().flush()

            history_time = user_info.depositCardExpiredDate
            logger.info(f"handle_deposit_card history:{history_time}, {type(history_time)}. "
                        f"now:{datetime.now()}, {type(datetime.now())}")
            if user_info.haveDepositCard and history_time and history_time > datetime.now():
                user_info.deposited = 1
                user_info.depositCardExpiredDate = history_time + timedelta(days=days)
                # depositcard_ex_time = history_time.__sub__(datetime.now()) + days * 24 * 3600
                depositcard_ex_time = (history_time - datetime.now()).seconds + days * 24 * 3600
            else:
                user_info.deposited = 1
                user_info.haveDepositCard = 1
                user_info.depositCardExpiredDate = datetime.now() + timedelta(days=days)
                depositcard_ex_time = days * 24 * 3600
            logger.info(f"handle_deposit_card depositcard_ex_time: {depositcard_ex_time}")
            history_value = dao_session.redis_session.r.get(DEPOSIT_CARD_USER_ID.format(user_id=user_info.id))
            dao_session.redis_session.r.set(DEPOSIT_CARD_USER_ID.format(user_id=user_info.id), user_info.id,
                                            ex=depositcard_ex_time)
            logger.info("user state key is :", USER_STATE.format(user_id=object_id))
            dao_session.session().commit()
            # redis操作放在db操作之后

            user_state = UserDBService.get_user_state(object_id)
            if isinstance(user_state, str):
                user_state = int(user_state)
            logger.info(f"handle_deposit_card, user_state:{user_state}")
            user_order = rd_db_service.exists_unpaid_order(object_id)
            if user_state in (UserState.RIDING.value, UserState.LEAVING.value):
                logger.info(f"callback after user riding: {object_id}, user_state: {user_state}")
            else:
                UserDBService().set_user_state(object_id, user_state, UserState.READY.value)
            # 如果用户有未支付的订单，将用户状态置为待支付
            if user_order:
                if user_state in (UserState.RIDING.value, UserState.LEAVING.value):
                    logger.info(f"callback after user riding: {object_id}, user_state: {user_state}")
                else:
                    UserDBService().set_user_state(object_id, user_state, UserState.TO_PAY.value)

            # 将会员卡过期集合中的用户删除
            dao_session.redis_session.r.srem(USER_DEPOSIT_CARD_EXPIRED, object_id)
            logger.info(f"会员卡购买成功，object_id:{object_id}, trade_no:{trade_no}, desposit_card_id:{deposit_card_id}")
        except Exception as e:
            logger.error(f"会员卡购买失败 e:{e}")
            dao_session.session().rollback()
            dao_session.session().close()
            PayNotifyNxLock().del_pay_notify_lock(PayKey.DEPOSIT_CARD.value, trade_no)
            raise KafkaRetry()


class DepositCardCreateService(BusinessFullPayInterface):
    def __init__(self, info):
        super().__init__()
        self.object_id, self.amount, self.channel, self.deposit_card_id, self.open_id, self.user_auth_code, self.car_id, \
        self.front_url, self.buyer_id, self.single_split = info
        self.subject = "购买会员卡"
        self.url = '/depositCard'

    def _check(self):
        rd_db_service = PayDBService(self.object_id)
        user_res = rd_db_service.get_user()
        if not user_res:
            return {"suc": False, "info": '用户不存在'}
        user_state = UserDBService.get_user_state(self.object_id)
        if user_state == UserState.SIGN_UP.value:
            return {"suc": False, "info": '请先实名认证'}
        deposit_config = rd_db_service.get_deposit_card_config(self.deposit_card_id)
        if not deposit_config:
            return {"suc": False, "info": '没有会员卡配置'}
        return {"suc": True, "info": deposit_config.content}

    def wx_pay(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        total_fee = content.get("curMoney")
        attach = json.dumps({
            "objectId": self.object_id,
            "configId": self.deposit_card_id
        })
        subject = self.subject
        if cfg.get('wepay').get("body"):
            subject = cfg.get('wepay').get("body")

        pay_params = WePayService().wx_pay(subject, attach, total_fee, PayHelper.rand_str_32(), PayHelper.rand_str_24(),
                                           self.url, RIDING_CHANNEL_TYPE.WEPAY.value)
        if not pay_params:
            raise MbException('发起支付失败')
        return pay_params['pay_data']

    def wx_lite(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        total_fee = content.get("curMoney")
        attach = json.dumps({
            "objectId": self.object_id,
            "configId": self.deposit_card_id
        })
        subject = self.subject
        if cfg.get('wx_lite').get("body"):
            subject = cfg.get('wx_lite').get("body")
        trade_no = PayHelper.rand_str_24()
        pay_params = WeLiteService().wx_pay(subject, attach, total_fee, PayHelper.rand_str_32(),
                                            trade_no, self.url, RIDING_CHANNEL_TYPE.WXLITE.value, self.open_id)
        if not pay_params:
            raise MbException('发起支付失败')
        pre_value = {
            "object_id": self.object_id,
            "deposit_card_id": self.deposit_card_id,
            "amount": self.amount,
            "trade_no": trade_no,
            "account_channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "deposit_channel": DEPOSIT_CHANNEL_TYPE.WXLITE.value,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        WeLiteService().set_info(pay_type="deposit_card", value={json.dumps(pre_value): str(time.time()).split('.')[0]})
        return pay_params['pay_data']

    def ali_pay(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        passback_params = {
            "objectId": self.object_id,
            "type": DEPOSIT_CONFIG_TYPE.DEPOSIT_CARD.value,
            "configId": self.deposit_card_id,
            "serialType": SERIAL_TYPE.DEPOSIT_CARD.value
        }
        biz_content = {
            "subject": self.subject,
            "out_trade_no": PayHelper.rand_str_32(),
            "timeout_express": "2h",
            "product_code": "QUICK_MSECURITY_PAY",
            "passback_params": passback_params,
            "total_amount": round(float(self.amount) / 100, 2)
        }
        sign_status = AliService().sign(self.url, biz_content)
        logger.info(
            "alipay buy favorable card, {},{},{},{}, {}".format(
                self.object_id, RIDING_CHANNEL_TYPE.ALIPAY.value, self.amount, passback_params,
                json.dumps(passback_params)))
        return sign_status

    def ali_lite(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        logger.info(f'ali_lite notify deposit_card_id: {self.deposit_card_id}')
        passback_params = {
            "objectId": self.object_id,
            "type": DEPOSIT_CONFIG_TYPE.DEPOSIT_CARD.value,
            "configId": self.deposit_card_id,
            "serialType": SERIAL_TYPE.DEPOSIT_CARD.value
        }
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        total_fee = content.get("curMoney")
        out_trade_no = PayHelper.rand_str_24()
        pay = AliLiteService().pay(self.buyer_id, out_trade_no, total_fee, self.url, self.subject, passback_params)
        if not pay:
            raise MbException("订单生成失败")
        return pay

    def union_pay_app(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        total_fee = content.get("curMoney")
        attach = json.dumps({"objectId": self.object_id, "configId": self.deposit_card_id})
        pay_params = UnionPayForApp().pay(total_fee, attach, PayHelper.rand_str_24(), self.url)
        if not pay_params:
            raise MbException("订单生成失败")
        return pay_params

    def union_pay_code(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        total_fee = content.get("curMoney")

        uc = UnionPayForCode()
        out_trade_no = PayHelper.rand_str_24()
        get_union_user_id = uc.get_unionpay_app_user_id(out_trade_no, self.user_auth_code, self.car_id)
        if not get_union_user_id.get('suc'):
            raise MbException("获取用户认证失败")
        order_id = get_union_user_id.get('order_id')
        app_user_id = get_union_user_id.get('app_user_id')

        attach = json.dumps({
            "objectId": self.object_id,
            "out_trade_no": out_trade_no,
            "configId": self.deposit_card_id,
            "backFrontUrl": PAY_CONFIG.UNIONPAY_CONFIG.get("normal_front_url")
        })
        pay_params = UnionPayForCode().pay(order_id, self.car_id, app_user_id, total_fee, attach)
        if not pay_params:
            raise MbException("订单生成失败")
        return pay_params

    def union_pay_lite(self):
        user_verify = DepositCardService().user_verify(self.object_id, self.deposit_card_id)
        if not user_verify['suc']:
            raise MbException(user_verify['info'])
        content = json.loads(user_verify['info'])
        service_id = user_verify.get('service_id')
        total_fee = content.get("curMoney")
        out_trade_no = PayHelper.rand_str_24()

        Unionpay_WXlite = SubAccount("depositeCard", service_id, self.single_split).get_payment_config()
        body = self.subject
        if Unionpay_WXlite and Unionpay_WXlite.get('body'):
            body = Unionpay_WXlite.get('body')
        notify_url = "{}/depositCard/unionpayNotify".format(
            Unionpay_WXlite.get("notify_url_head").replace("ebike", "anfu"))

        attach = json.dumps({
            "objectId": self.object_id,
            "configId": self.deposit_card_id,
            "out_trade_no": out_trade_no
        })
        pay_params = UnionPayForWXLite().pay(self.open_id, total_fee, body, notify_url, out_trade_no,
                                             PayHelper.rand_str_32(), attach, Unionpay_WXlite)
        if not pay_params["suc"]:
            raise MbException("订单生成失败")
        return pay_params["data"]


# 会员卡支付回调
class DepositCardNotifyService(BusinessFullPayInterface, NotifyMixing):
    def __init__(self, channel):
        super().__init__()
        self.subject = "购买会员卡"
        self.url = '/depositCard'
        self.channel = channel

    def wx_pay(self, xml, deposit_channel=DEPOSIT_CHANNEL_TYPE.WXLITE.value) -> str:
        wx_notify = self.get_notify_func()(xml)
        if not wx_notify['suc']:
            return WX_FAILED_XML
        attach_json = wx_notify.get("attach_json")
        notify = wx_notify.get("notify")
        if not (attach_json.get('objectId') and attach_json.get('configId')):
            return WX_FAILED_XML
        object_id = attach_json.get('objectId')
        deposit_card_id = attach_json.get('configId')
        time_end = notify.get('time_end')
        transaction_id = notify.get('transaction_id')
        out_trade_no = notify.get('out_trade_no')
        total_fee = notify.get('total_fee')
        res = DepositCardService.deposit_card_record(object_id, deposit_card_id, total_fee, transaction_id, time_end,
                                                     self.channel, deposit_channel)
        if not res:
            logger.info('会员卡息写入失败')
            return WX_ERROR_XML
        pre_value = {
            "object_id": object_id,
            "deposit_card_id": deposit_card_id,
            "amount": total_fee,
            "trade_no": out_trade_no,
            "account_channel": RIDING_CHANNEL_TYPE.WXLITE.value,
            "deposit_channel": DEPOSIT_CHANNEL_TYPE.WXLITE.value,
            "mch_id": cfg.get("wx_lite", {}).get("mch_id", ""),
            "app_id": cfg.get("wx_lite", {}).get("appid", "")
        }
        if self.channel == RIDING_CHANNEL_TYPE.WXLITE.value:
            WeLiteService.del_info("deposit_card", json.dumps(pre_value))
        return WX_SUCCESS_XML

    def wx_lite(self, xml):
        return self.wx_pay(xml)

    def ali_pay(self, notify):
        # gmt_create, charset, seller_email, notify_time, subject, seller_id, buyer_id, passback_params, version, notify_id, \
        # notify_type, out_trade_no, total_amount, trade_status, refund_fee, trade_no, auth_app_id, gmt_close, \
        # buyer_logon_id, app_id, sign_type, sign = notify

        res = self.get_notify_func()(notify)
        if not res['suc']:
            return ALI_FAILED_RESP
        total_amount = notify.get('total_amount')
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        logger.info(f'ali_pay notify passback_params: {notify.get("passback_params")}, '
                    f'type:{type(notify.get("passback_params"))}')
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        deposit_card_id = params.get("configId")
        amount = int(float(total_amount) * 100)
        deposit_card = DepositCardService.deposit_card_record(
            object_id, deposit_card_id, amount, trade_no, gmt_payment, self.channel, DEPOSIT_CHANNEL_TYPE.ALIPAY.value)
        if not deposit_card:
            logger.info('信息写入失败，ali_pay_notify is failed')
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def ali_lite(self, notify) -> dict:
        logger.info(f'depositcard ali lite notify: {notify}')
        decode_notify = self.get_notify_func()(notify)
        if not decode_notify['suc']:
            return ALI_FAILED_RESP
        trade_no = notify.get('trade_no')
        gmt_payment = notify.get('gmt_payment')
        out_trade_no = notify.get('out_trade_no')
        total_fee = int(float(notify.get('total_amount')) * 100)
        params = json.loads(notify.get("passback_params"))
        object_id = params.get("objectId")
        deposit_card_id = params.get("configId")
        logger.info(f'zzzzzzz total_fee: {total_fee}')
        if not object_id:
            return ALI_FAILED_RESP
        res = DepositCardService.deposit_card_record(object_id, deposit_card_id, total_fee, trade_no, gmt_payment,
                                                     self.channel, DEPOSIT_CHANNEL_TYPE.ALI_LITE.value)
        if not res:
            logger.info('会员卡息写入失败')
            return ALI_FAILED_RESP
        return ALI_SUCCESS_RESP

    def union_pay_app(self, notify, deposit_channel=DEPOSIT_CHANNEL_TYPE.UNIONPAY_CODE.value) -> dict:
        query_id = notify.get("queryId")
        validate = self.get_notify_func()(notify)
        logger.info(f'union_app notify validate: {validate}')
        if not validate['suc']:
            return UNION_FAILED_RESP
        attach = validate.get('info')
        object_id = attach.get('objectId')
        deposit_card_id = attach.get('configId')
        txn_time = notify.get('txnTime')
        total_fee = notify.get('txnAmt')
        res = DepositCardService.deposit_card_record(object_id, deposit_card_id, total_fee, query_id, txn_time,
                                                     self.channel, deposit_channel)
        if not res:
            logger.info('会员卡息写入失败')
            return UNION_FAILED_RESP
        return UNION_SUCCESS_RESP

    def union_pay_code(self, notify):
        return self.union_pay_app(notify, deposit_channel=DEPOSIT_CHANNEL_TYPE.UNIONPAY_CODE.value)

    def union_pay_lite(self, xml):
        return self.wx_pay(xml, deposit_channel=DEPOSIT_CHANNEL_TYPE.UNIONPAY_WXLITE.value)


class DepositCardRefundService(BusinessFullPayInterface):
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
                    XcEbikeAccount2.type == SERIAL_TYPE.DEPOSIT_CARD_REFUND.value).first()
        if refund_account:
            return {"suc": False, "info": "已退款"}
        refund_free = int(self.refund_fee)
        account = dao_session.session().query(XcEbikeAccount2) \
            .filter(XcEbikeAccount2.trade_no == self.trade_no,
                    XcEbikeAccount2.objectId == self.object_id,
                    XcEbikeAccount2.type == SERIAL_TYPE.DEPOSIT_CARD.value).first()
        if account and (account.channel != self.channel or account.amount < refund_free):
            return {"suc": False, "info": "退款失败0"}
        else:
            self.total_fee = account.amount
            return {"suc": True, "info": ""}

    def deposit_card_refund(self, refund_func):
        logger.info("deposit card is refund, wx_pay refund")
        d = DepositCardService()
        refund_verify = d.user_can_refund_deposit_card(self.object_id, self.trade_no, self.refund_fee)
        if not refund_verify.get('suc'):
            return refund_verify
        verify_data = refund_verify.get("info")
        refund_free = int(self.refund_fee)
        res = refund_func(self.object_id, self.trade_no, refund_free, self.total_fee)
        if res["suc"]:
            logger.info(f'xxxxxxxx deposit_card refund verify_data:{verify_data}, type:{type(verify_data)}')
            is_refund = d.refund_modify_db(self.object_id, self.trade_no, refund_free,
                                           verify_data.get('depositcard_record'), verify_data.get('account_record'))
            if is_refund:
                return {"suc": True, "info": "退款成功"}
        return {"suc": False, "info": "退款失败"}

    def wx_pay(self):
        return self.deposit_card_refund(WePayService().refund)

    def wx_lite(self):
        return self.deposit_card_refund(WeLiteService().refund)

    def ali_pay(self):
        return self.deposit_card_refund(AliService().refund)

    def ali_lite(self):
        return self.deposit_card_refund(AliLiteService().refund)

    def union_pay_app(self):
        return self.deposit_card_refund(UnionPayForApp().refund)

    def union_pay_code(self):
        return self.deposit_card_refund(UnionPayForCode().refund)

    def union_pay_lite(self):
        return self.deposit_card_refund(UnionPayForWXLite().refund)
