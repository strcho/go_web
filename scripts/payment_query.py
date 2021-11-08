import json
import time

from service.payment.deposit import DepositService
from service.payment.deposit_card import DepositCardService
from service.payment.favorable_card import FavorableCardService
from service.payment.pay_utils.wxlitepay import WeLiteService
from service.payment.riding_card import RidingCardService
from service.payment.wallet import WalletService
from mbutils import dao_session, logger
from model.all_model import *
from utils.schedule import prevent_concurrency


class Payment:

    @staticmethod
    def query_account(trade_no):
        return dao_session.session().query(XcEbikeAccount2).filter(XcEbikeAccount2.trade_no == trade_no).first()

    def query_payment_water_order(self, pay_type, min_time, max_time):
        # 获取需要操作的
        trade_no_list = WeLiteService.range_sel_info(pay_type, min_time, max_time)
        for trade in trade_no_list:
            trade_no_dict = json.loads(trade)
            object_id = trade_no_dict.get("object_id", "")
            amount = trade_no_dict.get("amount", "")
            trade_no = trade_no_dict.get("trade_no", "")
            active_id = trade_no_dict.get("active_id", "")
            account_channel = trade_no_dict.get("account_channel", "")
            app_id = trade_no_dict.get("app_id", "")
            mch_id = trade_no_dict.get("mch_id", "")
            gmt_create = time.time()
            result = WeLiteService().query_order(trade_no, app_id, mch_id)
            logger.info(f"query_payment_water_order trade_no:{trade_no}")
            if result:
                c_result = self.query_account(result)
                if not c_result:
                    logger.info(f"query_payment_water_order payment success trade_no:{trade_no}, trans_id: {result}")
                    WalletService.wallet_record(object_id, amount, result, gmt_create, account_channel, active_id)
                WeLiteService.del_info(pay_type, trade)

    def query_payment_deposit_order(self, pay_type, min_time, max_time):
        # 获取需要操作的
        trade_no_list = WeLiteService.range_sel_info(pay_type, min_time, max_time)
        for trade in trade_no_list:
            trade_no_dict = json.loads(trade)
            object_id = trade_no_dict.get("object_id", "")
            out_trade_no = trade_no_dict.get("out_trade_no", "")
            total_fee = trade_no_dict.get("total_fee", "")
            trade_no = trade_no_dict.get("trade_no", "")
            channel = trade_no_dict.get("channel", "")
            app_id = trade_no_dict.get("app_id", "")
            mch_id = trade_no_dict.get("mch_id", "")
            time_end = time.time()
            logger.info(f"query_payment_deposit_order trade_no:{trade_no}")
            result = WeLiteService().query_order(trade_no, app_id, mch_id)
            if result:
                c_result = self.query_account(result)
                if not c_result:
                    logger.info(f"query_payment_deposit_order trade:{trade}, trans_id: {result}")
                    DepositService.deposit_record(object_id, out_trade_no, result, time_end, channel, total_fee)
                WeLiteService.del_info(pay_type, trade)

    def query_payment_deposit_card_order(self, pay_type, min_time, max_time):
        # 获取需要操作的
        trade_no_list = WeLiteService.range_sel_info(pay_type, min_time, max_time)
        for trade in trade_no_list:
            trade_no_dict = json.loads(trade)
            object_id = trade_no_dict.get("object_id", "")
            deposit_card_id = trade_no_dict.get("deposit_card_id", "")
            amount = trade_no_dict.get("amount", "")
            trade_no = trade_no_dict.get("trade_no", "")
            account_channel = trade_no_dict.get("account_channel", "")
            deposit_channel = trade_no_dict.get("deposit_channel", "")
            app_id = trade_no_dict.get("app_id", "")
            mch_id = trade_no_dict.get("mch_id", "")
            gmt_create = time.time()
            logger.info(f"query_payment_deposit_card_order trade_no:{trade_no}")
            result = WeLiteService().query_order(trade_no, app_id, mch_id)
            if result:
                c_result = self.query_account(trade_no)
                if not c_result:
                    logger.info(f"query_payment_deposit_card_order trade:{trade}, trans_id: {result}")
                    DepositCardService.deposit_card_record(
                        object_id, deposit_card_id, amount, result, gmt_create, account_channel, deposit_channel)
                WeLiteService.del_info(pay_type, trade)

    def query_payment_riding_card_order(self, pay_type, min_time, max_time):
        # 获取需要操作的
        trade_no_list = WeLiteService.range_sel_info(pay_type, min_time, max_time)
        for trade in trade_no_list:
            trade_no_dict = json.loads(trade)
            object_id = trade_no_dict.get("object_id", "")
            serial_type = trade_no_dict.get("serial_type", "")
            amount = trade_no_dict.get("amount", "")
            trade_no = trade_no_dict.get("trade_no", "")
            channel = trade_no_dict.get("channel", "")
            riding_card_id = trade_no_dict.get("riding_card_id", "")
            app_id = trade_no_dict.get("app_id", "")
            mch_id = trade_no_dict.get("mch_id", "")
            time_end = time.time()
            logger.info(f"query_payment_riding_card_order trade_no:{trade_no}")
            result = WeLiteService().query_order(trade_no, app_id, mch_id)
            if result:
                c_result = self.query_account(trade_no)
                if not c_result:
                    logger.info(f"query_payment_riding_card_order trade:{trade}, trans_id: {result}")
                    RidingCardService.riding_card_record(
                        serial_type, object_id, result, amount, channel, time_end, riding_card_id)
                WeLiteService.del_info(pay_type, trade)

    def query_payment_favorable_card_order(self, pay_type, min_time, max_time):
        # 获取需要操作的
        trade_no_list = WeLiteService.range_sel_info(pay_type, min_time, max_time)
        for trade in trade_no_list:
            trade_no_dict = json.loads(trade)
            object_id = trade_no_dict.get("object_id", "")
            serial_type = trade_no_dict.get("serial_type", "")
            amount = trade_no_dict.get("amount", "")
            trade_no = trade_no_dict.get("trade_no", "")
            channel = trade_no_dict.get("channel", "")
            favorable_card_id = trade_no_dict.get("favorable_card_id", "")
            app_id = trade_no_dict.get("app_id", "")
            mch_id = trade_no_dict.get("mch_id", "")
            time_end = time.time()
            logger.info(f"query_payment_favorable_card_order trade_no:{trade_no}")
            result = WeLiteService().query_order(trade_no, app_id, mch_id)
            if result:
                c_result = self.query_account(trade_no)
                if not c_result:
                    logger.info(f"query_payment_favorable_card_order trade:{trade}, trans_id: {result}")
                    FavorableCardService.favorable_card_record(
                        object_id, favorable_card_id, result, serial_type, channel, time_end, amount)
                WeLiteService.del_info(pay_type, trade)

    @prevent_concurrency("Payment.query_payment_order", timeout=30 * 60)
    def query_payment_order(self):
        now_time = time.time()
        now_s = str(now_time).split('.')[0]
        twelve_hour_time = now_time - 48*60*60  # 微信那边的一次支付调起，支付状态只保留两个小时,数据量太大，循环一次，时间太长了
        twelve_hour_time_s = str(twelve_hour_time).split('.')[0]
        # 脚本
        self.query_payment_water_order("wallet", min_time=0, max_time=now_s)
        self.query_payment_deposit_order("deposit", min_time=0, max_time=now_s)
        self.query_payment_deposit_card_order("deposit_card", min_time=0, max_time=now_s)
        self.query_payment_riding_card_order("riding_card", min_time=0, max_time=now_s)
        self.query_payment_favorable_card_order("favorable_card", min_time=0, max_time=now_s)
        WeLiteService().range_del_info("wallet", twelve_hour_time_s)
        WeLiteService().range_del_info("deposit", twelve_hour_time_s)
        WeLiteService().range_del_info("deposit_card", twelve_hour_time_s)
        WeLiteService().range_del_info("riding_card", twelve_hour_time_s)
        WeLiteService().range_del_info("favorable_card", twelve_hour_time_s)



