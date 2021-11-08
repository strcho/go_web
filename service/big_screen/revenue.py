import datetime
import json
import time

from model.all_model import *
from mbutils import dao_session, logger
from utils.constant.config import BigScreenType, DeviceState
from utils.constant.redis_key import *
from utils.constant.account import *
from . import ScreenService


class BigScreenOperationService(ScreenService):

    def __init__(self, operation_type: int, op_area_ids: tuple):
        super().__init__(op_area_ids)
        self.operation_type = operation_type

    def query_one_today(self):
        """
            type == 1:历史欠款补交订单(historical_arrears_pay_back)D
            type == 2:新增实收订单(new_actual_payment)E
            type == 3:新增欠款订单(new_arrears)C
            type == 4:应收订单总额(amount_should_accept_total)A
            type == 5:实收订单总额(actual_payment)B
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
                                                       func.ifnull(func.count(XcEbikeUserOrder.orderId), 0))
        select_order_pay = select_order. \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId). \
            filter(XcEbikeUserWalletRecord.type == 1)
        redis_cost_key = BIG_SCREEN_STATISTICS_COST.format(**{"operation_type": self.operation_type})
        redis_num_key = BIG_SCREEN_STATISTICS_NUM.format(**{"operation_type": self.operation_type})
        total_statistics_cost = dao_session.redis_session.r.hgetall(redis_cost_key)
        total_statistics_num = dao_session.redis_session.r.hgetall(redis_num_key)
        cost_total, num_total = 0, 0
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            select_order_pay = select_order_pay.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            cost_total, num_total = 0, 0
            for op in self.op_area_ids:
                cost_total += float(total_statistics_cost.get(str(op), 0))
                num_total += int(total_statistics_num.get(str(op), 0))
        else:
            for k, v in total_statistics_cost.items(): cost_total += float(v)
            for k, v in total_statistics_num.items(): num_total += int(v)
        cost_total = round(cost_total, 2)
        pay_yesterday = self.pay_yesterday
        if self.operation_type == BigScreenType.historical_arrears_pay_back.value:
            data_time = self.redis_to_time(redis_cost_key)
            pay_today = select_order_pay.filter(
                XcEbikeUserOrder.createdAt < self.today_zero, XcEbikeUserOrder.isPaid == 1,
                XcEbikeUserWalletRecord.createdAt >= self.today_zero
            ).first()
            if data_time < self.today_zero:
                pay_yesterday = select_order_pay.filter(
                    XcEbikeUserOrder.createdAt < self.today_zero, XcEbikeUserOrder.isPaid == 1,
                    XcEbikeUserWalletRecord.createdAt >= data_time, XcEbikeUserWalletRecord.createdAt < self.today_zero
                ).first()
            return {"historical_arrears_pay_back_sum_today": float(pay_today[0]),
                    "historical_arrears_pay_back_sum_today_orders": pay_today[1],
                    "historical_arrears_pay_back_sum": round(cost_total + float(pay_today[0]) + float(pay_yesterday[0]),
                                                             2),
                    "historical_arrears_pay_back_sum_orders": num_total + pay_today[1] + pay_yesterday[1]
                    }
        elif self.operation_type == BigScreenType.new_actual_payment.value:
            data_time = self.redis_to_time(redis_cost_key)
            pay_today = select_order_pay.filter(
                XcEbikeUserOrder.createdAt >= self.today_zero, XcEbikeUserOrder.isPaid == 1,
                XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                XcEbikeUserWalletRecord.type == 1).first()
            if data_time < self.today_zero:
                pay_yesterday = select_order_pay.filter(
                    XcEbikeUserOrder.createdAt >= data_time, XcEbikeUserOrder.isPaid == 1,
                    XcEbikeUserWalletRecord.createdAt >= data_time,
                    XcEbikeUserWalletRecord.type == 1).first()
            return {"new_actual_payment_sum_today": float(pay_today[0]),
                    "new_actual_payment_sum_today_orders": int(pay_today[1]),
                    "new_actual_payment_sum": round(cost_total + float(pay_today[0]) + float(pay_yesterday[0]), 2),
                    "new_actual_payment_sum_orders": num_total + pay_today[1] + pay_yesterday[1]}
        elif self.operation_type == BigScreenType.new_arrears.value:
            data_time = self.redis_to_time(redis_cost_key)
            pay_today = select_order.filter(XcEbikeUserOrder.createdAt >= data_time,
                                            XcEbikeUserOrder.isPaid == 0).first()
            return {"new_arrears_sum_today": float(pay_today[0]),
                    "new_arrears_sum_today_orders": pay_today[1]}
        elif self.operation_type == BigScreenType.amount_should_accept.value:
            data_time = self.redis_to_time(redis_cost_key)
            pay_today = select_order.filter(XcEbikeUserOrder.createdAt >= self.today_zero).first()
            if data_time < self.today_zero:
                pay_yesterday = select_order.filter(XcEbikeUserOrder.createdAt >= data_time,
                                                    XcEbikeUserOrder.createdAt < self.today_zero).first()
            return {"amount_should_accept_total_sum_today": float(pay_today[0]),
                    "amount_should_accept_total_sum_today_orders": pay_today[1],
                    "amount_should_accept_total_sum": round(cost_total + float(pay_today[0]) + float(pay_yesterday[0]),
                                                            2),
                    "amount_should_accept_total_sum_orders": num_total + pay_today[1] + pay_yesterday[1]}
        elif self.operation_type == BigScreenType.actual_payment.value:
            data_time = self.redis_to_time(redis_cost_key)
            pay_today = select_order_pay.filter(XcEbikeUserOrder.isPaid == 1,
                                                XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                                                XcEbikeUserWalletRecord.type == 1).first()
            if data_time < self.today_zero:
                pay_yesterday = select_order_pay.filter(XcEbikeUserOrder.isPaid == 1,
                                                        XcEbikeUserWalletRecord.createdAt >= data_time,
                                                        XcEbikeUserWalletRecord.createdAt < self.today_zero,
                                                        XcEbikeUserWalletRecord.type == 1).first()
            return {"actual_payment_sum_today": float(pay_today[0]),
                    "actual_payment_sum_today_orders": pay_today[1],
                    "actual_payment_sum": round(cost_total + float(pay_today[0]) + float(pay_yesterday[0]), 2),
                    "actual_payment_sum_orders": num_total + pay_today[1] + pay_yesterday[1]}
        else:
            return {}

    def query_one_order_num(self):
        """
        已支付的订单流水（根据支付时间做过滤）
            type == 7:订单量(orderNum)
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.count(XcEbikeUserOrder.orderId), 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        redis_key = BIG_SCREEN_STATISTICS_NUM.format(**{"operation_type": self.operation_type})
        total_statistics_num = dao_session.redis_session.r.hgetall(redis_key)
        num_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                num_total += int(total_statistics_num.get(str(op), 0))
        else:
            for k, v in total_statistics_num.items(): num_total += int(v)
        data_time = self.redis_to_time(redis_key)
        pay_today = select_order.filter(XcEbikeUserOrder.isPaid == 1,
                                        XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                                        XcEbikeUserWalletRecord.type == 1).first()
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(XcEbikeUserOrder.isPaid == 1,
                                                XcEbikeUserWalletRecord.createdAt >= data_time,
                                                XcEbikeUserWalletRecord.createdAt < self.today_zero,
                                                XcEbikeUserWalletRecord.type == 1).first()

        today_num = int(pay_today[0])
        return {"sum": today_num + num_total + int(pay_yesterday[0]), "today": today_num}

    def query_one_order_water(self):
        """
            已支付的订单流水（根据支付时间做过滤）
            type == 6:订单流水(orderWater)
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        redis_key = BIG_SCREEN_STATISTICS_COST.format(**{"operation_type": self.operation_type})
        total_statistics_water = dao_session.redis_session.r.hgetall(redis_key)
        water_total = 0
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                water_total += float(total_statistics_water.get(str(op), 0))
        else:
            for k, v in total_statistics_water.items(): water_total += float(v)
        water_total = round(water_total, 2)
        pay_yesterday = self.pay_yesterday
        data_time = self.redis_to_time(redis_key)
        pay_today = select_order.filter(XcEbikeUserOrder.isPaid == 1,
                                        XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                                        XcEbikeUserWalletRecord.type == 1).first()
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(XcEbikeUserOrder.isPaid == 1,
                                                XcEbikeUserWalletRecord.createdAt >= data_time,
                                                XcEbikeUserWalletRecord.createdAt < self.today_zero,
                                                XcEbikeUserWalletRecord.type == 1).first()
        today_water = float(pay_today[0])
        return {"sum": round(today_water + water_total + float(pay_yesterday[0]), 2), "today": round(today_water, 2)}

    def query_one_order_penalty(self):
        """
            type == 8  # 总罚金(orderPenalty)
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.penalty) / 100, 0))
        redis_key = BIG_SCREEN_STATISTICS_PENALTY.format(**{"operation_type": self.operation_type})
        total_statistics_penalty = dao_session.redis_session.r.hgetall(redis_key)
        penalty_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                penalty_total += float(total_statistics_penalty.get(str(op), 0))
        else:
            for k, v in total_statistics_penalty.items(): penalty_total += float(v)
        penalty_total = round(penalty_total, 2)
        data_time = self.redis_to_time(redis_key)
        pay_today = select_order.filter(XcEbikeUserOrder.createdAt >= self.today_zero).first()
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(XcEbikeUserOrder.createdAt >= data_time,
                                                XcEbikeUserOrder.createdAt < self.today_zero).first()
        today_penalty = float(pay_today[0])
        return {"sum": round(today_penalty + penalty_total + float(pay_yesterday[0]), 2),
                "today": round(today_penalty, 2)}

    def query_one_charging_order_water(self):
        """
            type ==9  # 充值金额结算 + 赠送金额结算（已支付订单流水）(chargingOrderWater)
            根据支付时间去计算，聚合
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
                                                       func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        redis_key1 = BIG_SCREEN_STATISTICS_RECHARGE.format(**{"operation_type": self.operation_type})
        redis_key2 = BIG_SCREEN_STATISTICS_PRESENT.format(**{"operation_type": self.operation_type})
        total_statistics_recharge = dao_session.redis_session.r.hgetall(redis_key1)
        total_statistics_present = dao_session.redis_session.r.hgetall(redis_key2)
        recharge_total, present_total = 0, 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                recharge_total += float(total_statistics_recharge.get(str(op), 0))
                present_total += float(total_statistics_present.get(str(op), 0))
        else:
            for k, v in total_statistics_recharge.items(): recharge_total += float(v)
            for k, v in total_statistics_present.items(): present_total += float(v)
        data_time = self.redis_to_time(redis_key1)
        recharge_total, present_total = round(recharge_total, 2), round(present_total, 2)
        pay_today = select_order.filter(XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                                        XcEbikeUserWalletRecord.type == 1).first()
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(XcEbikeUserWalletRecord.createdAt >= data_time,
                                                XcEbikeUserWalletRecord.createdAt < self.today_zero,
                                                XcEbikeUserWalletRecord.type == 1).first()
        today_recharge, today_present = float(pay_today[0]), float(pay_today[1])
        yesterday_recharge, yesterday_present = float(pay_yesterday[0]), float(pay_yesterday[1])
        return {"presentSum": round(today_present + present_total + yesterday_present, 2),
                "presentSumToday": round(today_present, 2),
                "rechargeSum": round(today_recharge + recharge_total + yesterday_recharge, 2),
                "rechargeSumToday": round(today_recharge, 2)}

    def query_one_charging_timely_order_water(self):
        """
            type ==10  # 充值金额结算 + 赠送金额结算（实收订单流水）(chargingOrderWater)
            根据支付时间去计算，聚合
        """
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
                                                       func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0))
        redis_key1 = BIG_SCREEN_STATISTICS_RECHARGE.format(**{"operation_type": self.operation_type})
        redis_key2 = BIG_SCREEN_STATISTICS_PRESENT.format(**{"operation_type": self.operation_type})
        total_statistics_recharge = dao_session.redis_session.r.hgetall(redis_key1)
        total_statistics_present = dao_session.redis_session.r.hgetall(redis_key2)
        recharge_total, present_total = 0, 0
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                recharge_total += float(total_statistics_recharge.get(str(op), 0))
                present_total += float(total_statistics_present.get(str(op), 0))
        else:
            for k, v in total_statistics_recharge.items(): recharge_total += float(v)
            for k, v in total_statistics_present.items(): present_total += float(v)
        data_time = self.redis_to_time(redis_key1)
        recharge_total, present_total = round(recharge_total, 2), round(present_total, 2)
        pay_today = select_order.filter(XcEbikeUserOrder.createdAt >= self.today_zero).first()
        pay_yesterday = self.pay_yesterday
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(
                XcEbikeUserOrder.createdAt >= data_time, XcEbikeUserOrder.createdAt < self.today_zero).first()
        today_recharge, today_present = float(pay_today[0]), float(pay_today[1])
        yesterday_recharge, yesterday_present = float(pay_yesterday[0]), float(pay_yesterday[1])
        return {"presentSum": round(today_present + present_total + yesterday_present, 2),
                "presentSumToday": round(today_present, 2),
                "rechargeSum": round(today_recharge + recharge_total + yesterday_recharge, 2),
                "rechargeSumToday": round(today_recharge, 2)}


class BigScreenTimeService(ScreenService):

    def __init__(self, valid_data, op_area_ids: tuple):
        super().__init__(op_area_ids)
        start_time, end_time = valid_data
        #  s_time为开始时间，e_time为结束时间
        self.s_time, self.e_time = self.millisecond2datetime(start_time), self.millisecond2datetime(end_time)

    def query_receivable_orders(self):
        """
        A:应收订单(总额)：订单生成时间在该时段
        """
        receivable_orders = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.date_format(XcEbikeUserOrder.createdAt, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            receivable_orders = receivable_orders.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        receivable_orders = receivable_orders.filter(XcEbikeUserOrder.createdAt >= self.s_time,
                                                     XcEbikeUserOrder.createdAt <= self.e_time).group_by("ctime").all()
        receivable_orders_total, receivable_orders_dict = 0, {}
        for pay, pay_date in receivable_orders:
            receivable_orders_dict[pay_date] = pay
            receivable_orders_total += pay
        return receivable_orders_total, receivable_orders_dict

    def query_paid_orders(self):
        """
        B:实收订单(总额)：订单支付时间在该时段。
        """
        paid_orders = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime")). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            paid_orders = paid_orders.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        paid_orders = paid_orders.filter(XcEbikeUserWalletRecord.createdAt >= self.s_time,
                                         XcEbikeUserWalletRecord.createdAt <= self.e_time,
                                         XcEbikeUserOrder.isPaid == 1,
                                         XcEbikeUserWalletRecord.type == 1).all()
        paid_orders_total, paid_orders_dict = 0, {}
        for pay, pay_date in paid_orders:
            paid_orders_dict[pay_date] = pay
            paid_orders_total += pay
        return paid_orders_total, paid_orders_dict

    def query_today_paid_orders(self):
        """
        E:新增实收订单(总额)：订单支付时间及生成时间都在该时段
        """
        today_paid_orders = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime")). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            today_paid_orders = today_paid_orders.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        today_paid_orders = today_paid_orders.filter(
            XcEbikeUserWalletRecord.createdAt >= self.s_time, XcEbikeUserWalletRecord.createdAt <= self.e_time,
            XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.type == 1,
            func.datediff(XcEbikeUserWalletRecord.createdAt, XcEbikeUserOrder.createdAt) < 1).group_by("ctime").all()
        today_paid_orders_total, today_paid_orders_dict = 0, {}
        for pay, pay_date in today_paid_orders:
            today_paid_orders_dict[pay_date] = pay
            today_paid_orders_total += pay
        return today_paid_orders_total, today_paid_orders_dict

    def query_today_paid_orders_time(self):
        """
        E:新增实收订单(总额)：订单支付时间及生成时间都在该时段
        """
        today_paid_orders = dao_session.session().query(func.sum(XcEbikeUserOrder.cost) / 100). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            today_paid_orders = today_paid_orders.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        today_paid_orders = today_paid_orders.filter(
            XcEbikeUserWalletRecord.createdAt >= self.s_time, XcEbikeUserWalletRecord.createdAt <= self.e_time,
            XcEbikeUserOrder.isPaid == 1,
            func.datediff(XcEbikeUserWalletRecord.createdAt, XcEbikeUserOrder.createdAt) < 1).all()
        return today_paid_orders

    def query_historical_orders_payment(self):
        """
        D:历史欠款补交订单(总额)：订单支付时间在该时段，且生成时间不在该时段
        """
        historical_orders_payment = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime")). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            historical_orders_payment = historical_orders_payment.filter(
                XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        historical_orders_payment = historical_orders_payment.filter(
            XcEbikeUserWalletRecord.createdAt >= self.s_time, XcEbikeUserWalletRecord.createdAt <= self.e_time,
            XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.type == 1,
            func.datediff(XcEbikeUserWalletRecord.createdAt, XcEbikeUserOrder.createdAt) >= 1).all()
        historical_orders_payment_total, historical_orders_payment_dict = 0, {}
        for pay, pay_date in historical_orders_payment:
            historical_orders_payment_dict[pay_date] = pay
            historical_orders_payment_total += pay
        return historical_orders_payment_total, historical_orders_payment_dict

    def query_historical_orders_payment_time(self):
        """
        D:历史欠款补交订单(总额)：订单支付时间在该时段，且生成时间不在该时段
        """
        historical_orders_payment = dao_session.session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            historical_orders_payment = historical_orders_payment.filter(
                XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        historical_orders_payment = historical_orders_payment.filter(
            XcEbikeUserWalletRecord.createdAt >= self.s_time, XcEbikeUserWalletRecord.createdAt <= self.e_time,
            XcEbikeUserOrder.isPaid == 1,
            func.datediff(XcEbikeUserWalletRecord.createdAt, XcEbikeUserOrder.createdAt) >= 1).first()
        return historical_orders_payment

    def query_arrears_paid_ratio(self, se_time: int):
        """
            欠款支付，已支付和未支付的数据
            统计向后三个月的数据
        """
        se_time = self.millisecond2datetime(se_time),
        arrears_ratio = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.ifnull(func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d"), '').label("ctime")). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            arrears_ratio = arrears_ratio.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        arrears_ratio = arrears_ratio.filter(XcEbikeUserOrder.createdAt >= self.s_time,
                                             XcEbikeUserOrder.createdAt < self.e_time,
                                             XcEbikeUserWalletRecord.createdAt >= self.e_time,
                                             XcEbikeUserWalletRecord.createdAt < se_time,
                                             XcEbikeUserWalletRecord.type == 1).group_by("ctime").all()
        return {j: i for i, j in arrears_ratio}

    def query_order_should_payment(self):
        """
        当前时间范围内，应该支付的钱
        """
        arrears_ratio_total = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0))
        if self.op_area_ids:
            arrears_ratio_total = arrears_ratio_total.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        arrears_ratio_total = arrears_ratio_total.filter(
            XcEbikeUserOrder.createdAt >= self.s_time, XcEbikeUserOrder.createdAt < self.e_time).first()
        return float(arrears_ratio_total[0])

    def query_order_actually_payment(self):
        """
        当前时间范围内，实际支付的钱
        """
        payment_ratio_total = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            payment_ratio_total = payment_ratio_total.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        payment_ratio_total = payment_ratio_total.filter(
            XcEbikeUserOrder.createdAt >= self.s_time, XcEbikeUserOrder.createdAt < self.e_time,
            XcEbikeUserWalletRecord.createdAt >= self.s_time, XcEbikeUserWalletRecord.createdAt < self.e_time,
            XcEbikeUserWalletRecord.type == 1).first()
        return float(payment_ratio_total[0])

    def query_order_water_total(self):
        """时间范围内的订单流水"""
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100,
            func.ifnull(func.count(XcEbikeUserOrder.cost), 0), )
        if self.op_area_ids:
            order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        order_water = order_water.filter(XcEbikeUserOrder.createdAt.between(self.s_time, self.e_time)).first()
        return round(order_water[0], 2), order_water[1]

    def query_order_water_paid(self):
        """时间范围内的已支付的订单流水"""
        try:
            order_water = dao_session.sub_session().query(
                func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100,
                func.ifnull(func.count(XcEbikeUserOrder.cost), 0),
                func.count(XcEbikeUserWalletRecord.id)). \
                prefix_with("STRAIGHT_JOIN"). \
                join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
            order_water = order_water.with_hint(XcEbikeUserWalletRecord,
                                                "force index(idx_serviceId_type_createdAt_change)")
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserWalletRecord.createdAt.between(self.s_time, self.e_time),
                                             XcEbikeUserOrder.isPaid == 1,
                                             XcEbikeUserWalletRecord.type == 1).first()
        except Exception as e:
            logger.error("idx_serviceId_type_createdAt_change is not exits")
            order_water = dao_session.sub_session().query(
                func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100,
                func.ifnull(func.count(XcEbikeUserOrder.cost), 0),
                func.count(XcEbikeUserWalletRecord.id)). \
                prefix_with("STRAIGHT_JOIN"). \
                join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserWalletRecord.createdAt.between(self.s_time, self.e_time),
                                             XcEbikeUserOrder.isPaid == 1,
                                             XcEbikeUserWalletRecord.type == 1).first()
        return round(order_water[0], 2), order_water[1]

    def query_order_present_recharge(self):
        """时间范围内的订单流水和数量"""
        try:
            order_water = dao_session.sub_session().query(
                func.ifnull(func.sum(XcEbikeUserOrder.presentCost), 0) / 100,
                func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost), 0) / 100,
                func.count(XcEbikeUserWalletRecord.id)). \
                prefix_with("STRAIGHT_JOIN"). \
                join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
            order_water = order_water.with_hint(XcEbikeUserWalletRecord,
                                                "force index(idx_serviceId_type_createdAt_change)")
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserWalletRecord.createdAt.between(self.s_time, self.e_time),
                                             XcEbikeUserOrder.isPaid == 1,
                                             XcEbikeUserWalletRecord.type == 1).first()
        except Exception as e:
            logger.error("query_order_present_recharge is error, {}".format(e))
            order_water = dao_session.sub_session().query(
                func.ifnull(func.sum(XcEbikeUserOrder.presentCost), 0) / 100,
                func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost), 0) / 100,
                func.count(XcEbikeUserWalletRecord.id)). \
                prefix_with("STRAIGHT_JOIN"). \
                join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserWalletRecord.createdAt.between(self.s_time, self.e_time),
                                             XcEbikeUserOrder.isPaid == 1,
                                             XcEbikeUserWalletRecord.type == 1).first()
        return round(order_water[0], 2), round(order_water[1], 2)

    def query_order_water_total_statistics(self):
        """读取总订单流失，已支付的订单流水"""
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_paid_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_present_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_recharge_cost), 0) / 100,
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            order_water = order_water.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        order_water = order_water.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time)). \
            group_by("ctime").all()
        total_dict, paid_dict, present_dict, recharge_dict = {}, {}, {}, {}
        for total, paid, present, recharge, service_id in order_water:
            service_id = str(service_id)
            total_dict[service_id] = total
            paid_dict[service_id] = paid
            present_dict[service_id] = present
            recharge_dict[service_id] = recharge
        return total_dict, paid_dict, present_dict, recharge_dict

    def query_order_num_total_statistics(self):
        """获取订单数量（已支付和总量）"""
        order_num = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_paid_num), 0),
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            order_num = order_num.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        order_num = order_num.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time)). \
            group_by("ctime").all()
        total_dict, paid_dict = {}, {}
        for total, paid, service_id in order_num:
            service_id = str(service_id)
            total_dict[service_id] = total
            paid_dict[service_id] = paid
        return total_dict, paid_dict

    def query_arrears_paid_ratio_new(self, date_list):
        """
            从缓存中获取坏账的记录
        """
        now = datetime.now()
        now_year = now.year
        month, year = self.s_time.month, self.s_time.year
        # 从缓存中读取数据
        redis_key = BIG_SCREEN_STATISTICS_ARREARS_PROPORTION.format(**{"month": month})
        total_statistics = dao_session.redis_session.r.hgetall(redis_key)
        new_data_list = [{"paid_arrears": 0, "unpaid_arrears": 0, "date": d} for d in date_list]
        new_arrears_total, pay_arrears_total = 0, 0
        if now_year == year:
            if self.op_area_ids:
                for op in self.op_area_ids:
                    if total_statistics.get(str(op)):
                        arrears_data = json.loads(total_statistics.get(str(op)))
                        new_arrears_total += round(float(arrears_data.get("new_arrears", 0)), 2)
                        pay_arrears_total += round(float(arrears_data.get("pay_arrears", 0)), 2)
                        new_data = arrears_data.get("new_data", [])
                        for num, new in enumerate(new_data_list):
                            d = new["date"]
                            for n in new_data:
                                if n["date"] == d:
                                    new_data_list[num] = {
                                        "paid_arrears": round(new["paid_arrears"] + n["paid_arrears"], 2),
                                        "unpaid_arrears": round(new["unpaid_arrears"] + n["unpaid_arrears"], 2), "date": d}
            else:
                for k, v in total_statistics.items():
                    arrears_data = json.loads(v)
                    new_arrears_total += round(float(arrears_data.get("new_arrears", 0)), 2)
                    pay_arrears_total += round(float(arrears_data.get("pay_arrears", 0)), 2)
                    new_data = arrears_data.get("new_data", [])
                    for num, new in enumerate(new_data_list):
                        d = new["date"]
                        for n in new_data:
                            if n["date"] == d:
                                new_data_list[num] = {
                                    "paid_arrears": round(new["paid_arrears"] + n["paid_arrears"], 2),
                                    "unpaid_arrears": round(new["unpaid_arrears"] + n["unpaid_arrears"], 2),
                                    "date": d}

        return {"new_data": new_data_list, "new_arrears": round(new_arrears_total, 2),
                "pay_arrears": round(pay_arrears_total, 2)}

    def query_present_recharge(self):
        """历史数据充送分离查询"""
        history = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.history_paid_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.new_paid_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.history_recharge_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.history_present_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.new_recharge_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.new_present_cost), 0) / 100,
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            history = history.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        history = history.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time)). \
            group_by("ctime").all()
        history_cost_dict, new_cost_dict, history_recharge_dict, history_present_dict, new_recharge_dict, \
        new_present_dict = {}, {}, {}, {}, {}, {}
        for hc, nc, hr, hp, nr, np, ctime in history:
            ctime = str(ctime)
            history_cost_dict[ctime] = hc
            new_cost_dict[ctime] = nc
            history_recharge_dict[ctime] = hr
            history_present_dict[ctime] = hp
            new_recharge_dict[ctime] = nr
            new_present_dict[ctime] = np
        return history_cost_dict, new_cost_dict, history_recharge_dict, \
               history_present_dict, new_recharge_dict, new_present_dict

    def query_riding_card_sell_times_old(self):
        """
        旧版骑行卡的售卖次数（柱状图）
        """
        m_times = dao_session.session().query(
            func.date_format(XcEbike2RidingcardAccount.createdAt, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0),
            XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.type).filter(
            XcEbike2RidingcardAccount.createdAt.between(self.s_time, self.e_time),
            XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_old_riding_card_buy()),
            XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2RidingcardAccount.type, XcEbike2RidingcardAccount.money, "ctime").all()
        total_list = []
        for m in m_times:
            r_day, r_count, r_money, r_type = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}天骑行卡".format(
                    '%.2f' % (float(r_money) / 100), SERIAL_TYPE.riding_card_type_days().get(str(r_type), 0)),
                          "date": r_day}
                total_list.append(m_dict)
        return total_list

    def query_riding_card_sell_times_new(self):
        """
        新版骑行卡次卡的售卖次数（柱状图）
        """
        m_times = dao_session.session().query(
            func.date_format(XcEbike2RidingcardAccount.createdAt, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0),
            XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.ridingCardName).filter(
            XcEbike2RidingcardAccount.createdAt.between(self.s_time, self.e_time),
            XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_new_riding_card_buy()),
            XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2RidingcardAccount.ridingCardName, XcEbike2RidingcardAccount.money,
                                   "ctime").all()
        total_list = []
        for m in m_times:
            r_day, r_count, r_money, r_name = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}".format('%.2f' % (float(r_money) / 100), r_name),
                          "date": r_day}
                total_list.append(m_dict)
        return total_list


class BigScreenTotalService(ScreenService):

    def query_screen_operate_last_time(self):
        op_time = dao_session.sub_session().query(XcMieba2BigScreenOperate.day_time). \
            order_by(XcMieba2BigScreenOperate.created_at.desc()).first()
        return int(time.mktime(op_time[0].timetuple())) * 1000 if op_time else 0

    def query_screen_revenue_last_time(self):
        op_time = dao_session.sub_session().query(XcMieba2BigScreenOperate.day_time). \
            order_by(XcMieba2BigScreenOperate.created_at.desc()).first()
        return int(time.mktime(op_time[0].timetuple())) * 1000 if op_time else 0

    def query_deposit_income(self):
        """押金卡（会员卡）的营收金额"""
        redis_key = BIG_SCREEN_STATISTICS_DEPOSIT_INCOME
        data_time = self.redis_to_time(redis_key)
        data_today = dao_session.sub_session().query(func.ifnull(func.sum(XcEbike2DepositCard.money), 0) / 100)
        total_statistics = dao_session.redis_session.r.hgetall(redis_key)
        data_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            data_today = data_today.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids: data_total += float(total_statistics.get(str(op), 0))
        else:
            for k, v in total_statistics.items(): data_total += float(v)
        data_total = round(data_total, 2)
        data_today = data_today.filter(
            XcEbike2DepositCard.createdAt >= self.today_zero,
            XcEbike2DepositCard.channel.notin_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())).first()
        if data_time < self.today_zero:
            pay_yesterday = data_today.filter(
                XcEbike2DepositCard.createdAt > data_time,
                XcEbike2DepositCard.createdAt < self.today_zero,
                XcEbike2DepositCard.channel.notin_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())).first()
        data_today = float(data_today[0])
        data_yesterday = float(pay_yesterday[0])
        return {"sum": round(data_today + data_total + data_yesterday, 2), "today": round(data_today, 2)}

    def query_riding_income(self):
        """骑行卡的营收金额"""
        redis_key = BIG_SCREEN_STATISTICS_RIDING_INCOME
        data_time = self.redis_to_time(redis_key)
        data_today = dao_session.sub_session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100)
        total_statistics = dao_session.redis_session.r.hgetall(redis_key)
        data_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            data_today = data_today.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids: data_total += float(total_statistics.get(str(op), 0))
        else:
            for k, v in total_statistics.items(): data_total += float(v)
        if not data_total:
            data_total_s = data_today.filter(
                XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())).first()
            if data_total_s:
                data_total = data_total_s[0]
        data_total = round(float(data_total), 2)
        data_today = data_today.filter(
            XcEbike2RidingcardAccount.createdAt >= self.today_zero,
            XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())).first()
        if data_time < self.today_zero:
            pay_yesterday = data_today.filter(
                XcEbike2RidingcardAccount.createdAt > data_time,
                XcEbike2RidingcardAccount.createdAt < self.today_zero,
                XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())).first()
        data_today = float(data_today[0])
        data_yesterday = float(pay_yesterday[0])
        return {"sum": round(data_today + data_total + data_yesterday, 2), "today": round(data_today, 2)}

    def query_riding_card_sell_money(self):
        """骑行卡的营收金额"""
        data_today = dao_session.session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100). \
            filter(XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            data_today = data_today.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        data_total_s = data_today.filter().first()
        data_total = round(float(data_total_s[0]), 2)
        data_today = data_today.filter(XcEbike2RidingcardAccount.createdAt >= self.today_zero).first()
        data_today = float(data_today[0])
        return {"sum": round(data_today + data_total, 2), "today": round(data_today, 2)}

    def query_riding_card_giving_money(self):
        """骑行卡赠送金额"""
        money = dao_session.session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100).filter(
            XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money = money.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
            money_total = money.first()
            money_today = money.filter(XcEbike2RidingcardAccount.createdAt >= self.today_zero).first()
        else:
            money_total = money.first()
            money_today = money.filter(XcEbike2RidingcardAccount.createdAt >= self.today_zero).first()
        money_total_m = money_total[0] if money_total else 0
        money_today_m = money_today[0] if money_today else 0
        return {"sum": round(float(money_total_m) + float(money_today_m), 2), "today": round(float(money_today_m), 2)}

    def query_riding_refund(self, b_time=None):
        """
        骑行卡的退款
        b_time: 获取大于b_time的数据
        """
        data_today = dao_session.sub_session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100)
        if self.op_area_ids:
            data_today = data_today.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        if b_time:
            data_today = data_today.filter(XcEbike2RidingcardAccount.createdAt >= b_time)
        data_today = data_today.filter(
            XcEbike2RidingcardAccount.type == SERIAL_TYPE.RIDING_CARD_REFUND.value).first()
        return float(abs(data_today[0]))

    def query_wallet_income(self):
        """钱包的营收金额"""
        redis_key = BIG_SCREEN_STATISTICS_WALLET_INCOME
        data_time = self.redis_to_time(redis_key)
        data_today = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserWalletRecord.change), 0) / 100)
        total_statistics = dao_session.redis_session.r.hgetall(redis_key)
        data_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            data_today = data_today.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids: data_total += float(total_statistics.get(str(op), 0))
        else:
            for k, v in total_statistics.items(): data_total += float(v)
        data_total = round(data_total, 2)
        data_today = data_today.filter(
            XcEbikeUserWalletRecord.createdAt >= self.today_zero,
            XcEbikeUserWalletRecord.type.in_(PAY_TYPE.wallet_reality_income_type())).first()
        if data_time < self.today_zero:
            pay_yesterday = data_today.filter(
                XcEbikeUserWalletRecord.createdAt > data_time,
                XcEbikeUserWalletRecord.createdAt < self.today_zero,
                XcEbikeUserWalletRecord.type.in_(PAY_TYPE.wallet_reality_income_type())).first()
        data_today = float(data_today[0])
        data_yesterday = float(pay_yesterday[0])
        return {"sum": round(data_today + data_total + data_yesterday, 2), "today": round(data_today, 2)}

    def query_order_should_water(self):
        """总订单的营收"""
        select_order = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
                                                       func.ifnull(func.count(XcEbikeUserOrder.orderId), 0))
        redis_cost_key = BIG_SCREEN_STATISTICS_COST.format(
            **{"operation_type": BigScreenType.amount_should_accept.value})
        total_statistics_cost = dao_session.redis_session.r.hgetall(redis_cost_key)
        cost_total = 0
        pay_yesterday = self.pay_yesterday
        if self.op_area_ids:
            select_order = select_order.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                cost_total += float(total_statistics_cost.get(str(op), 0))
        else:
            for k, v in total_statistics_cost.items(): cost_total += float(v)
        data_time = self.redis_to_time(redis_cost_key)
        pay_today = select_order.filter(XcEbikeUserOrder.createdAt >= self.today_zero).first()
        if data_time < self.today_zero:
            pay_yesterday = select_order.filter(XcEbikeUserOrder.createdAt >= data_time,
                                                XcEbikeUserOrder.createdAt < self.today_zero).first()
        return {"today": float(pay_today[0]),
                "sum": round(cost_total + float(pay_today[0]) + float(pay_yesterday[0]), 2)}

    def query_order_ticket_water(self):
        """用户工单退款的充送分离"""
        redis_key_recharge = BIG_SCREEN_STATISTICS_WALLET_RECHARGE
        redis_key_present = BIG_SCREEN_STATISTICS_WALLET_PRESENT
        recharge = dao_session.redis_session.r.hgetall(redis_key_recharge)
        present = dao_session.redis_session.r.hgetall(redis_key_present)
        recharge_present = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.presentChange) / 100, 0)). \
            filter(XcEbikeUserWalletRecord.type == PAY_TYPE.ITINERARY_REFOUND.value)
        recharge_total, present_total = 0, 0
        y_recharge_present = self.pay_yesterday
        data_time = self.redis_to_time(redis_key_recharge)
        if self.op_area_ids:
            recharge_present = recharge_present.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            for op in self.op_area_ids:
                recharge_total += float(recharge.get(str(op), 0))
                present_total += float(present.get(str(op), 0))
        else:
            for k, v in recharge.items(): recharge_total += float(v)
            for k, v in present.items(): present_total += float(v)
        recharge_present1 = recharge_present.filter(XcEbikeUserWalletRecord.createdAt >= self.today_zero).first()
        if data_time < self.today_zero:
            y_recharge_present = recharge_present.filter(
                XcEbikeUserWalletRecord.createdAt >= data_time,
                XcEbikeUserWalletRecord.createdAt < self.today_zero).first()
        today_recharge, today_present = float(recharge_present1[0]), float(recharge_present1[1])
        yesterday_recharge, yesterday_present = float(y_recharge_present[0]), float(y_recharge_present[1])
        return {"backPresentWallet": round(today_present + present_total + yesterday_present, 2),
                "backPresentWalletToday": round(today_present, 2),
                "backRechargeWallet": round(today_recharge + recharge_total + yesterday_recharge, 2),
                "backRechargeWalletToday": round(today_recharge, 2)}

    def query_historical_arrears_present_recharge(self):
        operation = BigScreenType.historical_arrears_pay_back.value
        redis_recharge_key = BIG_SCREEN_STATISTICS_RECHARGE.format(**{"operation_type": operation})
        redis_present_key = BIG_SCREEN_STATISTICS_PRESENT.format(**{"operation_type": operation})
        total_statistics_recharge = dao_session.redis_session.r.hgetall(redis_recharge_key)
        total_statistics_present = dao_session.redis_session.r.hgetall(redis_present_key)
        recharge_total, present_total = 0, 0
        if self.op_area_ids:
            for op in self.op_area_ids:
                recharge_total += float(total_statistics_recharge.get(str(op), 0))
                present_total += float(total_statistics_present.get(str(op), 0))
        else:
            for k, v in total_statistics_recharge.items(): recharge_total += float(v)
            for k, v in total_statistics_present.items(): present_total += float(v)
        recharge_total, present_total = round(recharge_total, 2), round(present_total, 2)
        data_time = self.redis_to_time(redis_recharge_key)
        pay_today = self.query_historical_arrears_present_recharge_time(self.today_zero)
        pay_yesterday = self.pay_yesterday
        if self.today_zero > data_time:
            pay_yesterday = self.query_historical_arrears_present_recharge_time(data_time, self.today_zero)
        return {"historical_arrears_pay_back_recharge_today": float(pay_today[0]),
                "historical_arrears_pay_back_present_today": float(pay_today[1]),
                "historical_arrears_pay_back_recharge_sum": round(
                    recharge_total + float(pay_today[0]) + float(pay_yesterday[0]), 2),
                "historical_arrears_pay_back_present_sum": round(
                    present_total + float(pay_today[1]) + float(pay_yesterday[1]), 2)
                }

    def query_new_order_present_recharge(self):
        operation = BigScreenType.new_actual_payment.value
        redis_recharge_key = BIG_SCREEN_STATISTICS_RECHARGE.format(**{"operation_type": operation})
        redis_present_key = BIG_SCREEN_STATISTICS_PRESENT.format(**{"operation_type": operation})
        total_statistics_recharge = dao_session.redis_session.r.hgetall(redis_recharge_key)
        total_statistics_present = dao_session.redis_session.r.hgetall(redis_present_key)
        recharge_total, present_total = 0, 0
        if self.op_area_ids:
            for op in self.op_area_ids:
                recharge_total += float(total_statistics_recharge.get(str(op), 0))
                present_total += float(total_statistics_present.get(str(op), 0))
        else:
            for k, v in total_statistics_recharge.items(): recharge_total += float(v)
            for k, v in total_statistics_present.items(): present_total += float(v)
        recharge_total, present_total = round(recharge_total, 2), round(present_total, 2)
        data_time = self.redis_to_time(redis_recharge_key)
        pay_today = self.query_new_order_present_recharge_time(self.today_zero)
        pay_yesterday = self.pay_yesterday
        if self.today_zero > data_time:
            pay_yesterday = self.query_historical_arrears_present_recharge_time(data_time, self.today_zero)
        return {"new_actual_payment_recharge_today": float(pay_today[0]),
                "new_actual_payment_present_today": float(pay_today[1]),
                "new_actual_payment_recharge_sum": round(recharge_total + float(pay_today[0]) + float(pay_yesterday[0]),
                                                         2),
                "new_actual_payment_present_sum": round(present_total + float(pay_today[1]) + float(pay_yesterday[1]),
                                                        2)
                }

    def query_historical_arrears_present_recharge_time(self, data_time, today_time=None):
        """历史欠款补缴"""
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserOrder.createdAt < data_time, XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.createdAt >= data_time, XcEbikeUserWalletRecord.type == 1
        )
        if today_time:
            pay_today = pay_today.filter(XcEbikeUserWalletRecord.createdAt < today_time)
        pay_today = pay_today.first()
        return pay_today

    def query_new_order_present_recharge_time(self, data_time, today_time=None):
        """新增实际付款订单"""
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeUserOrder.orderId)
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserOrder.createdAt >= data_time, XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.createdAt >= data_time, XcEbikeUserWalletRecord.type == 1
        )
        if today_time:
            pay_today = pay_today.filter(XcEbikeUserWalletRecord.createdAt < today_time)
        pay_today = pay_today.first()
        return pay_today

    def query_merchants_wallet(self, merchants, merchants_type, merchants_channel):
        """商户统计钱包收入（支付宝，微信，云闪付的区分）"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserWalletRecord.change), 0) / 100,
                                            func.ifnull(func.count(XcEbikeAccount2.serialNo), 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
            join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeAccount2.serialNo)
        y = self.pay_yesterday
        if self.op_area_ids:
            m = m.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        data_time = self.redis_to_time(merchants_key)
        m1 = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                      XcEbikeUserWalletRecord.createdAt >= self.today_zero,
                      XcEbikeAccount2.channel.in_(merchants_channel)).first()
        if data_time < self.today_zero:
            y = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                         XcEbikeUserWalletRecord.createdAt >= data_time,
                         XcEbikeUserWalletRecord.createdAt < self.today_zero,
                         XcEbikeAccount2.channel.in_(merchants_channel)).first()
        return abs(float(m1[0])), m1[1], abs(float(y[0])), y[1]

    def query_merchants_deposit_card(self, merchants, merchants_type, merchants_channel):
        """商户统计会员卡收入（支付宝，微信，云闪付的区分）"""
        """XcEbikeAccount2.amount使用amount,而不是XcEbike2DepositCard.money"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
                                        func.ifnull(func.count(XcEbikeAccount2.serialNo), 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_account_2) */"). \
            join(XcEbike2DepositCard, XcEbikeAccount2.trade_no == XcEbike2DepositCard.trade_no)
        if self.op_area_ids:
            m = m.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        data_time = self.redis_to_time(merchants_key)
        y = self.pay_yesterday
        m1 = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                      XcEbikeAccount2.createdAt >= self.today_zero,
                      XcEbikeAccount2.channel.in_(merchants_channel),
                      XcEbikeAccount2.trade_no != "").first()
        if data_time < self.today_zero:
            y = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                         XcEbikeAccount2.createdAt >= data_time,
                         XcEbikeAccount2.createdAt < self.today_zero,
                         XcEbikeAccount2.channel.in_(merchants_channel),
                         XcEbikeAccount2.trade_no != "").first()
        return abs(float(m1[0])), m1[1], abs(float(y[0])), y[1]

    def query_merchants_riding_card(self, merchants, merchants_type, merchants_channel):
        """商户统计骑行卡收入（支付宝，微信，云闪付的区分）"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100,
                                            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0))
        if self.op_area_ids:
            m = m.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        data_time = self.redis_to_time(merchants_key)
        m1 = m.filter(XcEbike2RidingcardAccount.type.in_(merchants_type),
                      XcEbike2RidingcardAccount.createdAt >= self.today_zero,
                      XcEbike2RidingcardAccount.channel.in_(merchants_channel)).first()
        y = self.pay_yesterday
        if data_time < self.today_zero:
            y = m.filter(XcEbike2RidingcardAccount.type.in_(merchants_type),
                         XcEbike2RidingcardAccount.createdAt >= data_time,
                         XcEbike2RidingcardAccount.createdAt < self.today_zero,
                         XcEbike2RidingcardAccount.channel.in_(merchants_channel)).first()
        return abs(float(m1[0])), m1[1], abs(float(y[0])), y[1]

    def query_merchants_favorable_card(self, merchants, merchants_type, merchants_channel):
        """商户统计优惠卡收入（支付宝，微信，云闪付的区分）"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcMieba2FavorableCardAccount.price), 0) / 100,
                                            func.ifnull(func.count(XcMieba2FavorableCardAccount.id), 0))
        if self.op_area_ids:
            m = m.filter(XcMieba2FavorableCardAccount.service_id.in_(self.op_area_ids))
        data_time = self.redis_to_time(merchants_key)
        m1 = m.filter(XcMieba2FavorableCardAccount.serial_type.in_(merchants_type),
                      XcMieba2FavorableCardAccount.created_at >= self.today_zero,
                      XcMieba2FavorableCardAccount.channel.in_(merchants_channel)).first()
        y = self.pay_yesterday
        if data_time < self.today_zero:
            y = m.filter(XcMieba2FavorableCardAccount.serial_type.in_(merchants_type),
                         XcMieba2FavorableCardAccount.created_at >= data_time,
                         XcMieba2FavorableCardAccount.created_at < self.today_zero,
                         XcMieba2FavorableCardAccount.channel.in_(merchants_channel)).first()
        return abs(float(m1[0])), m1[1], abs(float(y[0])), y[1]

    def query_merchants_deposit(self, merchants, merchants_type, merchants_channel):
        """商户统计诚信金收入（支付宝，微信，云闪付的区分）"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
                                            func.ifnull(func.count(XcEbikeAccount2.amount), 0)). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId)
        if self.op_area_ids:
            m = m.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        data_time = self.redis_to_time(merchants_key)
        m1 = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                      XcEbikeAccount2.createdAt >= self.today_zero,
                      XcEbikeAccount2.channel.in_(merchants_channel)).first()
        y = self.pay_yesterday
        if data_time < self.today_zero:
            y = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                         XcEbikeAccount2.createdAt >= data_time,
                         XcEbikeAccount2.createdAt < self.today_zero,
                         XcEbikeAccount2.channel.in_(merchants_channel)).first()
        return abs(float(m1[0])), m1[1], abs(float(y[0])), y[1]

    def query_merchants_history(self, merchants: int):
        """商户统计读取缓存数据（支付宝，微信，云闪付的区分）"""
        merchants_key = BIG_SCREEN_STATISTICS_MERCHANTS.format(**{"merchants": merchants})
        merchants_count_key = BIG_SCREEN_STATISTICS_MERCHANTS_COUNT.format(**{"merchants": merchants})
        merchants_refund_key = BIG_SCREEN_STATISTICS_MERCHANTS_REFUND.format(**{"merchants": merchants})
        merchants_refund_count_key = BIG_SCREEN_STATISTICS_MERCHANTS_REFUND_COUNT.format(**{"merchants": merchants})
        merchants_amount = dao_session.redis_session.r.hgetall(merchants_key)
        merchants_amount_count = dao_session.redis_session.r.hgetall(merchants_count_key)
        merchants_amount_refund = dao_session.redis_session.r.hgetall(merchants_refund_key)
        merchants_amount_refund_count = dao_session.redis_session.r.hgetall(merchants_refund_count_key)
        cost_total, cost_refund_total, count_total, count_refund_total = 0, 0, 0, 0
        if self.op_area_ids:
            for op in self.op_area_ids:
                cost_total += float(merchants_amount.get(str(op), 0))
                count_total += int(merchants_amount_count.get(str(op), 0))
                cost_refund_total += float(merchants_amount_refund.get(str(op), 0))
                count_refund_total += int(merchants_amount_refund_count.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0", None):
                    cost_total += float(v)
            for k, v in merchants_amount_count.items():
                if k not in ("0", None):
                    count_total += int(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0", None):
                    cost_refund_total += float(v)
            for k, v in merchants_amount_refund_count.items():
                if k not in ("0", None):
                    count_refund_total += int(v)
        return abs(cost_total), count_total, abs(cost_refund_total), count_refund_total

    def query_revenue_deposit_history(self):
        """营收统计-押金读取缓存数据"""
        buy_history, refund_history = 0, 0
        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)
        return abs(buy_history), abs(refund_history)

    def query_revenue_wallet_history(self):
        """营收统计-钱包读取缓存数据）"""
        buy_history, refund_history = 0, 0
        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_WALLET_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)
        return abs(buy_history), abs(refund_history)

    def query_revenue_deposit_card_history(self):
        """营收统计押金卡读取缓存数据"""
        buy_history, refund_history = 0, 0
        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.ALI_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.WX_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)

        merchants_amount = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        merchants_amount_refund = dao_session.redis_session.r.hgetall(
            BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_REFUND.format(**{"merchants": MERCHANTS_PAY.UNION_PAY.value}))
        if self.op_area_ids:
            for op in self.op_area_ids:
                buy_history += float(merchants_amount.get(str(op), 0))
                refund_history += float(merchants_amount_refund.get(str(op), 0))
        else:
            for k, v in merchants_amount.items():
                if k not in ("0",):
                    buy_history += float(v)
            for k, v in merchants_amount_refund.items():
                if k not in ("0",):
                    refund_history += float(v)
        return abs(buy_history), abs(refund_history)

    def query_user_auth(self):
        """
        运营大屏- 用户旭日图
        :param op_area_ids:服务器id
        :param auth: 是否实名用户（1，实名。2，非实名）
        :return:
        """
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 1)
        if self.op_area_ids:
            authentication = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            authentication = query.first().count
        return authentication

    def query_user_not_auth(self):
        """
        运营大屏- 用户旭日图
        :param op_area_ids:服务器id
        :return:
        """
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 0)
        if self.op_area_ids:
            authentication = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            authentication = query.first().count
        return authentication

    def query_user_deposit_card_history(self):
        """ 历史会员卡会员"""

        # 历史会员卡会员
        historical_member = dao_session.sub_session().query(
            func.count(func.distinct(XcEbikeAccount2.objectId)).label("count")
        ).join(XcEbikeUsrs2, XcEbikeAccount2.objectId == XcEbikeUsrs2.id). \
            filter(XcEbikeAccount2.type == 9,
                   XcEbikeUsrs2.serviceId.in_(self.op_area_ids),
                   XcEbikeUsrs2.deposited == 0,
                   XcEbikeUsrs2.authed == 1
                   ).first().count

        return historical_member

    def query_user_deposit(self):
        """会员用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.deposited == 1, XcEbikeUsrs2.authed == 1)
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_not_deposit(self):
        """非会员用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.deposited == 0, XcEbikeUsrs2.authed == 1)
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_invalid_deposit(self):
        """待失效会员用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 1,
                   XcEbikeUsrs2.deposited == 1,
                   XcEbikeUsrs2.haveDepositCard == 1,
                   XcEbikeUsrs2.depositCardExpiredDate < func.date_format(
                       func.now(), "%Y-%m-%d %H:%i:%S"),
                   )
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_student(self):
        """学生认证用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 1,
                   XcEbikeUsrs2.deposited == 1,
                   XcEbikeUsrs2.student == 1
                   )
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_deposit_card(self):
        """会员卡用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 1,
                   XcEbikeUsrs2.deposited == 1,
                   XcEbikeUsrs2.haveDepositCard == 1,
                   XcEbikeUsrs2.depositCardExpiredDate > func.date_format(
                       func.now(), "%Y-%m-%d %H:%i:%S")
                   )
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_deposit_money(self):
        """会员费用户"""
        query = dao_session.sub_session().query(func.count(XcEbikeUsrs2.id).label("count")). \
            filter(XcEbikeUsrs2.authed == 1,
                   XcEbikeUsrs2.deposited == 1,
                   XcEbikeUsrs2.depositedMount > 0
                   )
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_riding_card_old_times(self, is_giving=0):
        """
        骑行卡老版售卖(赠送)次数
        is_giving: 1:赠送，0：购买
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.type). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_old_riding_card_buy()))
        if is_giving:
            money_times = money_times.filter(
                XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        else:
            money_times = money_times.filter(
                XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money_times = money_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        today_times = money_times.filter(XcEbike2RidingcardAccount.createdAt >= self.today_zero). \
            group_by(XcEbike2RidingcardAccount.type, XcEbike2RidingcardAccount.money).all()
        total_times = money_times.group_by(XcEbike2RidingcardAccount.type, XcEbike2RidingcardAccount.money).all()
        total_list = []
        for m in total_times:
            r_count, r_money, r_type = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}天骑行卡".format(
                    '%.2f' % (float(r_money) / 100), SERIAL_TYPE.riding_card_type_days().get(str(r_type), 0))}
                total_list.append(m_dict)
        today_list = []
        for m in today_times:
            r_count, r_money, r_type = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}天骑行卡".format(
                    '%.2f' % (float(r_money) / 100), SERIAL_TYPE.riding_card_type_days().get(str(r_type), 0))}
                today_list.append(m_dict)
        return total_list, today_list

    def query_riding_card_new_times(self, is_giving=0):
        """
        骑行卡新版购买（赠送）次数
        新增骑行卡次卡，增加了每种骑行卡的次数
        is_giving: 1:赠送，0：购买
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money,
            XcEbike2RidingcardAccount.ridingCardName). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_new_riding_card_buy()))
        if is_giving:
            money_times = money_times.filter(
                XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        else:
            money_times = money_times.filter(
                XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money_times = money_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        today_times = money_times.filter(XcEbike2RidingcardAccount.createdAt >= self.today_zero). \
            group_by(XcEbike2RidingcardAccount.ridingCardName, XcEbike2RidingcardAccount.money).all()
        total_times = money_times.group_by(XcEbike2RidingcardAccount.ridingCardName,
                                           XcEbike2RidingcardAccount.money).all()
        total_list = []
        for m in total_times:
            r_count, r_money, r_name = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}".format('%.2f' % (float(r_money) / 100), r_name)}
                total_list.append(m_dict)
        today_list = []
        for m in today_times:
            r_count, r_money, r_name = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}".format('%.2f' % (float(r_money) / 100), r_name)}
                today_list.append(m_dict)
        return total_list, today_list


class OperationScreenService(ScreenService):

    @staticmethod
    def acquire_imei_to_redis(service_id_list):
        """
        获取当前选择服务区下的所有车辆imei（xc_ebike_{service_id}_serviceGfence_deviceCount）
        service_id_list: 服务区id的列表
        return 该服务区下所有的车辆imei
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as imei_pipeline:
            for r in service_id_list:
                imei_pipeline.smembers(IMEI_LIST.format(**{"service_id": r}))
            imei_res = imei_pipeline.execute()
        imei = set()
        for i in imei_res:
            imei = imei.union(i)
        return list(imei)

    @staticmethod
    def acquire_device_info_to_redis(imei_list):
        """
        获取当前选择imei下的所有车辆的详细信息（xc_ebike_device_info_{imei}）
        imei_list: imei的列表
        return 所有的imei的车辆状态信息
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as device_pipeline:
            for i in imei_list:
                device_pipeline.hgetall(DEVICE_INFO.format(**{"imei": i}))
            device_list = device_pipeline.execute()
        return device_list

    @staticmethod
    def acquire_car_id_to_redis(imei_list):
        """
        获取当前选择imei下的车辆号（xc_ebike_imeiCarBindings_{imei}）
        imei_list: imei的列表
        return 所有的imei的车辆状态信息
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as car_pipeline:
            for i in imei_list:
                car_pipeline.get(CAR_BINDING.format(**{"imei": i}))
            car_res = car_pipeline.execute()
        return car_res

    @staticmethod
    def acquire_battery_name_to_redis(car_id_list):
        """
        获取当前选择imei下的所有车辆的详细信息（xc_ebike_xc_battery_Name_{imei}）
        imei_list: imei的列表
        return 所有的imei的车辆状态信息
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as battery_name:
            for i in car_id_list:
                battery_name.get(BATTERY_NAME.format(**{"car_id": i}))
            car_res = battery_name.execute()
        return car_res

    @staticmethod
    def acquire_fix_device_to_redis(service_id_list):
        """
        获取当前服务区下的所有保修车辆
        service_id_list: 服务区id的列表
        return 该服务区下维修工单的imei
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as fix_device:
            for r in service_id_list:
                fix_device.smembers(FIX_DEVICE.format(**{"service_id": r}))
            imei_res = fix_device.execute()
        imei = set()
        for i in imei_res:
            imei = imei.union(i)
        return list(imei)

    @staticmethod
    def acquire_alarm_type_device_to_redis(service_id_list, alarm_type):
        """
        获取当前服务区下的所有是否在异常工单中
        service_id_list: 服务区id的列表
        return 该服务区下某个异常工单的imei列表
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as alarm_type_device:
            for r in service_id_list:
                alarm_type_device.smembers(ALARM_DEVICE.format(**{"service_id": r, "alarm_type": alarm_type}))
            imei_res = alarm_type_device.execute()
        imei = set()
        for i in imei_res:
            imei = imei.union(i)
        return list(imei)

    @staticmethod
    def acquire_in_no_parking_zone(service_id_list):
        """
        禁停区车辆列表
        service_id_list: 服务区id的列表
        return 多服务区下在禁停区下的车辆列表
        """
        no_parking = dao_session.sub_session().query(XcEbikeParking).filter(XcEbikeParking.noParkingId.is_(None))
        if len(service_id_list) == 1:
            no_parking = no_parking.filter(XcEbikeParking.serviceId == service_id_list[0])
        elif len(service_id_list) > 1:
            no_parking = no_parking.filter(XcEbikeParking.serviceId.in_(tuple(service_id_list)))
        no_parking = no_parking.all()
        imei_list = [n.imei for n in no_parking if n]
        return imei_list

    @staticmethod
    def acquire_no_parking_zone(service_id_list):
        """
        获取某个服务区没有停车区的车辆
        service_id_list: 服务区id的列表
        return 多服务区下在禁停区下的车辆列表
        """
        no_parking = dao_session.sub_session().query(XcEbikeParking).filter(XcEbikeParking.noParkingId.is_(None),
                                                                            XcEbikeParking.parkingId.is_(None))
        if len(service_id_list) == 1:
            no_parking = no_parking.filter(XcEbikeParking.serviceId == service_id_list[0])
        elif len(service_id_list) > 1:
            no_parking = no_parking.filter(XcEbikeParking.serviceId.in_(tuple(service_id_list)))
        no_parking = no_parking.all()
        imei_list = [n.imei for n in no_parking if n]
        return imei_list

    @staticmethod
    def acquire_state_type_device_to_redis(service_id_list, state_type):
        """
        获取当前服务区下的不同状态设备
        service_id_list: 服务区id的列表
        state_type: config:DEVICE_STATE
        return 该服务区下某个异常工单的imei列表
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as state_type_device:
            for r in service_id_list:
                state_type_device.smembers(STATE_DEVICE.format(**{"service_id": r, "state_type": state_type}))
            imei_res = state_type_device.execute()
        imei = set()
        for i in imei_res:
            imei = imei.union(i)
        return list(imei)

    @staticmethod
    def acquire_imei_user_id_to_redis(imei_list, user_state):
        """
        听过imei列表获取到user列表
        imei_list: 设备的列表
        user_state: 需要过滤的用户状态
        return 该状态下的用户数量
        """
        with dao_session.redis_session.r.pipeline(transaction=False) as imei_user:
            for imei in imei_list:
                imei_user.smembers(IMEI_USER.format(**{"imei": imei}))
            user_list = imei_user.execute()
        user = set()
        for i in user_list:
            user = user.union(i)
        with dao_session.redis_session.r.pipeline(transaction=False) as user_info:
            for u in user:
                user_info.smembers(IMEI_USER.format(**{"imei": u}))
            user_info_list = user_info.execute()
        state, user_num = set(), 0
        for u in user_info_list:
            state = state.union(u)
        for s in state:
            if s == user_state:
                user_num += 1
        return user_num

    # @staticmethod
    # def acquire_move_car_device_to_redis(imei_list):
    #     """
    #     获取挪车，调度中的数量
    #     service_id_list: 设备id列表
    #     return 获取挪车，调度中的数量
    #     """
    #     #  MOVE_CAR_STATE结构错误
    #     with dao_session.redis_session.r.pipeline(transaction=False) as imei_move:
    #         for r in imei_list:
    #             imei_move.smembers(MOVE_CAR_STATE.format(**{"imei": r}))
    #         imei_list = imei_move.execute()
    #     imei = set()
    #     for i in imei_list:
    #         imei = imei.union(i)
    #     return len(imei)


class BigScreenOperationScreenService(OperationScreenService):

    def acquire_car_info_agg_by_hours(self):
        """
        获取当前服务区下的所有保修车辆
        service_id_list: 服务区id的列表
        return 该服务区下维修工单的imei
        """

        if self.op_area_ids:
            op_area_ids = self.op_area_ids
        else:
            op_area_ids = self.acquire_all_service_id()

        hours_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
        car_agg_list = {}
        for hour in hours_list:
            # 运营车辆(可租用， 骑行，被预约， 临时停车)
            car_rent, riding, reserved, temporary_park = 0, 0, 0, 0
            # 巡检车辆（保修，拖回，低电量，换电中，站点外，被占用，调度中）
            car_fix, drag_back, low_power, change_power, off_site, occupied, scheduling = 0, 0, 0, 0, 0, 0, 0
            # 运营警示（电瓶移除，移动异常，出服务区，离线，有单无程，一日无单，报失，订单超长，禁停区）
            operation_warning = 0
            # battery_remove, abnormal_move, out_of_service, off_line, there_no_way, no_order, report_loss, too_long_order, \
            # no_stop_zone = 0, 0, 0, 0, 0, 0, 0, 0, 0
            # 空闲情况(1 - 3h, 3 - 6h, 6 - 12h, 12 - 24h, 24 - 48h, 48h以上)
            one_three, three_six, six_twelve, twelve_twenty_four, twenty_four_forty_eight, forty_eight_up = 0, 0, 0, 0, 0, 0
            # 电量情况(0%-20%, 0%, 20%-35%, 35%以上)
            zero, zero_twenty, twenty_thirty_five, twenty_five_up = 0, 0, 0, 0
            car_info = dao_session.redis_session.r.hgetall(CAR_INFO_DATA.format(**{"hour": hour}))
            for s in op_area_ids:
                car = car_info.get(str(s), '')
                if car:
                    car_dict = json.loads(car, encoding="utf8")
                    car_state_data = car_dict.get("carStateData", {})
                    car_rent += int(car_state_data.get("可租用", 0))
                    riding += int(car_state_data.get("骑行", 0))
                    reserved += int(car_state_data.get("被预约", 0))
                    temporary_park += int(car_state_data.get("临时停车", 0))
                    car_fix += int(car_state_data.get("保修", 0))
                    drag_back += int(car_state_data.get("拖回", 0))
                    low_power += int(car_state_data.get("低电量", 0))
                    change_power += int(car_state_data.get("换电中", 0))
                    off_site += int(car_state_data.get("站点外", 0))
                    occupied += int(car_state_data.get("被占用", 0))
                    scheduling += int(car_state_data.get("调度中", 0))
                    # battery_remove += int(car_state_data.get("电瓶移除", 0))
                    # abnormal_move += int(car_state_data.get("移动异常", 0))
                    # out_of_service += int(car_state_data.get("出服务区", 0))
                    # off_line += int(car_state_data.get("离线", 0))
                    # there_no_way += int(car_state_data.get("有单无程", 0))
                    # no_order += int(car_state_data.get("一日无单", 0))
                    # report_loss += int(car_state_data.get("报失", 0))
                    # too_long_order += int(car_state_data.get("订单超长", 0))
                    # no_stop_zone += int(car_state_data.get("禁停区", 0))
                    operation_warning += int(car_state_data.get("运营警示", 0))

                    car_free_time_data = car_dict.get("carFreeTimeData", {})
                    one_three += int(car_free_time_data.get("1 - 3h", 0))
                    three_six += int(car_free_time_data.get("3 - 6h", 0))
                    six_twelve += int(car_free_time_data.get("6 - 12h", 0))
                    twelve_twenty_four += int(car_free_time_data.get("12 - 24h", 0))
                    twenty_four_forty_eight += int(car_free_time_data.get("24 - 48h", 0))
                    forty_eight_up += int(car_free_time_data.get("48h以上", 0))

                    car_voltage_data = car_dict.get("carVoltageData", {})
                    zero += int(car_voltage_data.get("0%", 0))
                    zero_twenty += int(car_voltage_data.get("0%-20%", 0))
                    twenty_thirty_five += int(car_voltage_data.get("20%-35%", 0))
                    twenty_five_up += int(car_voltage_data.get("35%以上", 0))
            car_agg_dict = {
                "car_state_data": {
                    "car_rent": car_rent,
                    "riding": riding,
                    "patrol": car_fix + low_power + change_power + off_site + occupied + scheduling,
                    "operation_warning": operation_warning,
                    "drag_back": drag_back
                },
                "car_free_time_data": {
                    "1-3h": one_three,
                    "3-6h": three_six,
                    "6-12h": six_twelve,
                    "12-24h": twelve_twenty_four,
                    "24-48h": twenty_four_forty_eight,
                    "48h以上": forty_eight_up,
                },
                "car_voltage_data": {
                    "0%": zero,
                    "0%-20%": zero_twenty,
                    "20%-35%": twenty_thirty_five,
                    "35%以上": twenty_five_up,
                }}
            car_agg_list[str(hour)] = car_agg_dict
        car_agg_dict_m = {
            "car_state_data": {
                "car_rent": 0,
                "riding": 0,
                "patrol": 0,
                "operation_warning": 0,
                "drag_back": 0
            },
            "car_free_time_data": {
                "1-3h": 0,
                "3-6h": 0,
                "6-12h": 0,
                "12-24h": 0,
                "24-48h": 0,
                "48h以上": 0,
            },
            "car_voltage_data": {
                "0%": 0,
                "0%-20%": 0,
                "20%-35%": 0,
                "35%以上": 0,
            }}
        now_time = datetime.now()
        now_hour = now_time.hour
        now_time_list = []
        now_n = 0
        while now_n < 24:
            now_hour += 1
            if now_hour == 24:
                now_hour = 0
            now_time_list.append(
                {"date": "{}:00".format(now_hour), "car_info": car_agg_list.get(str(now_hour), car_agg_dict_m)})
            now_n += 1
        return {"car_data": now_time_list}
