from model.all_model import *
from mbutils import dao_session, logger
from utils.constant.account import PAY_TYPE
from . import ScreenService


class BigScreenOrderService(ScreenService):

    # 根据不同的支付类型做区分，设计统一通过时间和服务区进行过滤
    def __init__(self, op_area_ids: tuple, start_time: datetime, end_time: datetime):
        """
        @param op_area_ids: 服务区id
        @param start_time: 开始时间（时间类型）
        @param end_time: 结束时间（时间类型）
        """
        super().__init__(op_area_ids)
        self.start_time = start_time
        self.end_time = end_time

    def acquire_order_data(self, order_type):
        """
        @param order_type: 查询类型（penalty:调度费，返回（调度费金额，）paid:支付的订单，返回：（支付金额，充值，赠送，数量）
        total:订单金额，返回：（金额，数量））
        @return: 元组类型(0, total, paid, present, recharge, num, paid_num, penalty)
        """
        order_data = {"total": 0, "paid": 0, "present": 0, "recharge": 0, "num": 0, "paid_num": 0, "penalty": 0}
        if order_type == "penalty":
            penalty = self.query_order_penalty()
            order_data["penalty"] = penalty
        elif order_type == "paid":
            paid = self.query_order_water_paid()
            order_data["paid"] = paid[0]
            order_data["present"] = paid[1]
            order_data["recharge"] = paid[2]
            order_data["paid_num"] = paid[3]
        elif order_type == "total":
            total = self.query_order_water_total()
            order_data["total"] = total[0]
            order_data["num"] = total[1]
        return order_data

    #  时间范围内的订单流水
    def query_order_water_total(self):
        """
        @return: 订单流水（金额，数量）
        """
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100,
            func.ifnull(func.count(XcEbikeUserOrder.cost), 0), )
        try:
            order_water = order_water.with_hint(XcEbikeUserOrder, "force index(createdAt)")
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserOrder.createdAt.between(self.start_time, self.end_time)).first()
        except Exception as e:
            logger.error("xc_ebike_user_orders not index createdAt, {}".format(e))
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserOrder.createdAt.between(self.start_time, self.end_time)).first()
        return round(order_water[0], 2), order_water[1]

    # 时间范围内的已支付的订单流水
    def query_order_water_paid(self):
        """
        @return: 订单金额，充值，赠送，数量
        """
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100,
            func.ifnull(func.sum(XcEbikeUserOrder.presentCost), 0) / 100,
            func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost), 0) / 100,
            func.ifnull(func.count(XcEbikeUserOrder.cost), 0),
            func.count(XcEbikeUserWalletRecord.id)). \
            prefix_with("STRAIGHT_JOIN"). \
            join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
        if self.op_area_ids:
            order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        order_water = order_water.filter(XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
                                         XcEbikeUserOrder.isPaid == 1,
                                         XcEbikeUserWalletRecord.type == 1).first()
        return round(order_water[0], 2), round(order_water[1], 2), round(order_water[2], 2), order_water[3]

    # 历史欠款补缴
    def query_historical_orders_payment(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0),
            func.count(XcEbikeUserWalletRecord.id)). \
            prefix_with("STRAIGHT_JOIN"). \
            join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserOrder.createdAt < self.start_time,
            XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
            XcEbikeUserWalletRecord.type == 1
        ).first()
        return {"history_paid_cost": pay_today[0], "history_recharge_cost": pay_today[1],
                "history_present_cost": pay_today[2]}

    # 新增实际付款订单
    def query_new_order_payment(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserOrder.presentCost) / 100, 0),
            func.count(XcEbikeUserWalletRecord.id)). \
            prefix_with("STRAIGHT_JOIN"). \
            join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserWalletRecord.orderId)
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserOrder.createdAt.between(self.start_time, self.end_time), XcEbikeUserOrder.isPaid == 1,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time), XcEbikeUserWalletRecord.type == 1
        ).first()

        return {"new_paid_cost": pay_today[0], "new_recharge_cost": pay_today[1], "new_present_cost": pay_today[2]}

    #  时间范围内的调度费
    def query_order_penalty(self):
        """
        @return: 订单调度费（金额，数量）
        """
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserOrder.penalty), 0) / 100)
        try:
            order_water = order_water.with_hint(XcEbikeUserOrder, "force index(createdAt)")
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserOrder.createdAt.between(self.start_time, self.end_time)).first()
        except Exception as e:
            logger.error("xc_ebike_user_orders not index createdAt, {}".format(e))
            if self.op_area_ids:
                order_water = order_water.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
            order_water = order_water.filter(XcEbikeUserOrder.createdAt.between(self.start_time, self.end_time)).first()
        return round(order_water[0], 2)

    # 获取订单相关的历史数据,已支付的订单流水,数量
    def query_order_water_history(self):
        """
        后期维护，往后面加参数
        @return: 按天聚合的数据（"total": total, "paid": paid, "present": present, "recharge": recharge, "num": num,
            "paid_num": paid_num, "penalty": penalty），(订单总金额，支付金额，支付充值金额，支付赠送金额，总数量，支付数量，调度费)
            两个字典
        """
        order_water = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_paid_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_present_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_recharge_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_paid_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.penalty), 0) / 100,
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            order_water = order_water.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        order_water = order_water.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.start_time, self.end_time)). \
            group_by("ctime").all()
        total_dict, total_data, paid_data, present_data, recharge_data, num_data, paid_num_data = {}, 0, 0, 0, 0, 0, 0
        penalty_data = 0
        for total, paid, present, recharge, num, paid_num, penalty, day_time in order_water:
            total_dict[day_time] = {"total": total, "paid": paid, "present": present, "recharge": recharge, "num": num,
                                    "paid_num": paid_num, "penalty": penalty}
            total_data += total
            paid_data += paid
            present_data += present
            recharge_data += recharge
            num_data += num
            paid_num_data += paid_num
            penalty_data += penalty
        data_dict = {"total": total_data, "paid": paid_data, "present": present_data, "recharge": recharge_data,
                     "num": num_data, "paid_num": paid_num_data, "penalty": penalty_data}
        return total_dict, data_dict

    # 获取历史的历史欠款补缴和新增实收的数据
    def query_order_historical_payment_history(self):
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
            XcMieba2BigScreenRevenue.day_time.between(self.start_time, self.end_time)). \
            group_by("ctime").all()
        history_dict, history_paid_total, new_paid_total, history_recharge_total, history_present_total, \
        new_recharge_total, new_present_total = {}, 0, 0, 0, 0, 0, 0
        for hc, nc, hr, hp, nr, np, ctime in history:
            history_dict[ctime] = {"history_paid_cost": hc, "new_paid_cost": nc, "history_recharge_cost": hr,
                                   "history_present_cost": hp, "new_recharge_cost": nr, "new_present_cost": np}
            history_paid_total += hc
            new_paid_total += nc
            history_recharge_total += hr
            history_present_total += hp
            new_recharge_total += nr
            new_present_total += np
        history_data = {"history_paid_cost": history_paid_total, "new_paid_cost": new_paid_total,
                        "history_recharge_cost": history_recharge_total, "history_present_cost": history_present_total,
                        "new_recharge_cost": new_recharge_total, "new_present_cost": new_present_total}
        return history_dict, history_data

    # 举报管理费（举报罚金）
    def query_orders_report_manage(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.presentChange) / 100, 0))
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserWalletRecord.type == PAY_TYPE.REPORT_PENALTY.value,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
        ).first()
        return {"cost": abs(pay_today[0]), "recharge_cost": abs(pay_today[1]), "present_cost": abs(pay_today[2])}

    # 举报管理费（举报罚金）
    def query_orders_report_manage_day(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.presentChange) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        select_order_pay = select_order_pay.filter(
            XcEbikeUserWalletRecord.type == PAY_TYPE.REPORT_PENALTY.value,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
        ).group_by("ctime").all()
        data_dict, cost_sum, recharge_sum, present_sum = {}, 0, 0, 0
        for c, r, p, d in select_order_pay:
            data_dict[d] = {"cost": abs(c), "recharge_cost": abs(r), "present_cost": abs(p)}
            cost_sum += abs(c)
            recharge_sum += abs(r)
            present_sum += abs(p)
        return data_dict, cost_sum, recharge_sum, present_sum

    # 用户工单退款
    def query_orders_refund(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.presentChange) / 100, 0))
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        pay_today = select_order_pay.filter(
            XcEbikeUserWalletRecord.type == PAY_TYPE.ITINERARY_REFOUND.value,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
        ).first()
        return {"cost": pay_today[0], "recharge_cost": pay_today[1], "present_cost": pay_today[2]}

    # 用户工单退款
    def query_orders_refund_day(self):
        """
        @return: 金额，充值，赠送
        """
        select_order_pay = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100, 0),
            func.ifnull(func.sum(XcEbikeUserWalletRecord.presentChange) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            select_order_pay = select_order_pay.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        select_order_pay = select_order_pay.filter(
            XcEbikeUserWalletRecord.type == PAY_TYPE.ITINERARY_REFOUND.value,
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
        ).group_by("ctime").all()
        data_dict, cost_sum, recharge_sum, present_sum = {}, 0, 0, 0
        for c, r, p, d in select_order_pay:
            data_dict[d] = {"cost": c, "recharge_cost": r, "present_cost": p}
            cost_sum += c
            recharge_sum += r
            present_sum += p
        return data_dict, cost_sum, recharge_sum, present_sum

