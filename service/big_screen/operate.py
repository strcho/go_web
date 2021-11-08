import json
from model.all_model import *
from mbutils import dao_session
from utils.constant.redis_key import *
from . import ScreenService


class BigScreenTimeService(ScreenService):

    def __init__(self, valid_data, op_area_ids: tuple):
        super().__init__(op_area_ids)
        start_time, end_time = valid_data
        #  s_time为开始时间，e_time为结束时间
        self.s_time, self.e_time = self.millisecond2datetime(start_time), self.millisecond2datetime(end_time)

    def query_order_statistics(self):
        """订单数量和金额"""
        order_num = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_num), 0))
        if self.op_area_ids:
            order_num = order_num.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        order_num = order_num.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time)).first()
        return order_num

    def query_operate_statistics(self):
        """运营大屏缓存的相关数据"""
        operate_num = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenOperate.change_battery), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.fix_tickets), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.alarm_tickets), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.move_operation), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.duration_sum), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.itinerary_sum), 0),
        )
        if self.op_area_ids:
            operate_num = operate_num.filter(XcMieba2BigScreenOperate.service_id.in_(self.op_area_ids))
        operate_num = operate_num.filter(
            XcMieba2BigScreenOperate.day_time.between(self.s_time, self.e_time)).first()
        return operate_num

    def query_earning_order_operate(self):
        """订单的类型"""
        earning_order = dao_session.sub_session().query(
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.free_order), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.riding_card_order), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.discount_order), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.no_discount_order), 0) / 100). \
            filter(XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time))
        if self.op_area_ids:
            earning_order = earning_order.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        earning_order = earning_order.group_by("ctime").all()
        earning_dict = {}
        for dt, fo, ro, do, no in earning_order:
            if dt and dt not in earning_dict.keys():
                earning_dict[dt] = {"free": float(fo), "riding_card": float(ro),
                                    "discount": float(do), "not_discount": float(no)}
        return earning_dict

    def query_ticket_order(self):
        """异议工单"""
        ticket_order = dao_session.sub_session().query(
            func.ifnull(func.count(XcEbikeUserTicket.ticketNo), 0),
            func.date_format(XcEbikeUserTicket.createdAt, "%Y-%m-%d").label("ctime")). \
            filter(XcEbikeUserTicket.createdAt.between(self.s_time, self.e_time))
        if self.op_area_ids:
            ticket_order = ticket_order.filter(XcEbikeUserTicket.serviceId.in_(self.op_area_ids))
        ticket_order = ticket_order.group_by("ctime").all()
        ticket_dict = {}
        for on, dt in ticket_order:
            if dt and dt not in ticket_dict.keys():
                ticket_dict[dt] = {"ticket": float(on)}
        return ticket_dict

    def query_order_statistics_date(self):
        """订单数量和金额,按时间聚合"""
        order_num = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_cost), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenRevenue.order_num), 0),
            func.date_format(XcMieba2BigScreenRevenue.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            order_num = order_num.filter(XcMieba2BigScreenRevenue.service_id.in_(self.op_area_ids))
        order_num = order_num.filter(
            XcMieba2BigScreenRevenue.day_time.between(self.s_time, self.e_time)).group_by("ctime").all()
        order_dict = {}
        for oa, on, dt in order_num:
            if dt and dt not in order_dict.keys():
                order_dict[dt] = {"num": int(on), "amount": float(oa)}
        return order_dict

    def query_user_statistics_date(self):
        """统计用户的数量"""
        user_num = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenOperate.new_user), 0),
            func.ifnull(func.sum(XcMieba2BigScreenOperate.active_user), 0),
            func.date_format(XcMieba2BigScreenOperate.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            user_num = user_num.filter(XcMieba2BigScreenOperate.service_id.in_(self.op_area_ids))
        user_num = user_num.filter(
            XcMieba2BigScreenOperate.day_time.between(self.s_time, self.e_time)).group_by("ctime").all()
        order_dict = {}
        for new, active, dt in user_num:
            if dt and dt not in order_dict.keys():
                order_dict[dt] = {"new": int(new), "active": float(active)}
        return order_dict

    def query_new_user_statistics(self):
        """统计用户的数量"""
        user_num = dao_session.sub_session().query(
            func.ifnull(func.count(XcEbikeUsrs2.id), 0))
        if self.op_area_ids:
            user_num = user_num.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        user_num = user_num.filter(
            XcEbikeUsrs2.createdAt < self.e_time).first()
        return int(user_num[0])

    def query_new_user_num(self):
        """统计新增用户的数量"""
        user_num = dao_session.sub_session().query(
            func.ifnull(func.count(XcEbikeUsrs2.id), 0))
        if self.op_area_ids:
            user_num = user_num.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        user_num = user_num.filter(
            XcEbikeUsrs2.createdAt.between(self.s_time, self.e_time)).first()
        return user_num[0]

    def query_user_num_by_time(self):
        """统计聚合每天新增用户的数量"""
        user_num = dao_session.sub_session().query(
            func.ifnull(func.count(XcEbikeUsrs2.id), 0),
            func.date_format(XcEbikeUsrs2.createdAt, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            user_num = user_num.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        user_num = user_num.filter(
            XcEbikeUsrs2.createdAt.between(self.s_time, self.e_time)).group_by("ctime").all()
        user_dict = {}
        for num, dat in user_num:
            user_dict[str(dat)] = int(num)
        return user_dict

    def query_active_user_num(self):
        """统计活跃用户的数量"""
        user_num = dao_session.sub_session().query(
            func.ifnull(func.count(func.distinct(XcEbikeUserOrder.userId)), 0))
        if self.op_area_ids:
            user_num = user_num.filter(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        user_num = user_num.filter(
            XcEbikeUserOrder.createdAt.between(self.s_time, self.e_time)).first()
        return user_num[0]

    def query_operate_order_time(self):
        """从缓存获取数量, 普通订单（2），超时订单（3），短时订单（1）"""
        filters = set()
        filters.add(XcMieba2DailyOrderAnalysis.created_at.between(self.s_time, self.e_time))
        if self.op_area_ids:
            filters.add(XcMieba2DailyOrderAnalysis.service_id.in_(self.op_area_ids))

        daily_order_query = dao_session.sub_session().query(
            func.date(XcMieba2DailyOrderAnalysis.created_at).label("date"),
            func.ifnull(func.sum(XcMieba2DailyOrderAnalysis.total), 0).label("total"),
            func.ifnull(func.sum(XcMieba2DailyOrderAnalysis.cost) / 100, 0).label("cost"),
            XcMieba2DailyOrderAnalysis.duration_type.label("d_type")
        ).filter(*filters)
        # 普通订单个数,总额
        before_order = daily_order_query.group_by(
            func.date(XcMieba2DailyOrderAnalysis.created_at), XcMieba2DailyOrderAnalysis.duration_type).all()
        general_order_result, long_order_time_result, short_order_time_result, total_result = {}, {}, {}, {}
        general_order_count, long_order_time_count, short_order_time_count, total_count = 0, 0, 0, 0
        general_order_cost, long_order_time_cost, short_order_time_cost, total_cost = 0, 0, 0, 0
        for r in before_order:
            date = r[0].strftime('%Y-%m-%d')
            order_count, order_cost, duration_type = int(r[1]), round(float(r[2]), 2) if r[2] else 0, int(r[3])
            if duration_type == 2:
                general_order_result.setdefault(date, {})["count"] = order_count
                general_order_result.setdefault(date, {})["cost"] = order_cost
                general_order_count += order_count
                general_order_cost += order_cost
            elif duration_type == 3:
                long_order_time_result.setdefault(date, {})["count"] = order_count
                long_order_time_result.setdefault(date, {})["cost"] = order_cost
                long_order_time_count += order_count
                long_order_time_cost += order_cost
            else:
                short_order_time_result.setdefault(date, {})["count"] = order_count
                short_order_time_result.setdefault(date, {})["cost"] = order_cost
                short_order_time_count += order_count
                short_order_time_cost += order_cost
            total_result.setdefault(date, {})["count"] = order_count
            total_result.setdefault(date, {})["cost"] = order_cost
            total_count += order_count
            total_cost += order_cost
        return (general_order_result, general_order_count, general_order_cost), \
               (long_order_time_result, long_order_time_count, long_order_time_cost), \
               (short_order_time_result, short_order_time_count, short_order_time_cost), \
               (total_result, total_count, total_cost)

    def query_operate_order_distance(self):
        """正常订单（0），站点外订单（1），服务区外订单（2），禁停区订单（3）"""
        order_filters = set()
        order_filters.add(XcEbikeUserOrder.createdAt >= self.s_time)
        order_filters.add(XcEbikeUserOrder.createdAt <= self.e_time)
        if self.op_area_ids:
            order_filters.add(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        order_analysis_query = dao_session.sub_session().query(
            func.date(XcEbikeUserOrder.createdAt).label("date"),
            func.count(XcEbikeUserOrder.orderId).label("count"),
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0).label("cost"),
            XcEbike2OrderAnalysis.type
        ).join(XcEbike2OrderAnalysis, XcEbikeUserOrder.orderId == XcEbike2OrderAnalysis.orderId).filter(*order_filters)

        # 正常订单个数,总额
        before_order = order_analysis_query.group_by(
            func.date(XcEbikeUserOrder.createdAt), XcEbike2OrderAnalysis.type).all()

        before_is_in_no_parking_zone_result, before_is_out_of_service_zone_result, before_is_parking_zone_result, before_normal_order_result = {}, {}, {}, {}
        before_is_in_no_parking_zone_count, before_is_out_of_service_zone_count, before_is_parking_zone_count, before_normal_order_count = 0, 0, 0, 0
        before_is_in_no_parking_zone_cost, before_is_out_of_service_zone_cost, before_is_parking_zone_cost, before_normal_order_cost = 0, 0, 0, 0
        for r in before_order:
            date = r[0].strftime('%Y-%m-%d')
            count, cost, o_type = int(r[1]), round(float(r[2]), 2) if r[2] else 0, int(r[3])
            if o_type == 3:
                before_is_in_no_parking_zone_result.setdefault(date, {})["count"] = count
                before_is_in_no_parking_zone_result.setdefault(date, {})["cost"] = cost
                before_is_in_no_parking_zone_count += count
                before_is_in_no_parking_zone_cost += cost
            elif o_type == 2:
                before_is_out_of_service_zone_result.setdefault(date, {})["count"] = count
                before_is_out_of_service_zone_result.setdefault(date, {})["cost"] = cost
                before_is_out_of_service_zone_count += count
                before_is_out_of_service_zone_cost += cost
            elif o_type == 1:
                before_is_parking_zone_result.setdefault(date, {})["count"] = count
                before_is_parking_zone_result.setdefault(date, {})["cost"] = cost
                before_is_parking_zone_count += count
                before_is_parking_zone_cost += cost
            elif o_type == 0:
                before_normal_order_result.setdefault(date, {})["count"] = count
                before_normal_order_result.setdefault(date, {})["cost"] = cost
                before_normal_order_count += count
                before_normal_order_cost += cost

        return (before_normal_order_result, before_normal_order_count, before_normal_order_cost), \
               (before_is_parking_zone_result, before_is_parking_zone_count, before_is_parking_zone_cost), \
               (before_is_out_of_service_zone_result, before_is_out_of_service_zone_count,
                before_is_out_of_service_zone_cost), \
               (before_is_in_no_parking_zone_result, before_is_in_no_parking_zone_count,
                before_is_in_no_parking_zone_cost)

    def query_operate_car_number(self):
        """历史车辆数的统计查询"""
        # 折线图总车辆信息
        car_filters = set()
        car_filters.add(XcMieba2CarAnalysis.created_at.between(self.s_time, self.e_time))
        if self.op_area_ids:
            car_filters.add(XcMieba2CarAnalysis.service_id.in_(self.op_area_ids))
        car = dao_session.session().query(
            func.date(XcMieba2CarAnalysis.created_at).label("date"),
            func.ifnull(func.sum(XcMieba2CarAnalysis.total), 0).label("total")
        ).filter(*car_filters).group_by(func.date(XcMieba2CarAnalysis.created_at)).all()

        car_result = {}
        for r in car:
            date = r[0].strftime('%Y-%m-%d')
            car_result[date] = int(r[1])
        return car_result


class BigScreenTotalService(ScreenService):

    def query_user_auth(self):
        """
        运营大屏- 用户旭日图 是否实名用户（1，实名。2，非实名）
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
        运营大屏- 用户旭日图- 没有实名
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
                   XcEbikeUsrs2.student == 1,
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
                   XcEbikeUsrs2.haveDepositCard == 0,
                   XcEbikeUsrs2.depositedMount > 0
                   )
        if self.op_area_ids:
            member = query.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids)).first().count
        else:
            member = query.first().count
        return member

    def query_user_info(self):
        user_info_dict = dao_session.redis_session.r.hgetall(BIG_SCREEN_USER_INFO)
        auth_dict = json.loads(user_info_dict.get("auth_dict", '{}'))
        not_auth_dict = json.loads(user_info_dict.get("not_auth_dict", '{}'))
        deposit_card_history_dict = json.loads(user_info_dict.get("deposit_card_history_dict", '{}'))
        deposit_dict = json.loads(user_info_dict.get("deposit_dict", '{}'))
        not_deposit_dict = json.loads(user_info_dict.get("not_deposit_dict", '{}'))
        invalid_deposit_dict = json.loads(user_info_dict.get("invalid_deposit_dict", '{}'))
        student_dict = json.loads(user_info_dict.get("student_dict", '{}'))
        deposit_card_dict = json.loads(user_info_dict.get("deposit_card_dict", '{}'))
        deposit_money_dict = json.loads(user_info_dict.get("deposit_money_dict", '{}'))
        auth_total, not_auth_total, deposit_card_history_total, deposit_total, not_deposit_total, \
        invalid_deposit_total, student_total, deposit_card_total, deposit_money_total = 0, 0, 0, 0, 0, 0, 0, 0, 0
        if self.op_area_ids:
            for service_id in self.op_area_ids:
                service_id = str(service_id)
                auth_total += auth_dict.get(service_id, 0)
                not_auth_total += not_auth_dict.get(service_id, 0)
                deposit_card_history_total += deposit_card_history_dict.get(service_id, 0)
                deposit_total += deposit_dict.get(service_id, 0)
                not_deposit_total += not_deposit_dict.get(service_id, 0)
                invalid_deposit_total += invalid_deposit_dict.get(service_id, 0)
                student_total += student_dict.get(service_id, 0)
                deposit_card_total += deposit_card_dict.get(service_id, 0)
                deposit_money_total += deposit_money_dict.get(service_id, 0)
        else:
            auth_total = sum(auth_dict.values())
            not_auth_total = sum(not_auth_dict.values())
            deposit_card_history_total = sum(deposit_card_history_dict.values())
            deposit_total = sum(deposit_dict.values())
            not_deposit_total = sum(not_deposit_dict.values())
            invalid_deposit_total = sum(invalid_deposit_dict.values())
            student_total = sum(student_dict.values())
            deposit_card_total = sum(deposit_card_dict.values())
            deposit_money_total = sum(deposit_money_dict.values())
        return auth_total, not_auth_total, deposit_card_history_total, deposit_total, not_deposit_total, \
               invalid_deposit_total, student_total, deposit_card_total, deposit_money_total


class BigScreenOperationScreenService(ScreenService):

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

    def query_operate_free_order_earnings(self, start_time):
        """新用户免单"""
        if self.op_area_ids:
            op_area_ids = self.op_area_ids
        else:
            op_area_ids = self.acquire_all_service_id()
        order_data = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100). \
            filter(XcEbikeUserOrder.isFreeOrder == 1, XcEbikeUserOrder.serviceId.in_(op_area_ids),
                   XcEbikeUserOrder.createdAt >= start_time).first()
        return float(order_data[0])

    def query_operate_riding_card_order_earnings(self, start_time):
        """骑行卡优惠"""
        if self.op_area_ids:
            op_area_ids = self.op_area_ids
        else:
            op_area_ids = self.acquire_all_service_id()
        order_data = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100). \
            filter(XcEbikeUserOrder.isFreeOrder == 0, XcEbikeUserOrder.isUseRidingCard == 1,
                   XcEbikeUserOrder.serviceId.in_(op_area_ids),
                   XcEbikeUserOrder.createdAt >= start_time).first()
        return float(order_data[0])

    def query_operate_discount_order_earnings(self, start_time):
        """折扣优惠"""
        if self.op_area_ids:
            op_area_ids = self.op_area_ids
        else:
            op_area_ids = self.acquire_all_service_id()
        order_data = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100). \
            filter(XcEbikeUserOrder.isFreeOrder == 0, XcEbikeUserOrder.isUseRidingCard == 0,
                   XcEbikeUserOrder.discount < 1, XcEbikeUserOrder.serviceId.in_(op_area_ids),
                   XcEbikeUserOrder.createdAt >= start_time).first()
        return float(order_data[0])

    def query_operate_not_discount_order_earnings(self, start_time):
        """无优惠"""
        if self.op_area_ids:
            op_area_ids = self.op_area_ids
        else:
            op_area_ids = self.acquire_all_service_id()
        order_data = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserOrder.cost), 0) / 100). \
            filter(XcEbikeUserOrder.isFreeOrder == 0, XcEbikeUserOrder.isUseRidingCard == 0,
                   XcEbikeUserOrder.discount == 1, XcEbikeUserOrder.serviceId.in_(op_area_ids),
                   XcEbikeUserOrder.createdAt >= start_time).first()
        return float(order_data[0])

    def query_operate_order_time(self, valid_date, duration_type):
        """
        获取某个时间后的时长类型的订单数据
        start_time: 开始时间， duration_type：订单类型（1，短时，2，正常，3，超长）
        return: count, cost
        """
        start_time, end_time = valid_date
        filters = set()
        filters.add(XcEbikeUserOrder.createdAt >= start_time)
        filters.add(XcEbikeUserOrder.createdAt <= end_time)
        if self.op_area_ids:
            filters.add(XcEbikeUserOrder.serviceId.in_(self.op_area_ids))
        order_query = dao_session.sub_session().query(
            func.count(XcEbikeUserOrder.orderId).label("count"),
            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0).label("cost")
        ).join(XcEbikeDeviceItinerary, XcEbikeUserOrder.deviceItineraryId == XcEbikeDeviceItinerary.itinId).filter(
            *filters)
        if duration_type == 2:
            general_order = order_query.filter(func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                                                  XcEbikeDeviceItinerary.endTime).between(2, 120)).one()
            return int(general_order[0]), float(general_order[1])
        elif duration_type == 3:
            long_order = order_query.filter(func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                                               XcEbikeDeviceItinerary.endTime) > 120).one()
            return int(long_order[0]), float(long_order[1])
        elif duration_type == 1:
            short_order = order_query.filter(func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                                                XcEbikeDeviceItinerary.endTime) < 2).one()
            return int(short_order[0]), float(short_order[1])
        else:
            return 0, 0
