import datetime
import random
import operator
import time
from sqlalchemy import sql
from math import inf
# import scripts.dispatch.vhicle_routes2
from model.all_model import XcMieba2WorkmanConfig, XcMieba2Dispatch
from service import MBService
from mbutils import dao_session, logger, MbException
from mb3party.amap import Amap
from utils.constant.dispatch import ActualType, DispatchTaskType
from utils.constant.redis_key import DISPATCH_WORKMAN_START_LIST, DISPATCH_WORKMAN_TASK_NUM, DISPATCH_WORKMAN_CONFIG, SERVICE_ASSIGNED_WORKMANIDS
from utils.constant.task import VehicleType, T1, T2, K_CONFIG, T3


class DispatchTaskService(MBService):
    def __init__(self, one=None):
        if one:
            dispatch_id, service_id, ticket_id, dispatch_reason, imei, start_lat, start_lng, end_lat, end_lng, expect_expend_time, members, deadline, expect_achievement, is_force, origin_type, actual_type, created_at = one
            self.dispatch_id = dispatch_id
            self.start_lat = float(start_lat)
            self.start_lng = float(start_lng)
            self.id = dispatch_id
            self.deadline = deadline
            logger.info("one is : {}".format(one))
            self.end_lat = float(end_lat)
            self.end_lng = float(end_lng)
            # self.actual_type = 1
            self.actual_type = actual_type

    def get_task_duration(self, workman_id, origin, destination):
        """
        预测任务时长(挪车)
        :param workman_id:工人id
        :param origin:出发点经纬度 填入规则：X,Y，采用","分隔，例如“117.500244, 40.417801”
        :param destination:目的地经纬度 填入规则：X,Y，采用","分隔，例如“117.500244, 40.417801”
        :return:s 起终点的骑行距离(m)  ts 任务耗时(s)
        """

        # 获取单次最大挪车能力
        move_car_capacity_info = dao_session.session().query(XcMieba2WorkmanConfig.move_car_capacity).filter(
            XcMieba2WorkmanConfig.workman_id == workman_id).first()

        move_car_capacity = move_car_capacity_info.move_car_capacity if move_car_capacity_info else 1

        # 获取车型
        vehicle_type = VehicleType.get_vehicle_type(move_car_capacity)

        amp = Amap()
        if vehicle_type == "ELECTRIC":
            # 电动车
            s, ts = amp.do_direction(origin, destination, mode=1)
        elif vehicle_type == "TRICYCLE":
            # 三轮车
            s, ts = amp.do_direction(origin, destination, size=1, mode=2)
        elif vehicle_type == "TRUCK":
            # 小卡车
            s, ts = amp.do_direction(origin, destination, size=2, mode=2)
        else:
            s, ts = 0, 0
        t = T1 + ts + T2 + random.randint(1, 5) * 60
        return s, t

    def get_task_achievement(self, actual_type, s, n=1):
        """
        预测任务绩效
        :param actual_type:任务类型
        :param s:行驶里程(km)
        :param n:车辆数
        :return:achievement 预测收益
        """

        if actual_type == ActualType.CHANGE_BATTERY.value:
            achievement = K_CONFIG.get("CHANGE_BATTERY") * 1
        elif actual_type == ActualType.FIX.value:
            achievement = K_CONFIG.get("FIX") * 1
        elif actual_type == ActualType.MOVE_CAR.value:
            # 单台挪车
            if s < 1:
                achievement = K_CONFIG.get("MOVE").get("SINGLE") * 1
            else:
                achievement = K_CONFIG.get("MOVE").get("SINGLE") * (1 + (s - 1) / 2)
        elif actual_type == ActualType.INSPECT.value:
            achievement = K_CONFIG.get("INSPECT") * 1
        else:
            achievement = 0

        achievement = round(achievement + random.randint(1, 20) / 100, 2)

        return achievement

    def cal_actual_achievement(self, achievement, finish_time, deadline):
        """
        计算实际绩效
        :param achievement: 预测绩效
        :param finish_time: 完成时间(datetime)
        :param deadline: 截止时间(datetime)
        :return:
        """
        actual_achievement = 0
        if finish_time.__le__(deadline):
            actual_achievement = achievement
        if finish_time.__ge__(deadline + datetime.timedelta(seconds=+T3)):
            actual_achievement = 0
        if finish_time.__gt__(deadline) and finish_time.__lt__(deadline + datetime.timedelta(seconds=+T3)):
            diff_timedelta = finish_time - deadline
            diff_seconds = diff_timedelta.days * 24 * 3600 + diff_timedelta.seconds
            actual_achievement = (1 - diff_seconds / T3) * achievement
        return actual_achievement

    def best_match_workman(self, workman_list, start_lat, start_lng):
        """
        从工人列表找最合适的
        :param workman_list:
        :param start_lat:
        :param start_lng:
        :return:
        """
        # 获取开工工人的任务数  xc_mieba_task_num_{workman_id} SET
        with dao_session.redis_session.r.pipeline(transaction=False) as workman_pipeline:
            for s in workman_list:
                workman_pipeline.get(DISPATCH_WORKMAN_TASK_NUM.format(**{"workman_id": s}))
            task_num_res = workman_pipeline.execute()
        logger.info(task_num_res)
        # 开工工人有任务的
        start_task_num = [v for k, v in enumerate(workman_list) if task_num_res[k]]
        logger.info(start_task_num)
        # 开工工人无任务的
        start_without_task_num = [v for k, v in enumerate(workman_list) if not task_num_res[k]]
        logger.info(start_without_task_num)
        if start_without_task_num:
            # 开工无任务里选择p值低的(p为任务起点距离工人的距离)
            p = []
            for r in start_without_task_num:
                # 工人当前位置
                workman_lat = dao_session.redis_session.r.hget(
                    DISPATCH_WORKMAN_CONFIG.format(workman_id=r), "lat")
                workman_lng = dao_session.redis_session.r.hget(
                    DISPATCH_WORKMAN_CONFIG.format(workman_id=r), "lng")
                origin = "{},{}".format(workman_lat, workman_lng)
                # 任务起点位置
                destination = "{},{}".format(start_lat, start_lng)
                s, t = self.get_task_duration(r, origin, destination)
                p.append(s)

            # 找出最小的p值
            min_index, min_number = min(enumerate(p), key=operator.itemgetter(1))
            assign_workman_id = start_without_task_num[min_index]
            return assign_workman_id
        else:
            # TODO 开工有任务列表进行筛选(最优路径,做路径规划,比较时间)，满足时间的比较q值,q值小的
            pass

    def assign_workman(self, dispatch_id, service_id, members, start_lat, start_lng):
        """
        分配任务到指定工人(任务数+最优路径)
        :param dispatch_id:
        :param service_id:服务区id
        :param members:13590200801;13590200801
        :param start_lat: 起点lat
        :param start_lng: 起点lnt
        :return:
        """
        if not members:
            # 没有分配的,从开工列表找,获取当前服务区的开工列表
            workman_start_set = dao_session.redis_session.r.smembers(
                DISPATCH_WORKMAN_START_LIST.format(**{"service_id": service_id}))
            if not workman_start_set:
                return False
            # 从当前服务区的开工列表找最合适的
            self.best_match_workman(workman_start_set, start_lat, start_lng)
        else:
            members_list = members.split(';')
            # 查询members下的开工状态
            with dao_session.redis_session.r.pipeline(transaction=False) as start_pipeline:
                for m in members_list:
                    start_pipeline.sismember(DISPATCH_WORKMAN_START_LIST.format(**{"service_id": service_id}), m)
                start_res = start_pipeline.execute()
            if True not in start_res:
                # members下无开工状态的工人
                # TODO 从当前服务区的开工列表找最合适的(优先给无任务的,无任务的按照p值排序)
                pass
            else:
                # members下有开工状态的工人
                members_start_list = [v for k, v in enumerate(members_list) if start_res[k]]
                # 获取members开工工人的任务数  xc_mieba_task_num_{workman_id} SET
                with dao_session.redis_session.r.pipeline(transaction=False) as workman_pipeline:
                    for s in members_start_list:
                        workman_pipeline.get(DISPATCH_WORKMAN_TASK_NUM.format(**{"workman_id": s}))
                    task_num_res = workman_pipeline.execute()
                logger.info(task_num_res)
                # members开工工人有任务的
                members_start_task_num = [v for k, v in enumerate(members_start_list) if task_num_res[k]]
                logger.info(members_start_task_num)
                # members开工工人无任务的
                members_start_without_task_num = [v for k, v in enumerate(members_start_list) if not task_num_res[k]]
                logger.info(members_start_without_task_num)
                if members_start_without_task_num:
                    # members开工无任务里选择p值低的(p为任务起点距离工人的距离)
                    p = []
                    for r in members_start_without_task_num:
                        # 工人当前位置
                        workman_lat = dao_session.redis_session.r.hget(
                            DISPATCH_WORKMAN_CONFIG.format(workman_id=r), "lat")
                        workman_lng = dao_session.redis_session.r.hget(
                            DISPATCH_WORKMAN_CONFIG.format(workman_id=r), "lng")
                        origin = "{},{}".format(workman_lat, workman_lng)
                        # 任务起点位置
                        destination = "{},{}".format(start_lat, start_lng)
                        s, t = self.get_task_duration(r, origin, destination)
                        p.append(s)

                    # 找出最小的p值
                    min_index, min_number = min(enumerate(p), key=operator.itemgetter(1))
                    assign_workman_id = members_start_without_task_num[min_index]
                    return assign_workman_id
                else:
                    # TODO members开工有任务筛选(做路径规划,比较时间),不符合则在当前服务区开工列表筛选
                    pass

    def get_workman_ability(self, workman_list: list, actual_type):
        """
        获取工人列表的能力
        :param workman_list: 工人列表
        :param actual_type: 派单类型
        :return:[(13578945612,10),(13578945612,20)]
        """
        logger.info("get_workman_ability workman_list:{} actual_type:{}".format(workman_list, actual_type))
        if actual_type == ActualType.CHANGE_BATTERY.value:
            sql = """
                        SELECT
                            workman_id,
                            30 AS ability 
                        FROM
                            xc_mieba_2_workman_config 
                        WHERE
                            workman_id IN :workman_id  AND can_change_battery = 1
                    """
        elif actual_type == ActualType.FIX.value:
            sql = """
                        SELECT
                            workman_id,
                            10 AS ability 
                        FROM
                            xc_mieba_2_workman_config 
                        WHERE
                            workman_id IN :workman_id AND can_fix = 1
                    """
        elif actual_type == ActualType.MOVE_CAR.value:
            sql = """
                        SELECT
                            workman_id,
                            10 AS ability
                        FROM
                            xc_mieba_2_workman_config 
                        WHERE
                            workman_id IN :workman_id AND can_move_car = 1
                    """
        else:
            sql = """
                        SELECT
                            workman_id,
                            10 AS ability   
                        FROM
                            xc_mieba_2_workman_config 
                        WHERE
                            workman_id IN :workman_id AND can_inspect = 1
                    """
        filter_result = dao_session.session().execute(sql, {"workman_id": workman_list}).fetchall()
        logger.info("get_workman_ability filter_result:{}".format(filter_result))
        return filter_result

    def check_workmanlist_ability(self, workman_start_list: list, actual_type):
        """
        检查工人的接任务的能力
        """
        # 筛选具有能力及其各自限制数量 [(13578945612,10),(13578945612,20)]
        logger.debug("check_workmanlist_ability: {},{}".format(workman_start_list,actual_type))
        filter_result = self.get_workman_ability(workman_start_list, actual_type)
        # filter_result.append(('13026288084', 10,),)
        if not filter_result:
            return False

        # 查询当前过滤出的工人的任务数
        # 查询当前过滤出的工人(workman_start_list) 此时actual_type 当天的任务数 工人的能力和actual_type有关吗
        # 从这里也可以查出所有的任务
        now = datetime.datetime.now()
        zero_today = now - datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,
                                              microseconds=now.microsecond)
        last_today = zero_today + datetime.timedelta(hours=23, minutes=59, seconds=59)
        filters = set()
        filters.add(XcMieba2Dispatch.created_at.between(zero_today, last_today))
        filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))
        filters.add(XcMieba2Dispatch.workman_id.in_(workman_start_list))
        filters.add(XcMieba2Dispatch.actual_type == actual_type)
        task_result = dao_session.session().query(XcMieba2Dispatch.workman_id,
                                                  sql.func.count(XcMieba2Dispatch.id).label("task_sum")).filter(
            *filters).group_by(XcMieba2Dispatch.workman_id).all()

        # r:('工人phone','今天的任务数')
        task_result_dict = {}
        for r in task_result:
            task_result_dict.setdefault(r[0], r[1])
        # r:('工人phone','能力')
        workman_id_list = []
        for r in filter_result:
            if not task_result_dict.get(r[0], 0):  # 如果工人没有任务，并且能力大于等于1，则加入备选
                if r[1] >= 1:
                    workman_id_list.append(r[0])
            else:
                if task_result_dict.get(r[0]) <= r[1] - 1:  # 如果有任务，并且任务数+1小于等于能力数，则加入备选
                    workman_id_list.append(r[0])
        logger.debug("check_workmanlist_ability: {}".format(workman_id_list))
        if workman_id_list:
            return workman_id_list
        else:
            return False

    def filter_workman_start_list(self, workman_start_list: list, actual_type):
        """
        筛选工人
        :param workman_start_list:
        :param actual_type:
        :return:
        """
        # workman_id_list = self.check_workmanlist_ability(workman_start_list, actual_type)
        workman_id_list =workman_start_list
        # assign_workman_id = random.choice(workman_id_list) if workman_id_list else False
        logger.info("workman_id_list: {}".format(workman_id_list))
        if workman_id_list:
            try:
                assign_workman_info = self.get_best_workman(workman_id_list)
                if assign_workman_info:
                    return assign_workman_info.get("workmanid", False)
                else:
                    raise MbException("计算推荐人失败")
            except Exception:
                return random.choice(workman_id_list) if workman_id_list else False
        else:
            return False

    # TODO 这个函数有点乱，后面需要重构一下
    def get_best_workman(self, workman_id_list):
        start_timestamp = time.time()
        logger.info("start find best man at:{}, the workman_id_list is: {}".format(start_timestamp, workman_id_list))
        # TODO 可能时循环引用的问题，后续需要解决一下
        from scripts.dispatch.vhicle_routes2 import VhicleRoutes
        import json
        now = datetime.datetime.now()
        zero_today = now - datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,
                                              microseconds=now.microsecond)
        last_today = zero_today + datetime.timedelta(hours=23, minutes=59, seconds=59)

        # 数据库查询 deadline大于现在，status是开工的，工人列表是提供的
        # TODO 这个应该和getroute的查询统一一下？
        filters = set()
        filters.add(XcMieba2Dispatch.deadline > now)
        filters.add(XcMieba2Dispatch.created_at.between(zero_today, last_today))
        filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))
        filters.add(XcMieba2Dispatch.workman_id.in_(workman_id_list))
        task_result = dao_session.session().query(XcMieba2Dispatch.id,
                                                  XcMieba2Dispatch.start_lat,
                                                  XcMieba2Dispatch.start_lng,
                                                  XcMieba2Dispatch.end_lat,
                                                  XcMieba2Dispatch.end_lng,
                                                  XcMieba2Dispatch.deadline,
                                                  XcMieba2Dispatch.status,
                                                  XcMieba2Dispatch.actual_type,
                                                  XcMieba2Dispatch.workman_id).filter(*filters).all()
        move_car_dict = {}
        other_dict = {}
        other_dict_id = {}
        move_car_start_dict_id = {}
        move_car_end_dict_id = {}

        for record in task_result:
            workman_id = record.workman_id
            move_car_list = move_car_dict.setdefault(workman_id, [])
            other_list = other_dict.setdefault(workman_id, [])
            other_list_id = other_dict_id.setdefault(workman_id, [])
            move_car_start_list_id = move_car_start_dict_id.setdefault(workman_id, [])
            move_car_end_list_id = move_car_end_dict_id.setdefault(workman_id, [])

            # TODO 这个应该和gettoute方法里面的max_nodes进行统一
            if (len(other_list) + 2 * len(move_car_list)) > 15:
                continue
            start_lat = float(record.start_lat)
            start_lng = float(record.start_lng)
            dispatch_id = record.id
            tw = self.get_tw(record.deadline)
            if record.actual_type == ActualType.MOVE_CAR.value:
                end_lat = float(record.end_lat)
                end_lng = float(record.end_lng)
                move_car_list.append((start_lat, start_lng, end_lat, end_lng, tw))
                move_car_start_list_id.append((start_lat, start_lng, dispatch_id))
                move_car_end_list_id.append((end_lat, end_lng, dispatch_id))
            else:
                other_list.append((start_lat, start_lng, tw))
                other_list_id.append((start_lat, start_lng, dispatch_id))

        logger.info("move_car_dict:{}".format(json.dumps(move_car_dict)))
        logger.info("other_dict:{}".format(json.dumps(other_dict)))
        logger.info("other_dict_id:{}".format(json.dumps(other_dict_id)))
        logger.info("move_car_start_dict_id:{}".format(json.dumps(move_car_start_dict_id)))
        logger.info("move_car_end_dict_id:{}".format(json.dumps(move_car_end_dict_id)))

        min_change = inf
        min_info = {}
        for workman_id in workman_id_list:
            logger.info("增加任务: id:{}, start: {}_{}, end: {}_{}, type: {}".format(self.dispatch_id, self.start_lat,
                                                                                 self.start_lng, self.end_lat,
                                                                                 self.end_lng, self.actual_type))
            # 拿工人的经纬度
            lat_lng = self.get_worker_gps(workman_id)
            move_car_list = move_car_dict.get(workman_id, [])
            other_list = other_dict.get(workman_id, [])
            other_list_id = other_dict_id.get(workman_id, [])
            move_car_start_list_id = move_car_start_dict_id.get(workman_id, [])
            move_car_end_list_id = move_car_end_dict_id.get(workman_id, [])

            # 不加新的任务的
            logger.info("工人当前任务计算路径!")
            task_data = {
                "other_list": other_list,
                "move_car_list": move_car_list,
            }
            index2id = other_list_id[:]
            index2id.extend(move_car_start_list_id)
            index2id.extend(move_car_end_list_id)
            index2id.insert(0, lat_lng)

            logger.info("task_data:{}".format(json.dumps(task_data)))

            vr1 = VhicleRoutes(task_data, lat_lng)
            vr1_distance = vr1.cal_route_distance()

            # 加入新的任务的
            logger.info("增加额外任务计算路径")
            start_lat = float(self.start_lat)
            start_lng = float(self.start_lng)
            dispatch_id = self.id
            deadline = self.deadline
            tw = 3600
            # tw = self.get_tw(deadline)

            if self.actual_type == ActualType.MOVE_CAR.value:
                end_lat = float(self.end_lat)
                end_lng = float(self.end_lng)
                move_car_list.append((start_lat, start_lng, end_lat, end_lng, tw))
                move_car_start_list_id.append((start_lat, start_lng, dispatch_id))
                move_car_end_list_id.append((end_lat, end_lng, dispatch_id))
            else:
                other_list.append((start_lat, start_lng, tw))
                other_list_id.append((start_lat, start_lng, dispatch_id))
            task_data = {
                "other_list": other_list,
                "move_car_list": move_car_list,
            }
            logger.info("task_data:{}".format(json.dumps(task_data)))
            index2id = other_list_id[:]
            index2id.extend(move_car_start_list_id)
            index2id.extend(move_car_end_list_id)
            vr2 = VhicleRoutes(task_data, lat_lng)
            # 因为计算路径时增加了一个起始点，比往常多以，所以这里写成+1-1
            if len(index2id) + 1 - 1 in vr2.drop_node_list:
                logger.info("workmanid: {} dispatch_id:{} dropped!".format(workman_id, self.dispatch_id))
                continue
            else:
                vr2_distance = vr2.cal_route_distance()
                cost_change = vr2_distance - vr1_distance
                if cost_change < min_change:
                    min_change = cost_change
                    min_info = {
                        "workmanid": workman_id,
                        "routes": vr2.best_route,
                        "drop_nodes_list": vr2.drop_node_list
                    }

        logger.info("cal finish! at: {}, using {} seconds".format(time.time(), time.time() - start_timestamp))
        if min_info:
            min_info["min_change"] = min_change
            return min_info
        else:
            return False

    def get_worker_gps(self, workman_id):
        workman_lat = dao_session.redis_session.r.hget(
            DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id), "lat")
        workman_lng = dao_session.redis_session.r.hget(
            DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id), "lng")
        logger.info(
            "workman_id:{} location:{}_{} type:{}".format(workman_id, workman_lat, workman_lng, type(workman_lat)))
        if workman_lat and workman_lng:
            try:
                return (float(workman_lat), float(workman_lng),)
            except Exception as e:
                raise MbException("经纬度转换出问题：{}".format(e))
        else:
            raise MbException("无法找到工人GPS")

    def get_tw(self, date_time):
        return date_time.timestamp() - time.time()


    def choose_from_start_list(self, service_id, actual_type):
        """
        从DISPATCH_WORKMAN_START_LIST开工列表中找
        :param service_id:
        :param actual_type:实际派单分类
        :return:
        """
        # 开工列表
        workman_start_set = dao_session.redis_session.r.smembers(
            DISPATCH_WORKMAN_START_LIST.format(**{"service_id": service_id}))
        if not workman_start_set:
            return False
        return self.filter_workman_start_list(list(workman_start_set), actual_type)

    def assign_workman_by_task_num(self, service_id, members, actual_type):
        """
        分配任务到指定工人(工人选择的问题(根据任务类型、工人配置的能力,随机派发),优先members,后开工列表)
        :param service_id:服务区id
        :param members:13554131850;13554131851
        :param actual_type:实际派单分类
        :return:
        """
        logger.info("assign_workman_by_task_num service_id:{},members:{},actual_type:{}".format(service_id, members,
                                                                                                actual_type))
        if members:
            members_list = members.split(';')
            # 查询members下的开工状态
            with dao_session.redis_session.r.pipeline(transaction=False) as start_pipeline:
                for m in members_list:
                    start_pipeline.sismember(DISPATCH_WORKMAN_START_LIST.format(**{"service_id": service_id}), m)
                start_res = start_pipeline.execute()
            if True not in start_res:
                """members下无开工状态的工人,从当前服务区的开工列表选"""
                assign_workman_id = self.choose_from_start_list(service_id, actual_type)
                logger.info("assign_workman_by_task_num members1==>assign_workman_id:{}".format(assign_workman_id))
                return assign_workman_id
            else:
                """members下有开工状态的工人"""
                members_start_list = [v for k, v in enumerate(members_list) if start_res[k]]
                f_result = self.filter_workman_start_list(members_start_list, actual_type)
                assign_workman_id = f_result if f_result else self.choose_from_start_list(service_id,
                                                                                          actual_type)
                logger.info("assign_workman_by_task_num members2==>assign_workman_id:{}".format(assign_workman_id))
                return assign_workman_id
        else:
            assign_workman_id = self.choose_from_start_list(service_id, actual_type)
            logger.info("assign_workman_by_task_num no members==>assign_workman_id:{}".format(assign_workman_id))
            return assign_workman_id



    def assign_workman_by_task_num_v2(self, service_id, members, actual_type):
        """
        分配任务到指定工人(工人选择的问题(根据任务类型、工人配置的能力,随机派发),优先members,后开工列表)
        2020-12-29新增逻辑，设置了一个全局变量记录已经被分配过的人，优先级 members中未被分配过的，开工列表未被分配过的，members，开工列表
        :param service_id:服务区id
        :param members:13554131850;13554131851
        :param actual_type:实际派单分类
        :return:
        """
        logger.info("assign_workman_by_task_num_v2 service_id:{},members:{},actual_type:{}".format(service_id, members,
                                                                                                actual_type))
        members = members.split(";")
        members = set(members)
        # 已经分配的
        assigned_workmanids = set(dao_session.redis_session.r.smembers(SERVICE_ASSIGNED_WORKMANIDS.format(service_id=service_id)))
        # 开工列表
        workman_start_set = set(dao_session.redis_session.r.smembers(DISPATCH_WORKMAN_START_LIST.format(service_id=service_id)))
        # 开工并且有能力的人
        workman_start_set = set(self.check_workmanlist_ability(workman_start_list=list(workman_start_set),actual_type=actual_type))
        # 即在members又在开工的人又有能力的人都有谁，这里用交集
        members = members & workman_start_set
        # members中开工并且没有被分配过的，在members不在assigned，这里用-
        members_not_assigned = members - assigned_workmanids
        # 所有开工的人没有被分配过的
        workman_start_not_assigned = workman_start_set - assigned_workmanids
        logger.debug("assigned_workmanids:{} workman_start_set:{} members_not_assigned:{} workman_start_not_assigned:{}".format(assigned_workmanids, workman_start_set, members_not_assigned, workman_start_not_assigned))
        actual_list = []
        # 如果members里面有没有分配过的，就分配给他
        if members_not_assigned:
            actual_list = list(members_not_assigned)
            logger.debug("case1! {}".format(actual_list))
        # 如果members都被分配了，从开工列表里面选一个
        elif workman_start_not_assigned:
            actual_list = list(workman_start_not_assigned)
            logger.debug("case2! {}".format(actual_list))
        # 如果两个都分配过了，那么将他们从分配列表里面删除，并且分配给members
        else:
            # 移除所有的开工列表
            removelist = list(members | workman_start_set)
            if removelist:
                removed_num = dao_session.redis_session.r.srem(SERVICE_ASSIGNED_WORKMANIDS.format(service_id=service_id), *removelist)
            # 如果members里面有人，就分配给members
            if members:
                actual_list = members
                logger.debug("case3! {}".format(actual_list))
            # 如果其他有人，分配给其他
            elif workman_start_set:
                actual_list = workman_start_set
                logger.debug("case4! {}".format(actual_list))
            # 都没人不分配
            else:
                logger.debug("case5! 没人可以分配")
                return False
        assign_workman_id = self.filter_workman_start_list(actual_list, actual_type)
        logger.debug("cal finish! assign_workman_id is:{}".format(assign_workman_id))
        if assign_workman_id:
            dao_session.redis_session.r.sadd(SERVICE_ASSIGNED_WORKMANIDS.format(service_id=service_id), assign_workman_id)
        else:
            logger.info("assign_workman_by_task_num_v2 no members==>assign_workman_id:{}".format(assign_workman_id))
        logger.info("assign_workman_by_task_num_v2 finish! ==>assign_workman_id:{}".format(assign_workman_id))
        return assign_workman_id

if __name__ == '__main__':
    res = DispatchTaskService()

    # distance, t = res.get_task_duration(move_car_capacity=1, origin="116.481499,39.990475",
    #                                            destination="116.465063,39.999538")
    # distance, t = res.get_task_duration(move_car_capacity=4, origin="116.481499,39.990475",
    #                                            destination="116.465063,39.999538")
    # distance, t = res.get_task_duration(workman_id=15871688162, origin="116.481499,39.990475",
    #                                     destination="116.465063,39.999538")
    result = res.assign_workman(dispatch_id=1, service_id=1, members="123;134")
    # print(distance, t)
    print(result)
