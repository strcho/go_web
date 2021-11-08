import json
import random

from model.all_model import *
from service import MBService
from service.config import ConfigService
from service.dashboard import DashboardService
from service.device import send_cmd, acc_fn, lock_fn
from service.electric_fence import ParkService
from service.geo_algorithm import parse_gps_address, inside_polygon_area, compute_distance_by_imei_trail
from service.permission import PermissionService
from mbutils import MbException, cfg, dao_session, logger
from mbutils.constant import ErrorType
from utils.constant.config import ConfigName
from utils.constant.device import DeviceState, OneMoveType, MoveType
from utils.constant.device import GfenceType
from utils.constant.redis_key import *
from utils.constant.ticket import AlarmType, FixState


class MovingVehicleBase(MBService):

    @staticmethod
    def tag_move(car_id, imei, op_man_id):
        # 标记挪车,防止自动锁车
        dao_session.redis_session.r.zadd(MOVE_BIKE_ZSET, {car_id: op_man_id})
        expire_time = dao_session.redis_session.r.get(DYNAMIC_MOVE_EBIKE_KEY) or 6 * 3600
        dao_session.redis_session.r.set(MOVE_CAR_STATE.format(imei=imei), 1, ex=expire_time)

    @staticmethod
    def untag_move(car_id, imei):
        # 清除标记挪车
        dao_session.redis_session.r.zrem(MOVE_BIKE_ZSET, car_id)
        dao_session.redis_session.r.delete(MOVE_CAR_STATE.format(imei=imei))

    @staticmethod
    def get_soc(imei) -> float:
        report_soc = dao_session.redis_session.r.get(DEVICE_REPORT_SOC.format(imei=imei))
        soc = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "soc")
        if not report_soc or not soc:
            return -1
        else:
            return float(soc)

    @staticmethod
    def get_rest_battery(voltage: str, imei: str, car_id: str) -> float:
        """ 根据电压计算电量 """
        if not voltage or voltage == "NaN":
            return 0
        logger.info("voltage:", voltage)
        soc = MovingVehicleBase.get_soc(imei)
        plan = dao_session.redis_session.r.get(CAR_BINDING_BATTERY_NAME.format(**{"car_id": car_id}))
        # logger.info("电压方案", plan)
        if plan and json.loads(plan).get("batteryType", None):
            battery_type = json.loads(plan).get("batteryType", None)
            # 有电池类型,则按照电池类型计算
            rest_battery = DashboardService().battery_type_4cell_num(float(voltage), battery_type)
            if soc != -1 and rest_battery - soc < 50:
                # 当计算得出的电压和bms绝对值小于50时，才去用soc。避免soc出现错误
                rest_battery = soc
            logger.info("voltage:", battery_type, rest_battery, soc, rest_battery - soc)
        else:
            # 没有电池类型, 则按通过计算
            rest_battery = (100 * (float(voltage) - 3366 * 13)) / (839 * 13)
        logger.info("rest_battery:", rest_battery)
        if rest_battery > 100:
            rest_battery = 100
        elif rest_battery < 0:
            rest_battery = 0
        return int(rest_battery)

    @staticmethod
    def get_rest_itinerary(battery: str, imei: str, car_id: str) -> float:

        """ 根据电量计算剩余里程 """
        if battery == "NaN":
            battery = 0
        plan = dao_session.redis_session.r.get(CAR_BINDING_BATTERY_NAME.format(**{"car_id": car_id}))
        if not plan:
            return 0
        full_itinerary = float(plan.get("maxMileage", 60))
        itinerary_rest = float(battery) / 100 * full_itinerary
        if itinerary_rest < 0:
            itinerary_rest = 0
        return itinerary_rest

    def get_state(self, imei):
        """
            获取当前设备状态
            空状态也不做服务区围栏判定及恢复操作。上线的车除非下线，不会把状态置空，如果有则就是下线，或者需要解决bug
        :param imei:
        :return:
        """
        battery_state = dao_session.redis_session.r.get(DEVICE_STATE_BATTERY.format(imei=imei))
        if battery_state:
            return DeviceState.CHANGING_BATTERY.value
        else:
            return dao_session.redis_session.r.get(DEVICE_STATE.format(imei=imei))

    def judge_parking_capacity(self, parking_id):
        """ 判断停车区容量是否足够 """
        num = dao_session.sub_session().query(func.count(XcEbikeParking.parkingId)).filter_by(
            parkingId=parking_id).scalar() or 0
        max_num = dao_session.sub_session().query(XcEbikeGfence2.maxParkingNumber).filter_by(
            id=parking_id).scalar() or 0
        return max_num - num

    def device_position_limit(self, car_id, imei, service_id):
        """ 获取此时的车辆点位信息记录, 表示导致当前位置挪车的问题 """
        lock_time = dao_session.redis_session.r.zscore(LOCK_CAR_TIME.format(service_id=service_id), car_id)
        lock_hour = (datetime.now().timestamp() - lock_time / 1000) / 3600 if lock_time else 0
        alarm_list = [t for t in AlarmType.to_tuple() if
                      dao_session.redis_session.r.sismember \
                          (ALARM_DEVICE.format(service_id=service_id, alarm_type=t), imei)]

        return {
            "isParkingZone": dao_session.redis_session.r.get(DEVICE_BINDING_PARK_KEY.format(imei=imei)),
            "isOutOfServiceZone": dao_session.redis_session.r.get(Out_Gfence_Service_Flag.format(imei=imei)),
            "isInNoParkingZone": dao_session.redis_session.r.get(DEVICE_BINDING_NO_PARK_KEY.format(imei=imei)),
            "noOrderTime": lock_hour,
            "deviceState": dao_session.redis_session.r.get(DEVICE_STATE.format(imei=imei)),
            "alarmTypes": json.dumps(alarm_list)
        }

    @staticmethod
    def check_and_get_operator(phone, service_id):
        """ 检查操作人存在和有对应服务区的权限 """
        operator = dao_session.sub_session().query(XcOpman).filter_by(opManId=phone).first()
        if not operator:
            raise MbException("操作人不存在")
        # 判断操作人是否有权限
        if not PermissionService().has_service_property_by_phone(phone, service_id):
            logger.info('movingVehicleService.js startMoving/findoperator: no service permission!, phone: ', phone)
            raise MbException("该操作人无权限")
        return operator

    def move_close_alarm_tikcet(self, imei, service_id, car_id, device_state, operator, phone):
        """
            如果有超一日无单在，挪车后自动闭合
        """
        # 重置车辆一天超时无单的过期key
        without_order_days = cfg.get("withoutOrderDays", 1)
        dao_session.redis_session.r.setex(ONE_DAY_WITHOUT_ORDER.format(imei=imei), without_order_days * 86400, imei)
        dao_session.redis_session.r.zadd(LOCK_CAR_TIME.format(service_id=service_id),
                                         {car_id: int(datetime.now().timestamp()) * 1000})

        # 如果不存在开锁异常的订单, 不是骑行中或者损坏的状态, 则挪车后状态变为可用车
        exist_abnormal = dao_session.sub_session().query(XcEbikeAlarmTickets2.ticketNo) \
            .filter(
            XcEbikeAlarmTickets2.ebikeNo == car_id,
            XcEbikeAlarmTickets2.type == AlarmType.UNLOCK_ABNORMAL.value,
            XcEbikeAlarmTickets2.state.in_(FixState.unfixed_list())).first()
        if device_state not in [DeviceState.RIDING.value, DeviceState.BROKEN.value,
                                DeviceState.DRAG_BACK.value] and not exist_abnormal:
            dao_session.redis_session.r.set(DEVICE_STATE.format(imei=imei), DeviceState.READY.value)

        # 关闭一日无单的订单 TODO 关闭的范围太远了
        dao_session.session().query(XcEbikeAlarmTickets2) \
            .filter(XcEbikeAlarmTickets2.ebikeNo == car_id,
                    XcEbikeAlarmTickets2.type == AlarmType.ONE_DAY_WITHOUT_ORDER.value,
                    XcEbikeAlarmTickets2.state.in_(FixState.unfixed_list())).update({
            "state": FixState.FIXED.value,
            "fixedTime": datetime.now(),
            "updatedAt": datetime.now(),
        }, synchronize_session=False)
        #        logger.info('批量挪车后闭合超一日无单：', '车辆号：', carId, '操作人：', phone);
        logger.biz("[fixTicket]", logger.format_equal({
            "ticketType": "ALARM",
            "agentId": 2,
            "opMan": operator,
            "opManId": phone,
            "serviceId": service_id,
            "carId": car_id,
            "state": FixState.FIXED.value
        }))
        # 清零该设备车辆的开锁失败记录
        fail_info = json.dumps({"failNum": 0, "failType": []})
        dao_session.redis_session.r.set(UNLOCK_FAIL_INFO.format(car_id=car_id), fail_info)

    def set_single_moving_lock(self, car_id):
        """ 设置互斥锁, 设置失败则异常 """
        t = dao_session.redis_session.r.get(MOVING_STATE_TIME) or 30 * 60
        self.nx_lock(MOVING_STATE_CARID.format(car_id=car_id), timeout=t, promt="该车辆已被申请挪车")

    def get_current_position(self, imei):
        device_info = dao_session.redis_session.r.hgetall(IMEI_BINDING_DEVICE_INFO.format(imei=imei))
        if not device_info:
            logger.info(f"movingVehicleService.js endMoving: {imei} no instanceFromRedis!")
            raise MbException("获取车辆信息失败")
        return device_info.get("lat", 0), device_info.get("lng", 0)

    def is_in_parking_zone(self, lat, lng):
        """ 获取当前位置的停车区或者道钉站点 """
        parking_list = ParkService.get_near_parking(lng, lat, 500, GfenceType.FOR_PARK.value)
        tb_parking_list = ParkService.get_near_parking(lng, lat, 500, GfenceType.TBEACON_PARKING.value)  # 加入获取道钉的站点
        park = parking_list + tb_parking_list
        cur_location = [float(lng), float(lat)]

        # 判断是否在站点内
        return inside_polygon_area(cur_location, park)

    # raise MbException("请将车辆挪至附近停车区", ErrorType.OUT_OF_PARKING)
    def is_in_parking_zone2(self, lat, lng):
        """ 获取当前位置的停车区 """
        parking_list = ParkService.get_near_parking(lng, lat, 500, GfenceType.FOR_PARK.value)
        park = parking_list
        cur_location = [float(lng), float(lat)]  # 获取当前位置的停车区

        # 判断是否在站点内
        return inside_polygon_area(cur_location, park)

    def get_batch_move_num(self, record_id):
        """ 必须走主库,获取已完成挪车总数和当前总数 """
        complete_num = dao_session.session().query(func.count(XcEbikeOnemoveRecord.id)) \
                           .filter_by(recordId=record_id, isFinish=1).scalar() or 0
        num = dao_session.session().query(func.count(XcEbikeOnemoveRecord.id)) \
                  .filter_by(recordId=record_id).scalar() or 0
        return complete_num, num

    def move_vehicle_result_auto_check(self, record_id):
        """
            对应node函数: MoveVehicleResultAutoCheck
            只有完成挪车才开始检查
            content:
            {"positionLimit":{"isParkingZone":null,
            "isOutOfServiceZone":null,
            "isInNoParkingZone":null,
            "noOrderTime":null},
            "examineConfig":{"isInPark":"0",
            "hasPicture":"0",
            "oneMoveMinDistance":0,
            "oneMoveMinTime":1,
            "oneMoveMaxTime":84199,
            "oneMoveTimeDiff":0},
            "examineControl":{"examineStartTime":"1626417156",
            "examineEndTime":"1630252799"},
            "performance":1,
            "performanceMultiple":1,
            "closed":1,
            "version":1626417156}
        :param record_id:
        :return:
        """
        one_operation = dao_session.session().query(XcEbikeMoveOperation).filter_by(recordId=record_id).first()
        if not one_operation or not one_operation.serviceAreaId or not one_operation.isFinish or not one_operation.movingNumber:
            return False
        # 是否
        router = ConfigName.AUTOMOVEVEHICLESINGLE.value if one_operation.movingType == 1 else ConfigName.AUTOMOVEVEHICLELIST.value
        content = ConfigService().get_router_content(router, one_operation.serviceAreaId)
        if not content:
            return False
        # 挪车自动审核开启
        if int(content.get("closed", 0)) != 0:
            # 是否在审核时间段
            try:
                start_date = int(content["examineControl"]["examineStartTime"])
                end_date = int(content["examineControl"]["examineEndTime"])
                if start_date <= datetime.now().timestamp() <= end_date:
                    # 批量挪车成功多少绩效算多少，100辆通过89辆就算89辆的绩效而不是直接审核不过
                    time_cost = one_operation.endTime.timestamp() - one_operation.startTime.timestamp()
                    # 缺省的时候没有配置0
                    is_in_park = int(content["examineConfig"]["isInPark"]) or 0
                    has_picture = int(content["examineConfig"]["hasPicture"]) or 0
                    min_time = content["examineConfig"]["oneMoveMinTime"] or 0
                    max_time = content["examineConfig"]["oneMoveMaxTime"] or 24 * 3600
                    min_distance = content["examineConfig"]["oneMoveMinDistance"] or 0
                    min_sence_last_move = content["examineConfig"]["oneMoveTimeDiff"] or 0  # 影响性能,暂时不做
                    performance_mult = content["performanceMultiple"]
                    performance = content["performance"]
                    is_parking_zone = content["positionLimit"]["isParkingZone"] or 0
                    is_out_of_service_zone = content["positionLimit"]["isOutOfServiceZone"] or 0
                    is_in_no_parking_zone = content["positionLimit"]["isInNoParkingZone"] or 0
                    no_order_time = content["positionLimit"]["noOrderTime"] or 0
                    one_operation.updatedAt = datetime.now()

                    # 集中检查是否在站点,图片; 分开检查:通过检查挪车前点位,检查挪车距离,耗时
                    if time_cost >= min_time and time_cost <= max_time \
                            and not (is_in_park and not one_operation.isInPark) \
                            and not (has_picture and not (one_operation.frontPicture or one_operation.backPicture)):
                        one_moves = dao_session.session().query(XcEbikeOnemoveRecord).filter_by(
                            recordId=record_id).all()
                        # 更新挪车检查记录xc_ebike_move_check_record
                        effective_num = 0
                        for one_move in one_moves:
                            params = {
                                # 满足条件是1
                                "recordId": one_operation.recordId,
                                "movingType": one_operation.movingType,
                                "carId": one_move.carId,
                                "isParkingZone": not (is_parking_zone and not one_move.isParkingZone),
                                # isParkingZone
                                "isOutOfServiceZone": not (is_out_of_service_zone and not one_move.isOutOfServiceZone),
                                "isInNoParkingZone": not (is_in_no_parking_zone and not one_move.isInNoParkingZone),
                                "noOrderTime": not (no_order_time and one_move.noOrderTime <= no_order_time),
                                "isInPark": not (is_in_park and not one_move.isParkingZone),
                                "hasPicture": not (has_picture and not (
                                        one_operation.frontPicture or one_operation.backPicture)),
                                "moveMinTime": int(time_cost >= min_time),
                                "moveMaxTime": int(time_cost <= max_time),
                                "moveDistance": int(one_move.distance >= min_distance),
                                "moveTimeDiff": 1
                            }
                            if not (is_parking_zone and not one_move.isParkingZone) \
                                    and not (is_out_of_service_zone and not one_move.isOutOfServiceZone) \
                                    and not (is_in_no_parking_zone and not one_move.isInNoParkingZone) \
                                    and not (no_order_time and one_move.noOrderTime <= no_order_time) \
                                    and one_move.distance >= min_distance:
                                effective_num += 1
                                #  没有配置的时候全部成功
                            check_record = XcEbikeMoveCheckRecord(**params)  # 这个表看起来有重复
                            dao_session.session().add(check_record)
                            dao_session.session().commit()
                        one_operation.checkResult = 3  # 审核通过
                        # 更新kpi记录
                        # logger.info("绩效参数:", performance_mult, performance, effective_num)
                        dao_session.session().query(XcEbikeKpiMoveCar).filter_by(recordId=record_id).update({
                            "achievement": performance_mult * performance * effective_num, "updatedAt": datetime.now()})
                        dao_session.session().commit()
                    else:
                        logger.error("自动审核失败没有记录", time_cost >= min_time and time_cost <= max_time,
                                     not (is_in_park and not one_operation.isInPark), not (
                                    has_picture and not (one_operation.frontPicture or one_operation.backPicture)))
                        one_operation.checkResult = 4  # 审核不通过
                        dao_session.session().commit()
            except Exception as ex:
                logger.error("挪车自动审核失败:", ex)
                dao_session.session().rollback()

    def judge_batch_finish(self, service_id, operator, phone, record_id, end_address, team_worker, end_moving_park_id,
                           back_picture):
        try:
            # 如果全部完成,则开始结算
            if not dao_session.session().query(XcEbikeOnemoveRecord.id).filter_by(recordId=record_id,
                                                                                  isFinish=0).first():
                # 完成批量挪车任务
                update_data = self.remove_empty_param({"endAddress": end_address,
                                                       "teamworker": team_worker,
                                                       "endTime": datetime.now(),
                                                       "isFinish": 1,
                                                       "isInPark": bool(end_moving_park_id),
                                                       "backPicture": back_picture})
                dao_session.session().query(XcEbikeMoveOperation).filter_by(recordId=record_id).update(update_data)

                # 添加kpi数据
                one = XcEbikeKpiMoveCar(
                    **{"recordId": record_id, "opMan": operator, "phone": phone})
                dao_session.session().add(one)
                logger.info("[routes/moveVehicle]",
                            logger.format_equal(
                                {"recordId": record_id, "opMan": operator,
                                 "phone": phone, "isTeamworker": False, "movingType": MoveType.BATCH.value}))

                # 添加团队kpi数据
                if team_worker:
                    team_worker_list = json.loads(team_worker)
                    for item in team_worker_list:
                        if item.get("name", None):
                            one = XcEbikeKpiMoveCar(
                                **{"recordId": record_id, "opMan": item["name"],
                                   "phone": item["phone"]})
                            dao_session.session().add(one)
                            logger.info("[routes/moveVehicle]",
                                        logger.format_equal(
                                            {"serviceId": service_id, "recordId": record_id, "opMan": item["name"],
                                             "phone": item["phone"], "isTeamworker": True,
                                             "movingType": MoveType.BATCH.value}))

                dao_session.session().commit()
        except Exception as ex:
            logger.info("judge_batch_finish:", ex)


class MovingVehicleService(MovingVehicleBase):
    def start_moving(self, valid_data, is_ble=False):
        """ 单台开始挪车 """
        car_id, tp, phone, start_address, agent_id, front_picture = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("无设备信息")
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))

        # 获取操作人信息
        operator = self.check_and_get_operator(phone, service_id)

        # 获取互斥锁
        self.set_single_moving_lock(car_id)

        # 判断是否在骑行中
        device_state = self.get_state(imei)
        if device_state == DeviceState.RIDING.value:
            raise MbException("设备处于骑行中，发送撤防命令失败", ErrorType.SEND_CMD_FAIL)

        if not is_ble:
            # 挪车还是推车
            if tp == OneMoveType.START_BUTTON.value:
                res = send_cmd(acc_fn(imei, True, True))
                if not res or res.get("code", None) != 0:
                    self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))
                    logger.info(f"moving_vehicle/START_BUTTON lock: {imei}is Removal failure")
                    raise MbException("发送解锁命令失败", ErrorType.SEND_CMD_FAIL)
            elif tp == OneMoveType.PUSH_CAR_BUTTON.value:
                res = send_cmd(lock_fn(imei, False))
                if not res or res.get("code", None) != 0:
                    self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))
                    logger.info(f"moving_vehicle/PUSH_CAR_BUTTON lock: {imei}is Removal failure")
                    raise MbException("发送撤防命令失败", ErrorType.SEND_CMD_FAIL)

        # 获取当前位置的停车区
        start_moving_park_id = ParkService.unbind_imei_park_id(imei)

        # 获取导致当前位置挪车的问题
        current_position = self.device_position_limit(car_id, imei, service_id)

        # 解除绑定关系
        ParkService.del_parking_info(imei)
        lat, lng = self.get_current_position(imei)
        try:
            # 记录一次挪车记录
            params = {
                "operator": operator.name,
                "phone": phone,
                "startTime": datetime.now(),
                "startAddress": start_address,
                "movingNumber": 1,
                "agentId": 2,
                "movingType": MoveType.ONE.value,
                "frontPicture": front_picture,
                "serviceAreaId": service_id
            }
            record = XcEbikeMoveOperation(**params)
            dao_session.session().add(record)
            dao_session.session().flush()

            infos = {
                "recordId": record.recordId,
                "carId": car_id,
                "operator": operator.name,
                "phone": phone,
                "startTime": datetime.now(),
                "startAddress": start_address,
                "isFinish": 0,
                "agentId": 2,
                "serviceAreaId": service_id,
                "startMovingParkId": start_moving_park_id,
                "updatedAt": datetime.now(),
                "startLat": lat,
                "startLng": lng
            }
            infos.update(current_position)
            dao_session.session().add(XcEbikeOnemoveRecord(**infos))
            dao_session.session().commit()
            self.tag_move(car_id, imei, phone)
        except Exception as ex:
            logger.error("创建挪车记录失败:", ex)
            dao_session.session().rollback()
            raise MbException("创建挪车记录失败")
        finally:
            self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))
        return "解锁成功"

    def end_moving(self, valid_data, is_force=False, is_ble=False):
        car_id, phone, back_picture = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))

        # 获取车辆信息
        lat, lng = self.get_current_position(imei)

        # 是否在停车区
        end_moving_park_id = self.is_in_parking_zone(lat, lng)
        if not is_force and not end_moving_park_id:
            raise MbException("请将车辆挪至附近停车区", ErrorType.OUT_OF_PARKING)

        # 判断是否站点已满
        if not is_force and self.judge_parking_capacity(end_moving_park_id) < 1:
            raise MbException("该停车区停车数量已满")

        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        # 根据坐标求出,建筑位置
        end_address = parse_gps_address(lng, lat, 'gcj02')

        # 判断是否在骑行中, 防止用户骑行时候误锁车
        device_state = self.get_state(imei)
        if not is_force and not is_ble:
            if device_state != DeviceState.RIDING.value:
                res = send_cmd(lock_fn(imei, True))
                if not res or res.get("code", None) != 0:
                    raise MbException("发送锁车命令失败", ErrorType.SEND_CMD_FAIL)
        try:
            # 建立车辆与停车区关系
            ParkService.bind_imei_park_id(imei, end_moving_park_id, service_id)

            # 更新挪车记录
            one_operation = dao_session.session().query(XcEbikeMoveOperation).filter_by(phone=phone,
                                                                                        isFinish=0).first()
            if not one_operation:
                raise MbException("获取挪车记录失败")
            # 通过tsdb获取距离
            move_distance = compute_distance_by_imei_trail(imei, one_operation.startTime.timestamp(),
                                                           datetime.now().timestamp())
            record_id = one_operation.recordId
            operator = one_operation.operator
            dao_session.session().query(XcEbikeOnemoveRecord).filter_by(recordId=record_id, carId=car_id) \
                .update({
                "endAddress": end_address,
                "isFinish": 1,
                "endTime": datetime.now(),
                "distance": move_distance,
                "serviceAreaId": service_id,
                "endMovingParkId": end_moving_park_id,
                "updatedAt": datetime.now(),
                "endLat": lat,
                "endLng": lng
            })
            dao_session.session().query(XcEbikeMoveOperation).filter_by(recordId=record_id).update(
                self.remove_empty_param({
                    "endAddress": end_address,
                    "backPicture": back_picture,
                    "isFinish": 1,
                    "endTime": datetime.now(),
                    "isInPark": bool(end_moving_park_id),
                    "updatedAt": datetime.now()
                }))

            # 新建单台挪车kpi记录
            one = XcEbikeKpiMoveCar(**{
                "recordId": record_id,
                "opMan": operator,
                "phone": phone,
                "achievement": 0
            })
            dao_session.session().add(one)
            # 车辆与停车区绑定
            ParkService.set_parking_info(imei, end_moving_park_id)

            # 清除挪车中的标志
            dao_session.session().commit()
            self.untag_move(car_id, imei)
        except Exception as ex:
            logger.error("生成挪车记录失败:", ex)
            dao_session.session().rollback()
            raise MbException("生成挪车记录失败")

        logger.biz("[routes/moveVehicle]", logger.format_equal({
            "serviceId": service_id,
            "recordId": record_id,
            "opMan": operator,
            "phone": phone,
            "movingType": MoveType.ONE.value,
            "isTeamworker": False
        }))
        self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))

        self.move_close_alarm_tikcet(imei, service_id, car_id, device_state, operator, phone)
        if is_force:
            return {"info": "强制单台挪车成功", "sum": 1, "completedSum": 1}, record_id
        else:
            return "锁车成功", record_id

    def batch_start(self, valid_data, is_ble=False):
        car_id, phone, agent_id, is_push_car = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("无设备信息")
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))

        # 获取操作人信息
        operator = self.check_and_get_operator(phone, service_id)
        lat, lng = self.get_current_position(imei)

        # 获取此时的车辆点位信息记录
        current_position = self.device_position_limit(car_id, imei, service_id)
        ParkService.unbind_imei_park_id(imei)
        # 是否在停车区
        start_moving_park_id = self.is_in_parking_zone2(lat, lng)

        # 根据坐标求出,建筑位置
        start_address = parse_gps_address(lng, lat, 'gcj02')

        # 判断是否在骑行中
        device_state = self.get_state(imei)
        if device_state == DeviceState.RIDING.value:
            raise MbException("车辆在骑行中，挪车操作失败！")

        # 获取互斥锁
        self.set_single_moving_lock(car_id)

        if not is_ble:
            cmd = lock_fn(imei, False) if is_push_car else acc_fn(imei, True, True)
            res = send_cmd(cmd)
            if not res or res.get("code", None) != 0:
                logger.info(f"moving_vehicle.py/batch_start lock:  {imei} is Removal failure")
                raise MbException("发送撤防命令失败", ErrorType.SEND_CMD_FAIL)

        try:
            one_operation = dao_session.session().query(XcEbikeMoveOperation) \
                .filter_by(phone=phone, isFinish=0).first()
            # 记录一次挪车记录
            if not one_operation:
                params = {
                    "operator": operator.name,
                    "phone": phone,
                    "startAddress": start_address,
                    "movingNumber": 1,
                    "agentId": 2,
                    "startTime": datetime.now(),
                    "movingType": MoveType.BATCH.value,
                    "serviceAreaId": service_id
                }

                one_operation = XcEbikeMoveOperation(**params)
                dao_session.session().add(one_operation)
                dao_session.session().commit()
            record_id = one_operation.recordId
            one_move = dao_session.session().query(XcEbikeOnemoveRecord) \
                .filter_by(recordId=record_id, carId=car_id, phone=phone).first()
            if not one_move:
                infos = {
                    "recordId": record_id,
                    "carId": car_id,
                    "operator": operator.name,
                    "phone": phone,
                    "startTime": datetime.now(),
                    "startAddress": start_address,
                    "isFinish": 0,
                    "agentId": 2,
                    "serviceAreaId": service_id,
                    "startMovingParkId": start_moving_park_id,
                    "startLat": lat,
                    "startLng": lng
                }
                infos.update(current_position)
                dao_session.session().add(XcEbikeOnemoveRecord(**infos))
            dao_session.session().commit()
            self.tag_move(car_id, imei, phone)
            # 解除绑定关系
            ParkService.del_parking_info(imei)


        except Exception as ex:
            logger.error("创建挪车记录失败:", ex)
            dao_session.session().rollback()
            raise MbException("创建挪车记录失败")
        finally:
            self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))

        complete_num, num = self.get_batch_move_num(record_id)
        return {"info": "撤防成功", "sum": num, "completedSum": complete_num}

    def batch_end(self, valid_data, is_force=False, is_ble=False):
        car_ids, phone, team_worker, back_picture = valid_data
        team_worker = json.dumps(team_worker)
        one_operation = dao_session.session().query(XcEbikeMoveOperation) \
            .filter_by(phone=phone, isFinish=0).first()
        if not one_operation:
            raise MbException("没有挪车记录")
        record_id = one_operation.recordId
        operator = one_operation.operator
        move_num = len(car_ids)
        first_car_id = random.choice(car_ids)
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=first_car_id))

        # 获取车辆信息
        lat, lng = self.get_current_position(imei)

        # 是否在停车区
        end_moving_park_id = self.is_in_parking_zone(lat, lng)
        if not is_force and not end_moving_park_id:
            raise MbException("请将车辆挪至附近停车区", ErrorType.OUT_OF_PARKING)

        # 判断是否站点已满
        if not is_force and self.judge_parking_capacity(end_moving_park_id) < move_num:
            raise MbException("该停车区停车数量已满")

        # 只保留停车区判断, 保留禁停区判断吗,会存在故意挪到禁停区吗(或者无意),客户很容易查到上一个人故意挪到禁停区
        lock_fail_list = []  # 锁车失败集合
        end_address = parse_gps_address(lng, lat, 'gcj02')
        # 通过tsdb获取距离
        bike_move_istance = compute_distance_by_imei_trail(imei, one_operation.startTime.timestamp(),
                                                           datetime.now().timestamp())
        lock_num = 0
        for car_id in car_ids:
            imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
            device_state = self.get_state(imei)
            service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
            if not is_ble:
                if device_state != DeviceState.RIDING.value:
                    res = send_cmd(lock_fn(imei, True))
                    if not res or res.get("code", None) != 0:
                        lock_fail_list.append({
                            "carId": car_id,
                            "whetherPark": bool(end_moving_park_id)
                        })
            if end_moving_park_id:
                ParkService.set_parking_info(imei, end_moving_park_id)
            try:
                dao_session.session().query(XcEbikeOnemoveRecord).filter_by(recordId=record_id, carId=car_id,
                                                                            isFinish=0).update({
                    "endAddress": end_address,
                    "teamworker": team_worker,
                    "isFinish": 1,
                    "endTime": datetime.now(),
                    "distance": bike_move_istance,
                    "endMovingParkId": end_moving_park_id,
                    "updatedAt": datetime.now(),
                    "endLat": lat,
                    "endLng": lng
                }, synchronize_session=False)
                lock_num += 1
                self.del_lock(MOVING_STATE_CARID.format(car_id=car_id))
                self.move_close_alarm_tikcet(imei, service_id, car_id, device_state, operator, phone)
                dao_session.session().commit()
                self.untag_move(car_id, imei)
            except Exception as ex:
                logger.info("batch_end:", ex)
                dao_session.session().rollback()
        # 更新挪车总数
        try:
            total_num = dao_session.session().query(func.count(XcEbikeOnemoveRecord.id)).filter_by(
                recordId=record_id).scalar()
            one_operation.movingNumber = total_num
            dao_session.session().commit()
        except Exception:
            raise MbException("更新挪车总数失败")

        self.judge_batch_finish(service_id, operator, phone, record_id, end_address, team_worker, end_moving_park_id,
                                back_picture)
        if is_force or is_ble:
            return "挪车成功", record_id
        else:
            return {"moveNumber": move_num,
                    "lockNumber": lock_num,
                    "NotLock": lock_fail_list,
                    "prohibitPark": []}, record_id

    def batch_list(self, valid_data) -> dict:
        """
        查看多台挪车的详细信息
        :param valid_data:
        :return:
        """
        phone, = valid_data
        one_operation = dao_session.sub_session().query(XcEbikeMoveOperation) \
            .filter_by(phone=phone, isFinish=0).first()
        if not one_operation:
            raise MbException("获取挪车记录失败")
        record_id = one_operation.recordId
        many = dao_session.sub_session().query(XcEbikeOnemoveRecord.carId) \
            .filter_by(recordId=record_id, isFinish=0).all()
        car_ids = [one[0] for one in many]
        complete_num = dao_session.sub_session().query(func.count(XcEbikeOnemoveRecord.id)) \
                           .filter_by(recordId=record_id, isFinish=1).scalar() or 0
        num = dao_session.sub_session().query(func.count(XcEbikeOnemoveRecord.id)) \
                  .filter_by(recordId=record_id).scalar() or 0
        return {"completeNumber": complete_num, "carIds": car_ids, "sum": num}

    def query_not_finish(self, valid_data):
        phone, = valid_data
        one_operation = dao_session.session().query(XcEbikeMoveOperation) \
            .filter_by(phone=phone, isFinish=0).first()
        if not one_operation:
            raise MbException("没有挪车记录")
        many = dao_session.session().query(XcEbikeOnemoveRecord.carId).filter_by(recordId=one_operation.recordId,
                                                                                 isFinish=0).all()
        car_ids = []
        for one in many:
            car_id = one[0]
            imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
            voltage = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage") or 0
            rest_battery = self.get_rest_battery(voltage, imei, car_id)
            itinerary = self.get_rest_itinerary(rest_battery, imei, car_id)
            # device_info = dao_session.redis_session.r.hgetall(IMEI_BINDING_DEVICE_INFO.format(imei=imei))
            device_info = dao_session.session().query(XcEbike2Carinfo).filter_by(carId=car_id).first()
            info = {
                "imei": imei,
                "carId": car_id,
                # "lng": device_info["lng"],
                # "lat": device_info["lat"],
                "battery": rest_battery,
                "itinerary": itinerary,
                "state": self.get_state(imei),
                # "version": device_info["version"],
                # "lastGPSTimestamp": device_info["timestamp"],
                "carNo": device_info.carNo,
            }
            if info["state"] == DeviceState.OFFLINE.value:
                info["state"] == DeviceState.READY.value
            car_ids.append(info)
        return {"movingType": one_operation.movingType, "carIds": car_ids}

    def remove_car(self, valid_data):
        """ 从批量挪车中移除车辆 """
        car_id, phone = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))

        # 发送锁车命令
        res = send_cmd(lock_fn(imei, True))
        if not res or res.get("code", None) != 0:
            logger.info(f"moving_vehicle.py/remove_car lock:  {imei} is Removal failure")
            raise MbException("发送撤防命令失败", ErrorType.SEND_CMD_FAIL)

        # 是否有挪车记录
        one_operation = dao_session.session().query(XcEbikeMoveOperation) \
            .filter_by(phone=phone, isFinish=0).first()
        if not one_operation:
            raise MbException("获取挪车记录失败")
        moving_type = int(one_operation.movingType)
        record_id = one_operation.recordId
        logger.info("move_type0:", moving_type, car_id, record_id, phone)
        # 修改批量挪车的总数
        if moving_type == MoveType.BATCH.value:
            try:
                if one_operation.movingNumber > 0:
                    one_operation.movingNumber = one_operation.movingNumber - 1
                dao_session.session().query(XcEbikeOnemoveRecord).filter_by(carId=car_id, recordId=record_id,
                                                                            phone=phone).delete()
                logger.info("move_type:", moving_type, car_id, record_id, phone)
                dao_session.session().commit()
            except Exception:
                raise MbException("修改总挪车数失败")

        # 在停车区，并且是批量则建立关系
        self.untag_move(car_id, imei)
        lat, lng = self.get_current_position(imei)
        park_id = self.is_in_parking_zone2(lat, lng)
        if park_id and int(moving_type) == MoveType.BATCH.value:
            # 建立车辆与停车区关系
            service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
            ParkService.bind_imei_park_id(imei, park_id, service_id)
            ParkService.set_parking_info(imei, park_id)

        # 以前移除的时候结算一下,干掉了
        complete_num, num = self.get_batch_move_num(record_id)
        return {"info": "删除成功", "sum": num, "completedSum": complete_num}

    def push_car_button(self, valid_data):
        """ 推车按钮 """
        car_id, = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        # 发送锁车命令
        res = send_cmd(lock_fn(imei, False))
        if not res or res.get("code", None) != 0:
            logger.info(f"moving_vehicle.py/push_car_button lock:  {imei} is Removal failure")
            raise MbException("发送解锁命令失败", ErrorType.SEND_CMD_FAIL)
        return "解锁成功"

    def start_permission(self, valid_data):
        tp, car_id, phone = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("无设备信息")

        # 判断是否在骑行中
        device_state = self.get_state(imei)
        if device_state == DeviceState.RIDING.value:
            raise MbException("设备处于骑行中，发送撤防命令失败", ErrorType.SEND_CMD_FAIL)
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))

        # 获取操作人信息
        self.check_and_get_operator(phone, service_id)
        return "获取权限成功"
