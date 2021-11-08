from model.all_model import *
from service import MBService
from service.geo_algorithm import imei_trail_from_tsdb
from mbutils import dao_session
import random
from utils.constant.redis_key import *
from utils.constant.device import PropertyType, DeviceState, OneMoveType, MoveType, GfenceType
from mbutils import logger
from service.permission import PermissionService
from service.device import *
from utils.constant.ticket import AlarmType, FixState, ToolOrPF
from mbutils import MbException
from mbutils.constant import ErrorType
from datetime import date, timedelta
from service.electric_fence import ParkService
import json
from mbutils import cfg
import random, time
from utils.constant.dispatch import OriginType
from .moving_vehicle import MovingVehicleBase
from tornado.httputil import HTTPServerRequest
from service.config import ConfigService
from utils.constant.config import ConfigName


class ToolsBase(MBService):
    @staticmethod
    def write_operate_log(opManId, opType, request: HTTPServerRequest):
        op_log = XcEbike2Log(**{"opManId": opManId, "opType": opType,
                                "router": request.uri, "message": json.dumps(request.arguments)})
        dao_session.session().add(op_log)
        dao_session.session().commit()

    def clear_up_alarm_check_count(self, imei):
        """ 清除车辆异常工单的统计 """
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        agent_id = dao_session.redis_session.r.get(IMEI_AGENT_BINDING.format(imei=imei)) or 2
        with dao_session.redis_session.r.pipeline(transaction=False) as pp:
            for state in [DeviceState.BROKEN.value,
                          DeviceState.OFFLINE.value,
                          DeviceState.DRAG_BACK.value,
                          DeviceState.CHANGING_BATTERY.value,
                          DeviceState.LOW_BATTERY.value,
                          DeviceState.BOOKING.value,
                          DeviceState.RIDING.value]:
                dao_session.redis_session.r.srem(SERVICE_DEVICE_STATE.format(service_id=service_id, state=state), imei)
                dao_session.redis_session.r.srem(AGENT_DEVICE_STATE.format(agent_id=agent_id, state=state), imei)
                dao_session.redis_session.r.srem(ALLY_DEVICE_STATE.format(state=state), imei)
            for state in AlarmType.to_tuple():
                dao_session.redis_session.r.srem(ALARM_DEVICE.format(service_id=service_id, alarm_type=state), imei)
                dao_session.redis_session.r.srem(AGENT_ALARM_DEVICE.format(agent_id=agent_id, state=state), imei)
                dao_session.redis_session.r.srem(ALLY_ALARM_DEVICE.format(state=state), imei)
            pp.execute()

    def clean_up_device_abnormal_infos(self, imei):
        """
            用于闭合设备所有工单，清除所有统计数字
            目前只用于服务区上线，服务区下架，换电完毕
        """
        dao_session.session().query(XcEbikeAlarmTickets2) \
            .filter(XcEbikeAlarmTickets2.imei == imei, XcEbikeAlarmTickets2.state.in_(FixState.unfixed_list())) \
            .update({"state": FixState.FIXED.value, "updatedAt": datetime.now()}, synchronize_session=False)
        dao_session.session().commit()
        self.clear_up_alarm_check_count(imei)

    def create_change_battery_record(self, imei, car_id, service_id, op_man_id, op_man_name):
        if not dao_session.redis_session.r.set(CHANGE_BATTERY_LOCK.format(imei=imei, op_man_id=op_man_id), 1, ex=5,
                                               nx=True):
            # 已经在换电了
            raise MbException("已经在换电中")
        # 5分钟内不再创建新的记录,只更新
        recent_record = dao_session.session().query(XcEbike2ChangeBattery).filter(
            XcEbike2ChangeBattery.ebikeNo == car_id,
            XcEbike2ChangeBattery.phone == op_man_id,
            XcEbike2ChangeBattery.createdAt.between(datetime.now() - timedelta(minutes=5), datetime.now())).first()
        if not recent_record:
            voltage = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage")
            rest_battery = MovingVehicleBase().get_rest_battery(voltage, imei, car_id)
            new_record = XcEbike2ChangeBattery(
                **{"imei": imei,
                   "ebikeNo": car_id,
                   "serviceId": service_id,
                   "opMan": op_man_name,
                   "phone": op_man_id,
                   "restBatteryBefore": rest_battery,
                   "openBatBoxTime": datetime.now(),
                   "checkResult": 2})
            dao_session.session().add(new_record)
            dao_session.session().commit()

    def update_finish_change_battery_record(self, car_id, op_man_id, state, rest_battery=0):
        # 5分钟换电过了,只更新这次换电记录,不在创建新的记录
        recent_record = dao_session.session().query(XcEbike2ChangeBattery).filter(
            XcEbike2ChangeBattery.ebikeNo == car_id,
            XcEbike2ChangeBattery.phone == op_man_id,
            XcEbike2ChangeBattery.openBatBoxTime > (datetime.now() - timedelta(minutes=5))
        ).first()
        if not recent_record:
            raise MbException("没有找到换电记录")
            # imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
            # service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
            # op_man_name = dao_session.sub_session().query(XcOpman.name).filter_by(opManId=op_man_id).scalar()
            # voltage = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage")
            # rest_battery = MovingVehicleBase().get_rest_battery(voltage, imei, car_id)
            # recent_record = XcEbike2ChangeBattery(
            #     **{"imei": imei,
            #        "ebikeNo": car_id,
            #        "serviceId": service_id,
            #        "opMan": op_man_name,
            #        "phone": op_man_id,
            #        "restBatteryBefore": rest_battery,
            #        "openBatBoxTime": datetime.now(),
            #        "checkResult": 2})
            # dao_session.session().add(recent_record)
        try:
            before = recent_record.restBatteryBefore
            recent_record.restBatteryAfter = rest_battery
            recent_record.restBatteryDiff = rest_battery - before
            recent_record.closeBatBoxTime = datetime.now()
            recent_record.changeBatteryLastTime = (datetime.now() - recent_record.openBatBoxTime).seconds
            recent_record.state = state
            recent_record.performance = 0  # 未通过审核以及待审核为0，通过审核为1
            recent_record.checkResult = 0  # 审核结果, 0为待审核，1为未通过，2为通过
            recent_record.updatedAt = datetime.now()
            # 换电审核
            if recent_record.state == 1:
                content = ConfigService().get_router_content(ConfigName.AUTOCHANGEBATTREY.value,
                                                             recent_record.serviceId)
                # 判断换电审核开启
                if int(content.get("closed", 0)) != 0:
                    # 是否在审核时间段
                    start_date = datetime.fromtimestamp(int(content["examineControl"]["examineStartTime"]))
                    end_date = datetime.fromtimestamp(int(content["examineControl"]["examineEndTime"]))
                    if start_date <= datetime.now() <= end_date:
                        before_vol = content["examineConfig"]["restBatteryBeforeMin"] or 100
                        after_vol = content["examineConfig"]["restBatteryAfterMax"] or 0
                        diff_vol = content["examineConfig"]["restBatteryDiff"] or 0
                        last_time = content["examineConfig"]["changeBatteryLastTime"] or 24 * 60 * 60
                        performance = content["performance"] or 1

                        if recent_record.restBatteryBefore <= before_vol \
                                and recent_record.restBatteryAfter >= after_vol \
                                and recent_record.restBatteryDiff >= diff_vol \
                                and recent_record.changeBatteryLastTime >= last_time:
                            recent_record.performance = performance
                            recent_record.checkResult = 3
                        else:
                            recent_record.checkResult = 4
            dao_session.session().commit()
        except Exception as ex:
            logger.info("记录换电操作失败:", ex)
            dao_session.session().rollback()
            raise MbException("记录换电操作失败")

    def check_lowest_battery(self, imei, car_id, service_id):
        voltage = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage")
        rest_battery = MovingVehicleBase().get_rest_battery(voltage, imei, car_id)
        content = ConfigService().get_router_content(ConfigName.BATTERYCAPACITY.value, service_id)
        capacity = (content and content["capacity"]) or 100
        if int(rest_battery) > capacity:
            logger.info("没有到达最低换电阈值:", voltage, rest_battery, content, capacity)
            raise MbException("没有到达最低换电阈值", ErrorType.OPEN_BATBOX_FAIL)


class ToolsService(ToolsBase):
    def car_searching(self, valid_data):
        """ 播放车辆寻车音 """
        car_id, auth_info, imei = valid_data
        op_man_id = auth_info["opManId"]
        logger.info('carSearching opManId:', op_man_id, "carId:", car_id)

        if not self.exists_param(imei):
            imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        res = send_cmd(car_searching(imei))
        if not res or res.get("code", None) != 0:
            raise MbException("寻车音播放失败", ErrorType.CAR_SEARCHING_FAILED)
        else:
            return "寻车音播放成功"

    def open_bat_box(self, valid_data, is_ble=False):
        """ 换电打开电池仓,创建换电记录 """
        car_id, auth_info = valid_data
        op_man_id = auth_info["opManId"]
        logger.info('open_bat_box opManId:', op_man_id, "carId:", car_id)

        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        operator = MovingVehicleBase().check_and_get_operator(op_man_id, service_id)

        # 是否达到最低换电阈值
        if not is_ble:
            self.check_lowest_battery(imei, car_id, service_id)  # 因为蓝牙获取权限前已经判断过一次了

        # 设置换电状态
        self.nx_lock(DEVICE_STATE_BATTERY.format(imei=imei), timeout=60, promt="已经在换电中")
        if not is_ble:
            res = send_cmd(open_batbox_cmd(imei))
            if not res or res.get("code", None) != 0:
                # 如果开启电池仓延迟了，再次查下电池仓状态，如果电池仓已开启则正常生成换电单
                battery_lock = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei),
                                                                "batteryLock")
                if battery_lock != "false":
                    dao_session.redis_session.r.delete(DEVICE_STATE_BATTERY.format(imei=imei))
                    raise MbException("操作失败,建议蓝牙操作", ErrorType.OPEN_BATBOX_FAIL)
        self.create_change_battery_record(imei, car_id, service_id, op_man_id, operator.name)
        return "操作成功"

    def close_bat_box_cmd(self, valid_data):
        """ 打开换电仓命令 """
        car_id, auth_info = valid_data
        op_man_id = auth_info["opManId"]
        logger.info('closeBatBox/cmd opManId:', op_man_id, "carId:", car_id)

        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        res = send_cmd(open_batbox_cmd(imei))
        if not res or res.get("code", None) != 0:
            raise MbException("命令下发失败", ErrorType.CLOSE_BATBOX_FAIL)
        else:
            # 最后一个人和车
            dao_session.session().query(XcEbike2ChangeBattery).filter(
                XcEbike2ChangeBattery.ebikeNo == car_id,
                XcEbike2ChangeBattery.phone == op_man_id,
                XcEbike2ChangeBattery.openBatBoxTime > (datetime.now() - timedelta(days=1))
            ).update({"closeBatBoxTime": datetime.now(), "updatedAt": datetime.now()})
            dao_session.session().commit()
            return "命令下发成功"

    def close_bat_box(self, valid_data):
        """ 关闭换电仓 """
        car_id, auth_info, ticket_no, aftervoltage = valid_data
        op_man_id = auth_info["opManId"]
        logger.info('closeBatBox/cmd opManId:', op_man_id, "carId:", car_id)

        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("车辆未绑定盒子")
        # 查询设备是否能上报soc
        bms_sn = dao_session.redis_session.r.get(DEVICE_REPORT_SOC.format(imei=imei))
        if bms_sn:
            # 恢复电池BMS为闲置
            dao_session.session().query(XcEbikeBMS).filter_by(sn=bms_sn).update(
                **{"state": 0, "timestamp": datetime.now()})

            # 记录日志
            _temp_record = json.dumps({"userId": op_man_id, "imei": imei, 'carId': car_id, "sn": bms_sn})
            logger.biz(f"accessLog, url: '/tools/closeBatBox', method: 'POST', basicInfo: '', params: {_temp_record}")

        # 权限校验
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        if not service_id:
            raise MbException("设备找不到服务区")
        operator = MovingVehicleBase.check_and_get_operator(op_man_id, service_id)

        voltage = 48000
        rest_battery = 100  # 剩余电量
        device_info = dao_session.redis_session.r.hgetall(IMEI_BINDING_DEVICE_INFO.format(imei=imei))
        if device_info["isDisconnect"] == "1":
            # 设备下线下所有换电都成功
            rest_battery = 100
            voltage = 50000

        # 如果传了电压则,使用传入的电压并保存,否则使用查询的电压
        if self.exists_param(aftervoltage) and aftervoltage:
            dao_session.redis_session.r.hset(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage", aftervoltage)
            rest_battery = MovingVehicleBase().get_rest_battery(aftervoltage, imei, car_id)
            voltage = aftervoltage
        else:
            res = send_cmd(lookfor_battery_and_back_seat(imei))
            if not res or res.get("code", None) != 0:
                self.update_finish_change_battery_record(car_id, op_man_id, 0)
                logger.biz(f"[routes/tools/closeBatBox][{operator.name}][{op_man_id}][{car_id}][{service_id}][state=0]")
                raise MbException("未检测到电压恢复，请重试")
            else:
                voltage = res["result"]["voltageMv"]
                logger.info("device voltage: ", voltage)
                rest_battery = MovingVehicleBase().get_rest_battery(voltage, imei, car_id)

        # 是否高于电量阀值
        plan = dao_session.redis_session.r.get(CAR_BINDING_BATTERY_NAME.format(**{"car_id": car_id}))
        threshold = (plan and int(json.loads(plan).get("threshold", 0))) or cfg["threshold"][
            "LOW_VOLTAGE_STATIC_THRESHOLD"]
        if rest_battery < threshold:
            logger.biz(f"[routes/tools/closeBatBox][{operator.name}][{op_man_id}][{car_id}][{service_id}][state=0]")
            self.update_finish_change_battery_record(car_id, op_man_id, 0, rest_battery)
            raise MbException(f"电池电量低: {math.floor(voltage / 1000)}V")
        else:
            logger.biz(f"[routes/tools/closeBatBox][{operator.name}][{op_man_id}][{car_id}][{service_id}][state=1]")
            self.update_finish_change_battery_record(car_id, op_man_id, 1, rest_battery)
            dao_session.redis_session.r.delete(DEVICE_STATE_BATTERY.format(imei=imei))

            # 换电后闭合所有工单
            self.clean_up_device_abnormal_infos(imei)
            return voltage

    def open_bat_box_permission(self, valid_data):
        car_id, auth_info = valid_data
        op_man_id = auth_info["opManId"]
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        MovingVehicleBase.check_and_get_operator(op_man_id, service_id)

        # 是否达到最低换电阈值
        self.check_lowest_battery(imei, car_id, service_id)
        return "获取权限成功"

    def permission_ble(self, valid_data):
        car_id, auth_info = valid_data
        op_man_id = auth_info["opManId"]
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
        MovingVehicleBase.check_and_get_operator(op_man_id, service_id)
        return "获取权限成功"

    def sneak_order_info(self, valid_data):
        """
        :param valid_data:
        :return: {
        "serviceId":"7",
        "imei":"861251057285907",
        "state":"1",
        "agentId":2,
        "orderId":1001132452,
        "reportedUserId":"6078d78937d5a300019452ac",
        "phone":"18502769001",
        "reportedUserName":"苟媛丽",
        "itinId":1126614,
        "startLng":112.241101,
        "startLat":30.332142,
        "endLat":30.339417,
        "endLng":112.255768,
        "startTime":"2021-07-16 08:22:58",
        "endTime":"2021-07-16 08:31:05"}}
        """
        car_id, = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        device_info = dao_session.redis_session.r.hgetall(IMEI_BINDING_DEVICE_INFO.format(imei=imei))
        device_state = MovingVehicleBase().get_state(imei)
        service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))

        res = {
            "serviceId": service_id,
            "imei": imei,
            "state": device_state,
            "agentId": device_info.get("agentId", 2)
        }
        if device_state == DeviceState.RIDING.value:
            user_id = dao_session.redis_session.r.get(DEVICE_RIDING_USER_ID.format(imei=imei))
            user_info = dao_session.sub_session().query(XcEbikeUsrs2).filter_by(id=user_id).one()
            res["phone"] = user_info.phone
            res["reportedUserId"] = user_id
            res["reportedUserName"] = json.loads(user_info.personInfo).get("name", "")
            res["itinId"] = ""
            res["orderId"] = ""
            res["startLng"] = device_info["startLng"]
            res["startLat"] = device_info["startLat"]
            res["endLat"] = ""
            res["endLng"] = ""
            res["startTime"] = datetime.fromtimestamp(device_info["startTime"]).strftime("%Y-%m-%d %H:%M:%S")
            res["endTime"] = ""
        else:
            recent_itinerary = dao_session.sub_session().query(XcEbikeDeviceItinerary).filter_by(imei=imei) \
                .order_by(XcEbikeDeviceItinerary.createdAt.desc()).first()
            if recent_itinerary:
                user_info = dao_session.sub_session().query(XcEbikeUsrs2).filter_by(id=recent_itinerary.userId).one()
                res["phone"] = user_info.phone
                res["reportedUserId"] = recent_itinerary.userId
                res["reportedUserName"] = json.loads(user_info.personInfo).get("name", "")
                res["itinId"] = recent_itinerary.itinId
                res["orderId"] = recent_itinerary.orderId
                res["startLng"] = float(recent_itinerary.startLng)
                res["startLat"] = float(recent_itinerary.startLat)
                res["endLat"] = float(recent_itinerary.endLat)
                res["endLng"] = float(recent_itinerary.endLng)
                res["startTime"] = recent_itinerary.startTime.strftime("%Y-%m-%d %H:%M:%S")
                res["endTime"] = recent_itinerary.endTime.strftime("%Y-%m-%d %H:%M:%S")
        return res

    def create_sneak(self, valid_data):
        service_id, auth_info, car_id, report_user_id, report_user_name, \
        report_user_phone, report_type, imei, auth_info, itin_id, desc, imgs = valid_data
        agent_id = auth_info["agentId"]
        try:
            op_man_name = dao_session.session().query(XcOpman.name).filter_by(
                opManId=auth_info["opManId"]).scalar()
            one = XcEbike2Sneak(**{
                "imei": imei,
                "itinId": itin_id,
                "ebikeNo": car_id,
                "agentId": agent_id,
                "serviceId": service_id,
                "reportedUserId": report_user_id,
                "reportedUserName": report_user_name,
                "reportedUserPhone": report_user_phone,
                "reportManId": auth_info["opManId"],
                "reportType": json.dumps(report_type),
                "description": desc,
                "imgs": json.dumps(imgs),
                "reportManName": op_man_name,
                "reportNo": "".join([random.choice("0123456789") for i in range(10)]),  # 10位随机数
                "checkResult": 0

            })
            dao_session.session().add(one)
            dao_session.session().commit()
        except Exception as ex:
            dao_session.session().rollback()
            logger.info('/ebike/v2/tools/sneak failed: ', ex)
            raise MbException("创建举报单失败")
        return "创建举报单成功"

    def sneak_list(self, valid_data):
        op_man_id, page, size = valid_data
        many = dao_session.sub_session().query(XcEbike2Sneak.id, XcEbike2Sneak.reportNo, XcEbike2Sneak.reportedUserName,
                                               XcEbike2Sneak.remark, XcEbike2Sneak.createAt, XcEbike2Sneak.checkAt,
                                               XcEbike2Sneak.checkResult).filter_by(reportManId=op_man_id).order_by(
            XcEbike2Sneak.createAt.desc()).limit(size).offset(page * size).all()
        return [{
            "id": one.id,
            "reportNo": one.reportNo,
            "reportedUserName": one.reportedUserName,
            "remark": one.remark,
            "createAt": one.createAt.strftime("%Y-%m-%d %H:%M:%S"),
            "checkAt": one.checkAt and one.checkAt.strftime("%Y-%m-%d %H:%M:%S"),
            "checkResult": one.checkResult
        } for one in many]

    def sneak_info(self, valid_data):
        """
        :param valid_data:
        :return: {
        "reportInfo":{
        "checkResult":0,
        "remark":null,
        "reportType":{
        "otherType":"",
        "type":["11"]},
        "description":"骑车也不给钱",
        "reportNo":"5090087438",
        "createAt":"2021-07-07 11:37:00",
        "imgs":["https://chuduadmin.xiaoantech.com/inspectReport/1625629017835-1.png"],
        "handleType":0,
        "handleDescription":null,
        "checkAt":"Invalid date",
        "checkManId":null,
        "checkManName":null},
        "reportedOrderInfo":{"reportedUserName":"许晶",
        "ebikeNo":"100601321",
        "imei":"861251057181338",
        "state":"1","itinId":"920007",
        "startLng":112.196892,
        "startLat":30.353282,
        "endLat":30.353163,
        "endLng":112.197039,
        "startTime":"2021-07-05 00:00:00"
        }}
        """
        _id, = valid_data
        try:
            sneak_info = dao_session.sub_session().query(XcEbike2Sneak).filter_by(id=_id).one()
            res = {"reportInfo": {}, "reportedOrderInfo": {}}
            res["reportInfo"] = {
                "checkResult": sneak_info.checkResult,
                "remark": sneak_info.remark,
                "reportType": json.loads(sneak_info.reportType),
                "description": sneak_info.description,
                "reportNo": sneak_info.reportNo,
                "createAt": sneak_info.createAt.strftime("%Y-%m-%d %H:%M:%S"),
                "imgs": json.loads(sneak_info.imgs),
                "handleType": sneak_info.handleType,
                "handleDescription": sneak_info.handleDescription,
                "checkAt": sneak_info.checkAt.strftime("%Y-%m-%d %H:%M:%S"),
                "checkManId": sneak_info.checkManId,
                "checkManName": sneak_info.checkManName
            }
            res["reportedOrderInfo"] = {
                "reportedUserName": sneak_info.reportedUserName,
                "ebikeNo": sneak_info.ebikeNo,
                "imei": sneak_info.imei,
                "state": MovingVehicleBase().get_state(sneak_info.imei),
                "itinId": sneak_info.itinId
            }
            # 没有行程编号的情况下说明骑行没结束
            if len(sneak_info.itinId) == 0:
                recent_itinerary = dao_session.redis_session.r.hgetall(
                    DEVICE_ITINERARY_INFO.format(imei=sneak_info.imei))
                if recent_itinerary:
                    res["reportedOrderInfo"]["startLat"] = float(recent_itinerary["startLat"])
                    res["reportedOrderInfo"]["startLng"] = float(recent_itinerary["startLng"])
                    res["reportedOrderInfo"]["endLat"] = float(recent_itinerary["curLat"])
                    res["reportedOrderInfo"]["endLng"] = float(recent_itinerary["curLng"])
                    res["reportedOrderInfo"]["startTime"] = datetime.fromtimestamp(
                        recent_itinerary["startTime"]).strftime(
                        "%Y-%m-%d %H:%M:%S")
                    res["reportedOrderInfo"]["endTime"] = ""
            else:
                recent_itinerary = dao_session.sub_session().query(XcEbikeDeviceItinerary) \
                    .filter_by(itinId=sneak_info.itinId) \
                    .order_by(XcEbikeDeviceItinerary.createdAt.desc()).first()
                if recent_itinerary:
                    res["reportedOrderInfo"]["startLat"] = float(recent_itinerary.startLat)
                    res["reportedOrderInfo"]["startLng"] = float(recent_itinerary.startLng)
                    res["reportedOrderInfo"]["endLat"] = float(recent_itinerary.endLat)
                    res["reportedOrderInfo"]["endLng"] = float(recent_itinerary.endLng)
                    res["reportedOrderInfo"]["startTime"] = recent_itinerary.startTime.strftime("%Y-%m-%d %H:%M:%S")
                    res["reportedOrderInfo"]["endTime"] = recent_itinerary.endTime.strftime("%Y-%m-%d %H:%M:%S")
            return res
        except Exception:
            logger.info("获取举报单详情失败:", Exception)
            raise MbException("获取举报单详情失败")

    def create_repair(self, valid_data):
        """
        :param repair_info: [{'fixReason': '居然加私锁\\n',
        'photo': ['https://chuduadmin.xiaoantech.com/repair/100600001/1627285134648_0.png'],
        'type': ['8', '13', 14]}]}
        :param valid_data:
        :return:
        """
        car_id, agent_id, op_man_id, repair_info = valid_data
        self.nx_lock(REPAIR_STATE_CARID.format(car_id=car_id), timeout=2)
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("车辆号不存在")

        device_state = MovingVehicleBase().get_state(imei)
        if device_state == DeviceState.RIDING.value:
            raise MbException("用户预约或骑行中,报修失败")
        if not repair_info.get("type", None):
            raise MbException("报修类型错误")

        try:
            if isinstance(repair_info["type"], list):
                fix_config = ConfigService().get_router_content(ConfigName.FIX.value, None)
                fix_dict = {x["type"]: x["name"] for x in fix_config}

                params = {
                    "imei": imei,
                    "ebikeNo": car_id,
                    "type": repair_info["type"][0],
                    "fixReason": repair_info["fixReason"],
                    "photo": str(repair_info["photo"]),
                    "reportUsrId": op_man_id,
                    "extraInfo": ",".join([fix_dict[tp] for tp in repair_info["type"]])
                }
            else:
                # 上下写法本该统一
                params = {
                    "imei": imei,
                    "ebikeNo": car_id,
                    "type": repair_info["type"],
                    "fixReason": repair_info["fixReason"],
                    "photo": repair_info["photo"],
                    "reportUsrId": op_man_id,
                    "extraInfo": repair_info["fixReason"]
                }
            one = XcEbikeFixTickets2(**params)
            dao_session.session().add(one)
            dao_session.session().commit()
            dao_session.redis_session.r.set(DEVICE_STATE.format(imei=imei), DeviceState.BROKEN.value)
        except Exception:
            raise MbException("创建报修单失败")
        return "报修成功"

    def ebike_car_id(self, valid_data):
        imei_list, = valid_data
        many = dao_session.sub_session().query(XcEbike2BindingInfo.imei, XcEbike2BindingInfo.carId) \
            .filter(XcEbike2BindingInfo.imei.in_(imei_list)).all()
        return [{"imei": one.imei, "carId": one.carId} for one in many]

    def move_ebike(self, valid_data):
        imei, agent_id, auth_info, locked = valid_data
        op_man_id = auth_info["opManId"]
        device_state = MovingVehicleBase().get_state(imei)
        if device_state == DeviceState.RIDING.value:
            raise MbException("用户预约或骑行中，无法挪车")
        car_id = dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=imei))
        if locked == 0:
            # 开始挪车
            res = send_cmd(acc_fn(imei, True, True))
            if not res or res.get("code", None) != 0:
                raise MbException("开锁失败")

            # 增加挪车中的标志
            MovingVehicleBase().tag_move(car_id, imei, op_man_id)
        else:
            # 完成挪车
            res = send_cmd(lock_fn(imei, True))
            if not res or res.get("code", None) != 0:
                raise MbException("开锁失败")
            service_id = dao_session.redis_session.r.get(IMEI_GFENCE_BIND.format(imei=imei))
            MovingVehicleBase().move_close_alarm_tikcet(imei, service_id, car_id, device_state, op_man_id, op_man_id)
            # 清除挪车中的标志
            MovingVehicleBase().untag_move(car_id, imei, op_man_id)
        return "挪车操作成功"

    def move_vehicle_sum(self, valid_data):
        """ 自定义查询挪车记录 """
        service_id, size, page, car_id, phone, is_finish, moving_type, name, start_time, \
        end_time, check_result, before_state, no_order_hour, finish_start_time, finish_end_time = valid_data
        m = dao_session.sub_session().query(XcEbikeMoveOperation)
        if self.exists_param(start_time) and self.exists_param(end_time):
            m = m.filter(XcEbikeMoveOperation.createdAt.between(start_time / 1000, end_time / 1000))
        if self.exists_param(finish_start_time) and self.exists_param(finish_end_time):
            m = m.filter(XcEbikeMoveOperation.endTime.between(finish_start_time / 1000, finish_end_time / 1000))
        if self.exists_param(service_id):
            m = m.filter(XcEbikeMoveOperation.serviceAreaId == service_id)
        if self.exists_param(phone) and phone:
            m = m.filter(XcEbikeMoveOperation.phone == phone)
        if self.exists_param(is_finish):
            m = m.filter(XcEbikeMoveOperation.isFinish == is_finish)
        if self.exists_param(moving_type):
            m = m.filter(XcEbikeMoveOperation.movingType == moving_type)
        if self.exists_param(name) and name:
            m = m.filter(XcEbikeMoveOperation.operator == name)
        if self.exists_param(check_result):
            m = m.filter(XcEbikeMoveOperation.checkResult == check_result)

        # XcEbikeOnemoveRecord
        if self.exists_param(car_id) and car_id:
            m = m.filter(XcEbikeOnemoveRecord.carId == car_id)
        if self.exists_param(before_state):
            if before_state == 1:
                m = m.filter(XcEbikeOnemoveRecord.isOutOfServiceZone == 1)  # 服务区外 0不是 1是
            elif before_state == 2:
                # 站点外 isParkingZone 数据库中存的相应的站点ID 有站点ID则是在该站点 为null则是站点外。。。
                m = m.filter(XcEbikeOnemoveRecord.isParkingZone.is_(None))
            elif before_state == 3:
                # 禁停区内 isInNoParkingZone 数据库中存的相应的禁停区ID 有禁停区站点ID则是在该禁停区 为null则是站点外。。。
                m = m.filter(XcEbikeOnemoveRecord.isInNoParkingZone.isnot(None))
        if self.exists_param(no_order_hour):
            m = m.filter(XcEbikeOnemoveRecord.noOrderTime.__ge__(no_order_hour))
        m.filter(XcEbikeMoveOperation.movingNumber.__gt__(0))
        many = m.limit(size).offset((page - 0) * size).all()
        res = []
        for one in many:
            obj_dict = orm_to_dict(one, XcEbikeMoveOperation)
            obj_dict["positionLimit"] = []
            obj_dict["records"] = []
            obj_dict["costTime"] = obj_dict["endTime"] - obj_dict["startTime"]
            res.append(obj_dict)
        return {"sum": len(res), "list": res}

    def get_journey(self, valid_data):
        imei, start_time, end_time = valid_data
        return imei_trail_from_tsdb(imei, start_time / 1000, end_time / 1000)

    def change_battery_sum(self, valid_data):
        """ 自定义查询换电记录 """
        agent_id, car_id, start_time, end_time, finish_start_time, finish_end_time, phone, name, state, \
        after_battery, last_time, battery_diff, check_result, service_id, size, page = valid_data

        m = dao_session.sub_session().query(XcEbike2ChangeBattery)
        if self.exists_param(start_time) and self.exists_param(end_time):
            m = m.filter(XcEbike2ChangeBattery.createdAt.between(datetime.fromtimestamp(start_time / 1000),
                                                                 datetime.fromtimestamp(end_time / 1000)))
        if self.exists_param(finish_start_time) and self.exists_param(finish_end_time):
            m = m.filter(
                XcEbike2ChangeBattery.closeBatBoxTime.between(datetime.fromtimestamp(finish_start_time / 1000),
                                                              datetime.fromtimestamp(finish_end_time / 1000)))
        if self.exists_param(car_id) and car_id:
            m = m.filter(XcEbike2ChangeBattery.ebikeNo == car_id)
        if self.exists_param(phone) and phone:
            m = m.filter(XcEbike2ChangeBattery.phone == phone)
        if self.exists_param(name) and name:
            m = m.filter(XcEbike2ChangeBattery.opMan == name)
        if self.exists_param(state):
            m = m.filter(XcEbike2ChangeBattery.state == state)
        if self.exists_param(after_battery) and after_battery:
            m = m.filter(XcEbike2ChangeBattery.restBatteryAfter.__gt__(after_battery))
        if self.exists_param(last_time) and last_time:
            m = m.filter(XcEbike2ChangeBattery.changeBatteryLastTime.__lt__(last_time))
        if self.exists_param(battery_diff) and battery_diff:
            m = m.filter(XcEbike2ChangeBattery.restBatteryDiff.__gt__(battery_diff))
        if self.exists_param(check_result):
            m = m.filter(XcEbike2ChangeBattery.checkResult == check_result)
        if self.exists_param(service_id):
            m = m.filter(XcEbike2ChangeBattery.serviceId == service_id)
        many = m.order_by(XcEbike2ChangeBattery.closeBatBoxTime.desc()).limit(size).offset((page - 0) * size).all()
        res = []
        for one in many:
            obj_dict = orm_to_dict(one, XcEbike2ChangeBattery)
            if obj_dict["state"] == 0:
                obj_dict["checkResult"] = 2
            last_second = obj_dict["changeBatteryLastTime"] or 0
            obj_dict["changeBatteryLastTime"] = "{}:{}:{}".format(last_second // 3600, last_second // 60 % 60,
                                                                  last_second % 60)
            obj_dict["type"] = "换电"
            res.append(obj_dict)
        return {"sum": len(res), "list": res}

    def get_all_voltage_program(self):
        """
        获取所有电压方案
        """
        many = dao_session.sub_session().query(XcVoltageProgram).order_by(XcVoltageProgram.createdAt.desc()).all()
        res = [orm_to_dict_tz(one, XcVoltageProgram) for one in many]
        return {"count": len(res), "rows": res}

    def query_device_sate(self, valid_data):
        car_id, = valid_data
        imei = dao_session.redis_session.r.get(CAR_2_IMEI_KEY.format(car_id=car_id))
        if not imei:
            raise MbException("车辆不存在")
        device_state = MovingVehicleBase().get_state(imei)
        voltage = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=imei), "voltage")
        rest_battery = MovingVehicleBase().get_rest_battery(voltage, imei, car_id)
        return {"state": device_state, "restBattery": rest_battery}

    # def get_buried_logs(self, start=time.mktime(date.today().timetuple())
    #                     , end=time.mktime((date.today() + timedelta(days=1)).timetuple()), metric='*', limit=50,
    #                     offset=0):
    #     """ 解析埋点数据 """
    #     #  日志搜索
    #     raw_log = get_raw_logs(
    #         {"from": start, "to": end, "logStoreName": 'json4biz-log', "limit": limit, "offset": offset, "query":)
    #     return None

    def authorized_gfence_service(self, user_id):
        res = []
        try:
            # 获取用户的权限和代理商
            one = dao_session.sub_session().query(XcRole.roleId, XcRole.agentId) \
                .join(XcOpman, XcRole.roleId == XcOpman.roleId) \
                .join(XcAgent, XcAgent.agentId == XcRole.agentId) \
                .filter(XcOpman.opManId == user_id, XcOpman.isEnable == True, XcRole.isEnable == True,
                        XcAgent.isEnable == True).first()

            if one:
                # 角色所有的服务区权限
                role_id, agent_id = one.roleId, one.agentId
                many = dao_session.sub_session().query(XcRoleProperty.propertyId).filter(
                    XcRoleProperty.roleId == role_id).all()
                property_ids = [one[0] for one in many]

                # 过滤属于当前代理商的财务id
                # 如果为主代理商，则无需做以下财务 - 代理商所属关系的过滤
                if agent_id > 2:
                    many = dao_session.sub_session().query(XcEbikeGfence2.id).filter(
                        XcEbikeGfence2.agentId == agent_id).all()
                    gfence_ids = [one[0] for one in many]
                    property_ids = [property_id for property_id in property_ids if property_id in gfence_ids]

                # 获取服务区的详细信息
                many = dao_session.sub_session().query(XcEbikeGfence2.id, XcEbikeGfence2.name, XcEbikeGfence2.centerLat,
                                                       XcEbikeGfence2.centerLng).filter(
                    XcEbikeGfence2.type == GfenceType.AT_SERVICE.value, XcEbikeGfence2.id.in_(property_ids)).all()
                for one in many:
                    res.append({"name": one.name, "centerLng": float(one.centerLng), "centerLat": float(one.centerLat),
                                "id": one.id})
            return {"list": res, "count": len(res)}
        except Exception:
            raise MbException("获取自己可以分配的服务区失败")

    def device_ticket_info(self, valid_data, origin_type):
        size, page, agent_id, tp, car_id, service_id = valid_data
        ticket_list = []
        query_field = ["imei", "gsmSignal", "defend", 'acc', 'wgs84Lat',
                       'wgs84Lng', 'lat', 'lng', 'timestamp',
                       'batteryLock', 'isPowerExist', 'helmetLock',
                       'state', 'carId']
        if origin_type == OriginType.COMPLAINT.value:
            m = dao_session.sub_session().query(XcEbikeUserTicket, XcEbikeUserOrder.cost, XcEbikeUserOrder.imei,
                                                XcEbikeDeviceItinerary.startTime, XcEbikeDeviceItinerary.endTime) \
                .join(XcEbikeUserOrder, XcEbikeUserOrder.orderId == XcEbikeUserTicket.orderId) \
                .join(XcEbikeDeviceItinerary, XcEbikeUserOrder.orderId == XcEbikeDeviceItinerary.orderId) \
                .filter(XcEbikeUserTicket.state.in_(FixState.unfixed_list()))
            many = m.order_by(XcEbikeUserTicket.createdAt.desc()) \
                .limit(size).offset((page - 1) * size)
            for one in many:
                imei = one.imei
                query_result = dao_session.redis_session.r.hmget(IMEI_BINDING_DEVICE_INFO.format(imei=imei),
                                                                 query_field)
                device_info = dict(zip(query_field, query_result))
                device_info["state"] = MovingVehicleBase().get_state(imei)
                device_info["carId"] = dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=imei))
                ticket_info = orm_to_dict(one, XcEbikeUserTicket)

                order_info = {
                    "cost": one.cost,
                    "startTime": one.startTime.strftime("%Y-%m-%d %H:%M:%S"),
                    "endTime": one.endTime.strftime("%Y-%m-%d %H:%M:%S")
                }
                ticket_list.append({"device": device_info, "ticket": ticket_info, "orderInfo": order_info})
        else:
            if origin_type == OriginType.ALARM.value:
                m = dao_session.sub_session().query(XcEbikeAlarmTickets2) \
                    .join(XcEbike2Device, XcEbike2Device.imei == XcEbikeAlarmTickets2.imei) \
                    .filter(XcEbikeAlarmTickets2.state.in_(FixState.unfixed_list()),
                            XcEbike2Device.serviceId == service_id) \
                    .order_by(XcEbikeAlarmTickets2.createdAt.desc())
                if self.exists_param(tp):
                    m = m.filter(XcEbikeAlarmTickets2.type == tp)
                if self.exists_param(car_id) and car_id:
                    m = m.filter(XcEbikeAlarmTickets2.ebikeNo == car_id)
                many = m.limit(size).offset((page - 1) * size).all()
                for one in many:
                    imei = one.imei
                    query_result = dao_session.redis_session.r.hmget(IMEI_BINDING_DEVICE_INFO.format(imei=imei),
                                                                     query_field)
                    device_info = dict(zip(query_field, query_result))
                    device_info["state"] = MovingVehicleBase().get_state(imei)
                    device_info["carId"] = dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=imei))
                    ticket_info = orm_to_dict(one, XcEbikeAlarmTickets2)
                    ticket_list.append({"device": device_info, "ticket": ticket_info, "orderInfo": {}})

            elif origin_type == OriginType.FIX.value:
                m = dao_session.sub_session().query(XcEbikeFixTickets2) \
                    .join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei) \
                    .filter(XcEbikeFixTickets2.state.in_(FixState.unfixed_list()),
                            XcEbike2Device.serviceId == service_id) \
                    .order_by(XcEbikeFixTickets2.createdAt.desc())
                if self.exists_param(tp):
                    m = m.filter(XcEbikeFixTickets2.type == tp)
                if self.exists_param(car_id) and car_id:
                    m = m.filter(XcEbikeFixTickets2.ebikeNo == car_id)
                many = m.limit(size).offset((page - 1) * size).all()
                for one in many:
                    imei = one.imei
                    # 优化接口只返回需要的数据
                    query_result = dao_session.redis_session.r.hmget(IMEI_BINDING_DEVICE_INFO.format(imei=imei),
                                                                     query_field)
                    device_info = dict(zip(query_field, query_result))
                    device_info["state"] = MovingVehicleBase().get_state(imei)
                    device_info["carId"] = dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=imei))
                    ticket_info = orm_to_dict(one, XcEbikeFixTickets2)
                    ticket_list.append({"device": device_info, "ticket": ticket_info, "orderInfo": {}})
        return ticket_list

    def check_picture(self, valid_data):
        """ 更新挪车前或者挪车后的图片 """
        record_id, back_picture, front_picture = valid_data
        params = {"frontPicture": front_picture, "backPicture": front_picture, "updatedAt": datetime.now()}
        params = self.remove_empty_param(params)
        try:
            dao_session.session().query(XcEbikeMoveOperation).filter_by(recordId=record_id) \
                .update(params)
            dao_session.session().commit()
        except:
            pass
