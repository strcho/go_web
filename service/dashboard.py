import concurrent.futures
import json
import sys
import time
import zipfile
from datetime import timedelta
from io import BytesIO

from sqlalchemy import sql, or_

from model.all_model import *
from mbutils import cfg
from mbutils import dao_session, logger
from utils.aliyun_func import AliyunFunc
from utils.aliyun_oss import AliyunOSS
from utils.constant.user import UserState
from utils.constant.redis_key import EXPORT_EXCEL_LOCK, SERVICE_DEVICE_All_IMEIS, IMEI_BINDING_DEVICE_INFO, \
    IMEI_2_CAR_KEY, \
    CAR_BINDING_BATTERY_NAME, SERVICE_DEVICE_STATE, LOCK_CAR_TIME, USER_STATE_COUNT, EXPORT_SETTLEMENT_REPORT_LOCK, \
    BIG_SCREEN_USER_INFO_TREE
from . import MBService


class DashboardService(MBService):

    def get_service_id(self, op_area_ids: list, agent_ids: list):
        """
         获取service_id
        :param op_area_ids:
        :param agent_ids:
        :return:
        """
        if op_area_ids:
            op_area_ids = tuple([op_area_ids]) if not isinstance(op_area_ids, list) else tuple(op_area_ids)
            # agent_ids = tuple([agent_ids]) if not isinstance(agent_ids, list) else tuple(agent_ids)
        else:
            if agent_ids:
                agent_ids = tuple([agent_ids]) if not isinstance(agent_ids, list) else tuple(agent_ids)
                service_info = dao_session.sub_session().query(XcEbikeGfence2.id).filter(XcEbikeGfence2.type == 1,
                                                                                         XcEbikeGfence2.agentId.in_(
                                                                                             agent_ids),
                                                                                         XcEbikeGfence2.deletedAt.is_(
                                                                                             None)
                                                                                         ).all()
                service_list = [r.id for r in service_info if r]
                op_area_ids = tuple(service_list)
            # 代理商全选的时候没有agent_ids
            else:
                # 获取所有可用代理商
                # agent_info = dao_session.sub_session().query(XcAgent.agentId).filter(XcAgent.isEnable == 1).all()
                # agent_ids = [r.agentId for r in agent_info if r]
                service_info = dao_session.sub_session().query(XcEbikeGfence2.id).filter(XcEbikeGfence2.type == 1,
                                                                                         # XcEbikeGfence2.agentId.in_(
                                                                                         #     agent_ids),
                                                                                         XcEbikeGfence2.deletedAt.is_(
                                                                                             None)
                                                                                         ).all()
                service_list = [r.id for r in service_info if r]
                op_area_ids = tuple(service_list)
        return op_area_ids

    def date_list(self, valid_data: tuple):
        """
        返回时间区列表、开始时间、结束时间
        :param valid_data:
        :return:
        """
        begin_time, end_time = valid_data
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)

        date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                     range((end_date - begin_date).days + 1)]
        return date_list, begin_date, end_date

    def get_vehicle_statistics(self, op_area_ids: tuple):
        """
        根据服务区id取得车辆状态数量
        :param op_area_ids:服务区id
        :return:
        """

        """redis xc_ebike_serviceGfence_{service_id}_{stateDevices}{state}数据不可信，需在device表筛选一遍"""
        with dao_session.redis_session.r.pipeline(transaction=False) as vehicle_pipeline:
            for r in op_area_ids:
                vehicle_pipeline.smembers(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": r}))
                vehicle_pipeline.smembers(SERVICE_DEVICE_STATE.format(**{"service_id": r, "state": 2}))
                vehicle_pipeline.smembers(SERVICE_DEVICE_STATE.format(**{"service_id": r, "state": 1}))
                vehicle_pipeline.smembers(SERVICE_DEVICE_STATE.format(**{"service_id": r, "state": 3}))
            res = vehicle_pipeline.execute()

        total_imei, riding_imei, ready_imei, broken_imei = set(), set(), set(), set()
        for k, v in enumerate(res):
            if k % 4 == 0:
                total_imei = total_imei.union(v)
            elif k % 4 == 1:
                riding_imei = riding_imei.union(v)
            elif k % 4 == 2:
                ready_imei = ready_imei.union(v)
            elif k % 4 == 3:
                broken_imei = broken_imei.union(v)
        total_imei = tuple(list(total_imei))
        riding_imei = tuple(list(riding_imei))
        ready_imei = tuple(list(ready_imei))
        broken_imei = tuple(list(broken_imei))

        total = len(total_imei)

        riding_sum = dao_session.sub_session().query(sql.func.count(XcEbike2Device.imei).label("riding_sum")).filter(
            XcEbike2Device.imei.in_(riding_imei), XcEbike2Device.serviceId.in_(op_area_ids)).first().riding_sum

        ready_sum = dao_session.sub_session().query(sql.func.count(XcEbike2Device.imei).label("ready_sum")).filter(
            XcEbike2Device.imei.in_(ready_imei), XcEbike2Device.serviceId.in_(op_area_ids)).first().ready_sum

        broken_sum = dao_session.sub_session().query(sql.func.count(XcEbike2Device.imei).label("broken_sum")).filter(
            XcEbike2Device.imei.in_(broken_imei), XcEbike2Device.serviceId.in_(op_area_ids)).first().broken_sum

        other = total - riding_sum - ready_sum - broken_sum
        vehicle_statistics = {
            "riding_sum": riding_sum,
            "ready_sum": ready_sum,
            "broken_sum": broken_sum,
            "other": other,
            "total": total
        }
        return vehicle_statistics

    def battery_type_4cell_num(self, voltage:float, battery_name:str):
        """
        根据电压和电池品牌算出剩余电压百分比
        :param voltage:电压
        :param battery_name:电池品牌
        :return:
        """
        battery_name = battery_name.upper()
        voltage = voltage / 1000
        # 星恒4814 20摄氏度 8A
        if battery_name == "XH_4814_20":
            if voltage < 39:
                return 0
            elif voltage < 42.72:
                return 0.002327253 * voltage ** 2 - 0.178566106 * voltage + 3.434750486 * 100
            elif voltage < 52.20:
                return 0.0000514789 * voltage ** 3 - 0.005120385 * voltage ** 2 + 0.241923471 * voltage - 4.977783815 * 100
            else:
                return 100
        # 星恒4812_20摄氏度
        elif battery_name == "XINGHENG_4812_20":
            if voltage < 45:
                return 0
            elif 45 <= voltage < 46.2:
                return 0.2713 * voltage ** 2 - 22.105 * voltage + 451.03
            elif 46.2 <= voltage <= 49:
                return 3.9544 * voltage ** 2 - 360.49 * voltage + 8223.7
            elif 49 <= voltage <= 50.72:
                return 20.195 * voltage - 935.65
            elif 50.72 <= voltage <= 52.33:
                return -4.8459 * voltage ** 2 + 505.31 * voltage - 13074
            else:
                return 100
        # 沃泰通48V18Ah电池
        elif battery_name == "WOTAITONG_48V_18AH":
            if voltage < 40.3:
                return 0
            elif 40.3 <= voltage < 50.543:
                return 0.0082 * voltage ** 3 - 1.0359 * voltage ** 2 + 43.881 * voltage - 621.07
            elif 50.543 <= voltage < 52.976:
                return 12.949 * voltage ** 2 - 1309 * voltage + 33093
            elif 52.976 <= voltage < 52.981:
                return 262.49 * voltage - 13810
            else:
                return 100
        # 慧橙4820电池方案
        elif battery_name == "HC4820":
            if voltage < 48:
                return 0
            elif voltage < 51.2:
                return 1.5625 * voltage - 75
            elif voltage < 53.7:
                return 3.626339959 * voltage ** 3 - 551.6503959 * voltage ** 2 + 27973.35298 * voltage - 472830.3554
            else:
                return 100
        # 慧橙4821电池方案
        elif battery_name == "HC":
            if 49.2 < voltage < 50.8:
                return -19.783 * voltage * voltage + 2008.3 * voltage - 50867
            elif 47 < voltage <= 49.2:
                return 7.3334 * voltage * voltage - 686.9 * voltage + 16090
            elif voltage <= 47:
                return 0
            else:
                return 100
        # 48v-卓能
        elif battery_name == "ZN_48V":
            if voltage < 45:
                return 0
            else:
                return 5.1017 * voltage * voltage - 457.84 * voltage + 10275
        # 卓能48v-夏-动态
        elif battery_name == "ZN_48V14A_SUMMER":
            if voltage <= 40:
                return 0
            elif voltage <= 44.75:
                return 2.1026 * voltage - 84.104
            elif voltage <= 45.8:
                return 9.0009 * voltage - 392.84
            elif voltage <= 47.31:
                return 2.8866 * voltage ** 3 - 376.08 * voltage ** 2 + 16293 * voltage - 234638
            elif voltage <= 48.267:
                return 10.471 * voltage - 405.41
            else:
                return 100
        # 卓能48v-夏-静态
        elif battery_name == "ZN_48V14A_SUMMER_S":
            if voltage <= 46.819:
                return 0
            elif voltage < 49.249:
                return 5.303 * voltage * voltage - 497.69 * voltage + 11677
            elif voltage < 49.376:
                return 154.01 * voltage - 7554
            elif voltage < 49.844:
                return 42.517 * voltage - 2049.7
            elif voltage < 50.185:
                return 87.977 * voltage - 4315.1
            else:
                return 100
        # 星恒48v24a
        elif battery_name == "ZN_48V24A":
            if voltage <= 48:
                return 0
            elif voltage < 54:
                return -0.3841 * voltage ** 3 + 58.601 * voltage ** 2 - 2960.1 * voltage + 49545
            else:
                return 100
        # 蕊驰4821
        elif battery_name == "RC_4821":
            if voltage <= 49.2:
                return 0
            elif voltage < 53:
                return 2.6385 * voltage - 129.82
            elif voltage < 54.12:
                return 8.8496 * voltage - 458.94
            elif voltage < 56:
                return -18.439 * voltage ** 2 + 2066.6 * voltage - 57818
            elif voltage < 58.3:
                return 4.1667 * voltage - 143.33
            else:
                return 100
        elif battery_name == "RC_4821_20_DYNAMIC":
            if voltage < 40.04:
                return 0
            elif voltage < 48.17:
                return 1.23 * voltage - 49.25
            elif voltage < 50.57:
                return 2.41442618 * voltage ** 3 - 342.7906777 * voltage ** 2 + 16219.12661 * voltage - 255734.3227
            elif voltage < 50.82:
                return 40 * voltage - 1942.8
            elif voltage < 53.08:
                return 4.4248 * voltage - 134.87
            else:
                return 100
        elif battery_name == "RC_4821_20_STATIC":
            if voltage < 49.45:
                return 0
            elif voltage < 51.64:
                return 4.5662 * voltage - 225.8
            elif voltage < 52.55:
                return 20.169 * voltage ** 2 - 2079.4 * voltage + 53607
            elif voltage < 53.17:
                return 655.74744140 * voltage ** 3 - 104021.9217 * voltage ** 2 + 5500370.2270 * voltage - 96947701.1900
            elif voltage < 53.22:
                return 400 * voltage - 21198
            elif voltage < 55.76:
                return 3.937 * voltage - 119.53
            else:
                return 100
        # 芯合4826
        elif battery_name == "XH_4826":
            if voltage < 38.736:
                return 0
            elif 38.736 <= voltage < 43.434:
                return 0.1843 * voltage ** 2 - 14.333 * voltage + 279.74
            elif 43.434 <= voltage < 47.071:
                return 3.7823 * voltage ** 2 - 330.04 * voltage + 7207
            elif 47.071 <= voltage < 53.649:
                return 7.379 * voltage - 294.16
            else:
                return 100
        # 星恒4820
        elif battery_name == "XH_4820":
            if voltage < 36.6:
                return 0
            elif voltage < 47.576:
                return 0.012441072 * voltage ** 3 - 1.45187084 * voltage ** 2 + 56.77620584 * voltage - 743.1441676
            elif voltage < 52.893:
                return 0.405265609 * voltage ** 4 - 81.64777922 * voltage ** 3 + 6167.390435 * voltage ** 2 - 206997.6298 * voltage + 2604504.084
            elif voltage < 54.229:
                return 2.842034198 * voltage ** 3 - 462.0753204 * voltage ** 2 + 25044.9406 * voltage - 452433.8266
            else:
                return 100
        # 星恒4814
        elif battery_name == "XH_4814":
            if voltage < 38.9441:
                return 0
            elif voltage < 44.3287:
                return 1.0019 * voltage - 38.593
            elif voltage < 52.7267:
                return 9.9381 * voltage - 439.05
            elif voltage < 54.106:
                return 10.218 * voltage - 452.36
            else:
                return 100
        # 铁锂48V
        elif battery_name == "TIELI_48V":
            if voltage < 40:
                return 0
            elif voltage < 49.629:
                return 0.9028 * voltage - 38.237
            elif voltage < 52.272:
                return 1.5061 * voltage ** 3 - 215.2 * voltage ** 2 + 10230 * voltage - 161750
            elif voltage < 52.618:
                return 99
            else:
                return 100
        # 智泰51.2v20Ah
        elif battery_name == "ZT_5120":
            if voltage < 44.55:
                return 0
            elif voltage < 51.27:
                return 1.4881 * voltage - 66.295
            elif voltage < 52.43:
                return 17.226 * voltage - 873.34
            elif voltage < 53.20:
                return -70.753 * voltage ** 2 + 7528.1 * voltage - 200177
            elif voltage < 53.35:
                return -1411.91 * voltage ** 2 + 150641.50 * voltage - 4018016.27
            else:
                return 100
        # 合佳HJ02002 48V15Ah电压公式
        elif battery_name == "HJ_02002":
            if voltage < 40.12:
                return 0
            elif voltage < 43.28:
                return 1.1789 * voltage ** 2 - 95.423 * voltage + 1932.9
            elif voltage < 52.19:
                return -0.7873 * voltage ** 2 + 85.048 * voltage - 2195.4
            else:
                return 100
        # 德亚48V
        elif battery_name == "DEYA_48V":
            if voltage < 40:
                return 0
            elif voltage < 45.4:
                return 0.1804 * voltage ** 2 - 14.519 * voltage + 292.38
            elif voltage < 46.204:
                return 5.9332 * voltage - 264.33
            elif voltage < 48.48:
                return 3.9223 * voltage ** 3 - 540.96 * voltage ** 2 + 24879 * voltage - 381538
            elif voltage < 48.517:
                return 218.64 * voltage - 10508.77
            elif voltage < 48.608:
                return 99
            else:
                return 100
        # 默认48v为13芯，兼容老代码
        elif battery_name == "48V":
            return (100 * (voltage * 1000 - 3366 * 13)) / (839 * 13)
        # 默认60v为16芯，兼容老代码
        elif battery_name == "60V":
            return (100 * (voltage * 1000 - 3366 * 16)) / (839 * 16)
        # 默认13芯
        else:
            return (100 * (voltage * 1000 - 3366 * 13)) / (839 * 13)

    def get_electricity_statistics(self, op_area_ids: tuple):
        # logger.info("[get_electricity_statistics] op_area_ids:{}".format(op_area_ids))
        # 获取当前选择服务区下的所有车辆imei  xc_ebike_serviceGfence_{service_id}_deviceCount SET
        with dao_session.redis_session.r.pipeline(transaction=False) as imei_pipeline:
            for r in op_area_ids:
                imei_pipeline.smembers(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": r}))
            imei_res = imei_pipeline.execute()
        imei = set()
        for i in imei_res:
            imei = imei.union(i)
        imei_list = list(imei)
        logger.info("[get_electricity_statistics] service_id:{} imei_list:{}".format(op_area_ids, imei_list))

        # 根据imei找出电压voltage   xc_ebike_device_info_{imei} HASH
        with dao_session.redis_session.r.pipeline(transaction=False) as device_pipeline:
            for i in imei_list:
                device_pipeline.hget(IMEI_BINDING_DEVICE_INFO.format(**{"imei": i}), "voltage")
            voltage_res = device_pipeline.execute()
        logger.info("[get_electricity_statistics] service_id:{} voltage_res:{}".format(op_area_ids, voltage_res))

        # 根据imei查car_id    xc_ebike_imeiCarBindings_{imei} STRING
        with dao_session.redis_session.r.pipeline(transaction=False) as car_pipeline:
            for i in imei_list:
                car_pipeline.get(IMEI_2_CAR_KEY.format(**{"imei": i}))
            car_res = car_pipeline.execute()
        # logger.info("[get_electricity_statistics] car_res:{}".format(car_res))

        # 根据carid查电池battery_name   xc_ebike_xc_battery_Name_{car_id} STRING
        with dao_session.redis_session.r.pipeline(transaction=False) as battery_name_pipeline:
            for c in car_res:
                battery_name_pipeline.get(CAR_BINDING_BATTERY_NAME.format(**{"car_id": c}))
            battery_name_res = battery_name_pipeline.execute()
        battery_name_list = list(map(lambda a: json.loads(a).get("batteryType", None) if a else a, battery_name_res))
        logger.info(
            "[get_electricity_statistics] service_id:{} battery_name_list:{}".format(op_area_ids, battery_name_list))

        # imei_voltage_dict = dict(zip(imei_list, voltage_res))
        rest_battery = []
        for k, v in enumerate(voltage_res):
            if not v:
                rest_battery.insert(k, 0)
                continue
            battery = 100 * (int(v) - 3366 * 13) / (839 * 13)
            # 根据电池品牌
            if battery_name_list[k]:
                battery = self.battery_type_4cell_num(float(v), battery_name_list[k])
            if battery > 100:
                rest_battery.insert(k, 100)
            elif battery <= 0:
                rest_battery.insert(k, 0)
            else:
                rest_battery.insert(k, battery)
        logger.info("[get_electricity_statistics] rest_battery:{}".format(rest_battery))
        gt_40 = len(list(filter(lambda r: True if r > 40 else False, rest_battery)))
        beetween_20_40 = len(list(filter(lambda r: True if 20 <= r <= 40 else False, rest_battery)))
        lt_20 = len(list(filter(lambda r: True if 0 <= r < 20 else False, rest_battery)))
        other = len(list(filter(lambda r: True if r < 0 else False, rest_battery)))
        total = gt_40 + beetween_20_40 + lt_20 + other

        electricity_statistics = {
            "gt_40": gt_40,
            "20_to_40": beetween_20_40,
            "lt_20": lt_20,
            "other": other,
            "total": total
        }
        return electricity_statistics

    def get_idle_statistics(self, op_area_ids: tuple):

        now = int(round(time.time() * 1000))
        # 获取闲置车辆信息  xc_ebike_{service_id}_lockCarTime ZSET
        with dao_session.redis_session.r.pipeline(transaction=False) as idle_pipeline:
            for r in op_area_ids:
                idle_pipeline.zrangebyscore(name=LOCK_CAR_TIME.format(**{"service_id": r}),
                                            min=now - 8 * 60 * 60 * 1000,
                                            max=now - 6 * 60 * 60 * 1000)
                idle_pipeline.zrangebyscore(name=LOCK_CAR_TIME.format(**{"service_id": r}),
                                            min=now - 24 * 60 * 60 * 1000,
                                            max="({}".format(now - 8 * 60 * 60 * 1000))
                idle_pipeline.zrangebyscore(name=LOCK_CAR_TIME.format(**{"service_id": r}),
                                            min=now - 48 * 60 * 60 * 1000,
                                            max="({}".format(now - 24 * 60 * 60 * 1000))
                idle_pipeline.zrangebyscore(name=LOCK_CAR_TIME.format(**{"service_id": r}),
                                            min="-inf",
                                            max="({}".format(now - 48 * 60 * 60 * 1000))
            idle_res = idle_pipeline.execute()
        # print(idle_res)
        beetween_6_8_list, beetween_8_24_list, beetween_24_48_list, gt_48_list = [], [], [], []
        [beetween_6_8_list.extend(r) for r in idle_res[::4] if r]
        [beetween_8_24_list.extend(r) for r in idle_res[1::4] if r]
        [beetween_24_48_list.extend(r) for r in idle_res[2::4] if r]
        [gt_48_list.extend(r) for r in idle_res[3::4] if r]
        beetween_6_8 = len(beetween_6_8_list)
        beetween_8_24 = len(beetween_8_24_list)
        beetween_24_48 = len(beetween_24_48_list)
        gt_48 = len(gt_48_list)
        total = beetween_6_8 + beetween_8_24 + beetween_24_48 + gt_48

        idle_statistics = {
            "6_to_8": beetween_6_8,
            "8_to_24": beetween_8_24,
            "24_to_48": beetween_24_48,
            "gt_48": gt_48,
            "total": total
        }
        return idle_statistics

    def query_report_fee_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        营收大屏-举报管理费柱状图
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :param agent_ids:代理商id
        :return:
             data = {
             "fee_info": [
                  {
                    "date": "2020-07-31",
                    "report_fee":3,
                    "recharge_amount": 2,
                    "present_amount": 1
                  },
            "report_fee_sum": 3,
            "recharge_amount_sum":2,
            "present_amount_sum": 1
            }
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        filters = set()
        filters.add(XcEbikeUserWalletRecord.createdAt.between(begin_date, end_date))
        filters.add(XcEbikeUserWalletRecord.type == 100)
        if op_area_ids:
            filters.add(XcEbikeUserWalletRecord.serviceId.in_(op_area_ids))
        # if agent_ids:
        #     filters.add(XcEbikeUserOrder.agentId.in_(agent_ids))
        # a = time.time()
        # fee_sum = dao_session.sub_session().query(
        #     sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.penalty) / 100, 0).label("report_fee_sum"),
        #     sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.rechargeCost) / 100, 0).label("recharge_amount_sum"),
        #     sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.presentCost) / 100, 0).label("present_amount_sum")
        # ).filter(*filters).first()
        # logger.info("query_report_fee_detail diff_1: {}".format(time.time() - a))

        b = time.time()
        fee_info = dao_session.sub_session().query(
            sql.func.date(XcEbikeUserWalletRecord.createdAt).label("date"),
            sql.func.ifnull(sql.func.abs(sql.func.sum(XcEbikeUserWalletRecord.change) / 100), 0).label("report_fee"),
            sql.func.ifnull(sql.func.abs(sql.func.sum(XcEbikeUserWalletRecord.rechargeChange) / 100), 0).label(
                "recharge_amount"),
            sql.func.ifnull(sql.func.abs(sql.func.sum(XcEbikeUserWalletRecord.presentChange) / 100), 0).label(
                "present_amount")
        ).filter(*filters).group_by(sql.func.date(XcEbikeUserWalletRecord.createdAt)).all()
        logger.info("query_report_fee_detail diff_2: {}".format(time.time() - b))

        fee_result = {}
        report_fee_sum, recharge_amount_sum, present_amount_sum = 0, 0, 0
        for r in fee_info:
            date = r[0].strftime('%Y-%m-%d')
            report_fee = float(r[1]) if r[1] else 0
            recharge_amount = float(r[2]) if r[2] else 0
            present_amount = float(r[3]) if r[3] else 0
            fee_result.setdefault(date, {})["report_fee"] = float(r[1]) if r[1] else 0
            fee_result.setdefault(date, {})["recharge_amount"] = float(r[2]) if r[2] else 0
            fee_result.setdefault(date, {})["present_amount"] = float(r[3]) if r[3] else 0
            report_fee_sum += report_fee
            recharge_amount_sum += recharge_amount
            present_amount_sum += present_amount

        data = {}
        data.setdefault("report_fee_sum", report_fee_sum)
        data.setdefault("recharge_amount_sum", recharge_amount_sum)
        data.setdefault("present_amount_sum", present_amount_sum)
        for r in date_list:
            fee_dict = {}
            fee_dict["date"] = r
            fee_dict["report_fee"] = fee_result.get(r).get("report_fee", 0) if fee_result.get(r) else 0
            fee_dict["recharge_amount"] = fee_result.get(r).get("recharge_amount", 0) if fee_result.get(r) else 0
            fee_dict["present_amount"] = fee_result.get(r).get("present_amount", 0) if fee_result.get(r) else 0
            data.setdefault('fee_info', []).append(fee_dict)
        return data

    def query_present_balance_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
         营收大屏-活动赠送金额柱状图
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :param agent_ids:代理商id
        :return:
            data = {
                "activity_present_balance_sum":28,
                "activity_present_balance_info":[
                    {
                        "date":"2020-07-31",
                        "balance":28
                    }]
            }
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        filters = set()
        # 活动赠送的type
        pay_type = (7, 8, 9, 300)
        filters.add(XcEbikeUserWalletRecord.type.in_(pay_type))
        filters.add(XcEbikeUserWalletRecord.createdAt.between(begin_date, end_date))
        if op_area_ids:
            filters.add(XcEbikeUserWalletRecord.serviceId.in_(op_area_ids))
        # if agent_ids:
        #     filters.add(XcEbikeUserWalletRecord.agentId.in_(agent_ids))

        balance_sum = dao_session.sub_session().query(
            sql.func.ifnull(sql.func.sum(XcEbikeUserWalletRecord.change) / 100, 0).label("activity_present_balance_sum")
        ).filter(*filters).first()

        balance_info = dao_session.sub_session().query(
            sql.func.date(XcEbikeUserWalletRecord.createdAt).label("date"),
            sql.func.ifnull(sql.func.sum(XcEbikeUserWalletRecord.change) / 100, 0).label("balance")
        ).filter(*filters).group_by(sql.func.date(XcEbikeUserWalletRecord.createdAt)).all()

        balance_result = {}
        for r in balance_info:
            date = r[0].strftime('%Y-%m-%d')
            balance_result.setdefault(date, {})["balance"] = float(r[1]) if r[1] else 0

        data = {}
        data.setdefault("activity_present_balance_sum", float(
            balance_sum.activity_present_balance_sum) if balance_sum.activity_present_balance_sum else 0)
        for r in date_list:
            balance_dict = {}
            balance_dict["date"] = r
            balance_dict["balance"] = balance_result.get(r).get("balance", 0) if balance_result.get(r) else 0
            data.setdefault('activity_present_balance_info', []).append(balance_dict)
        return data

    def query_operation_data_1_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:order_sum,order_amount_sum
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        a = time.time()
        """总订单量"""
        order_filters = set()
        order_filters.add(XcEbikeUserOrder.createdAt.between(begin_date, end_date))
        if op_area_ids:
            order_filters.add(XcEbikeUserOrder.serviceId.in_(op_area_ids))
        # if agent_ids:
        #     order_filters.add(XcEbikeUserOrder.agentId.in_(agent_ids))

        # 索引优化,强制使用索引
        order = dao_session.sub_session().query(sql.func.count(XcEbikeUserOrder.orderId).label("order_sum"),
                                                sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.cost) / 100, 0).label(
                                                    "order_amount_sum")
                                                ).filter(*order_filters).first()
        # 总订单量
        order_sum = order.order_sum
        # 总订单消费
        order_amount_sum = round(float(order.order_amount_sum), 2)
        logger.info("query_operation_data_1_detail diff_1: {}".format(time.time() - a))
        return order_sum, order_amount_sum

    def query_operation_data_2_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:operation_sum
        """
        a = time.time()
        date_list, begin_date, end_date = self.date_list(valid_data)

        """维修次数 (运维，换电、挪车、检修车辆数)"""
        # 换电车辆数
        exchange_battery_filters = set()
        exchange_battery_filters.add(XcEbike2ChangeBattery.createdAt.between(begin_date, end_date))
        if op_area_ids:
            exchange_battery_filters.add(XcEbike2ChangeBattery.serviceId.in_(op_area_ids))
        exchange_battery = dao_session.sub_session().query(
            sql.func.count(XcEbike2ChangeBattery.id).label("exchange_battery_sum")).filter(
            *exchange_battery_filters).first()
        exchange_battery_sum = exchange_battery.exchange_battery_sum
        # 维修车辆数
        """xc_ebike_fix_tickets_2没有记录service_id,需关联xc_ebike_2_devices"""
        repair_filters = set()
        repair_filters.add(XcEbikeFixTickets2.createdAt.between(begin_date, end_date))
        repair_filters.add(XcEbikeFixTickets2.state == 2)
        if op_area_ids:
            repair_filters.add(XcEbike2Device.serviceId.in_(op_area_ids))
        repair = dao_session.sub_session().query(
            sql.func.count(XcEbikeFixTickets2.ticketNo).label("repair_sum")
        ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
            *repair_filters).first()
        repair_sum = repair.repair_sum

        # 挪车车辆数
        move_filters = set()
        move_filters.add(XcEbikeMoveOperation.createdAt.between(begin_date, end_date))
        if op_area_ids:
            move_filters.add(XcEbikeMoveOperation.serviceAreaId.in_(op_area_ids))
        move = dao_session.sub_session().query(
            sql.func.ifnull(sql.func.sum(XcEbikeMoveOperation.movingNumber), 0).label("move_sum")
        ).filter(*move_filters).first()
        move_sum = move.move_sum
        operation_sum = int(exchange_battery_sum + repair_sum + move_sum)
        logger.info("query_operation_data_2_detail diff_2: {}".format(time.time() - a))
        return operation_sum

    def query_operation_data_2_exchange_battery_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:operation_sum
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        """维修次数 (运维，换电、挪车、检修车辆数)"""
        # 换电车辆数
        exchange_battery_filters = set()
        exchange_battery_filters.add(XcEbike2ChangeBattery.createdAt.between(begin_date, end_date))
        if op_area_ids:
            exchange_battery_filters.add(XcEbike2ChangeBattery.serviceId.in_(op_area_ids))
        exchange_battery = dao_session.sub_session().query(
            sql.func.count(XcEbike2ChangeBattery.id).label("exchange_battery_sum")).filter(
            *exchange_battery_filters).first()
        exchange_battery_sum = exchange_battery.exchange_battery_sum
        return exchange_battery_sum

    def query_operation_data_2_repair_sum_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:operation_sum
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        """维修次数 (运维，换电、挪车、检修车辆数)"""
        # 换电车辆数
        repair_filters = set()
        repair_filters.add(XcEbikeFixTickets2.createdAt.between(begin_date, end_date))
        repair_filters.add(XcEbikeFixTickets2.state == 2)
        if op_area_ids:
            repair_filters.add(XcEbike2Device.serviceId.in_(op_area_ids))
        repair = dao_session.sub_session().query(
            sql.func.count(XcEbikeFixTickets2.ticketNo).label("repair_sum")
        ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
            *repair_filters).first()
        repair_sum = repair.repair_sum
        return repair_sum

    def query_operation_data_2_move_sum_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:operation_sum
        """
        date_list, begin_date, end_date = self.date_list(valid_data)
        """维修次数 (运维，换电、挪车、检修车辆数)"""
        # 挪车车辆数
        move_filters = set()
        move_filters.add(XcEbikeMoveOperation.createdAt.between(begin_date, end_date))
        if op_area_ids:
            move_filters.add(XcEbikeMoveOperation.serviceAreaId.in_(op_area_ids))
        move = dao_session.sub_session().query(
            sql.func.ifnull(sql.func.sum(XcEbikeMoveOperation.movingNumber), 0).label("move_sum")
        ).filter(*move_filters).first()
        move_sum = move.move_sum
        return move_sum

    def query_operation_data_3_detail(self, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:vehicle_sum
        """
        """车均单量"""
        # 获取总车辆数
        a = time.time()
        with dao_session.redis_session.r.pipeline(transaction=False) as pipeline:
            for r in op_area_ids:
                pipeline.scard(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": r}))
            vehicle = pipeline.execute()
        vehicle_sum = sum(vehicle)
        logger.info("query_operation_data_3_detail diff_3: {}".format(time.time() - a))
        return vehicle_sum

    def query_operation_data_4_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:duration_sum,itinerary_sum
        """
        a = time.time()
        date_list, begin_date, end_date = self.date_list(valid_data)
        logger.info("query_operation_data_4_detail begin_date: {} end_date :{}".format(begin_date, end_date))
        """均单时长"""
        b = time.time()
        sql = """
            SELECT
                IFNULL( SUM( TIMESTAMPDIFF( MINUTE, xc_ebike_device_itinerary.startTime, xc_ebike_device_itinerary.endTime ) ), 0 ) AS duration_sum,
                IFNULL( SUM( xc_ebike_device_itinerary.itinerary / 1000 ), 0 ) AS itinerary_sum
            FROM
                xc_ebike_device_itinerary
            WHERE
                {}
                AND xc_ebike_device_itinerary.createdAt BETWEEN :begin_date AND :end_date;
            """
        if op_area_ids:
            sql = sql.format("xc_ebike_device_itinerary.serviceId IN :serviceId")
        else:
            sql = sql.format("1=1")
        order_duration_itinerary = dao_session.sub_session().execute(sql,
                                                                     {"serviceId": op_area_ids,
                                                                      "begin_date": begin_date,
                                                                      "end_date": end_date}).fetchone()
        # order_duration_itinerary_filters = set()
        # order_duration_itinerary_filters.add(XcEbikeDeviceItinerary.createdAt.between(begin_date, end_date))
        # if op_area_ids:
        #     order_duration_itinerary_filters.add(XcEbikeDeviceItinerary.serviceId.in_(op_area_ids))
        # b = time.time()
        # order_duration_itinerary = dao_session.sub_session().query(
        #     sql.func.ifnull(
        #         (sql.func.sum(sql.func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
        #                                              XcEbikeDeviceItinerary.endTime))), 0).label("duration_sum"),
        #     sql.func.ifnull(sql.func.sum(XcEbikeDeviceItinerary.itinerary / 1000), 0).label("itinerary_sum")
        # ).filter(*order_duration_itinerary_filters).first()
        duration_sum = round(float(order_duration_itinerary.duration_sum), 2)
        itinerary_sum = round(float(order_duration_itinerary.itinerary_sum), 2)
        logger.info("query_operation_data_4_detail diff_4: {}".format(time.time() - a))
        logger.info("query_operation_data_4_detail diff_4_1: {}".format(time.time() - b))
        return duration_sum, itinerary_sum

    def query_operation_data_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-总订单量,运维次数,车均单量。。。
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:
            "data": {
                "order_sum": 22,
                "operation_sum": 0,
                "average_order_vehicle": 3.67,
                "average_order_cost_vehicle": 1.83,
                "average_duration_order": 3.16,
                "average_itinerary_order": 0.161
            }
        """
        date_list, begin_date, end_date = self.date_list(valid_data)

        """总订单量"""
        order_filters = set()
        order_filters.add(XcEbikeUserOrder.createdAt.between(begin_date, end_date))
        if op_area_ids:
            order_filters.add(XcEbikeUserOrder.serviceId.in_(op_area_ids))
        # if agent_ids:
        #     order_filters.add(XcEbikeUserOrder.agentId.in_(agent_ids))

        order = dao_session.sub_session().query(sql.func.count(XcEbikeUserOrder.orderId).label("order_sum"),
                                                sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.cost) / 100, 0).label(
                                                    "order_amount_sum")
                                                ).filter(*order_filters).first()
        # 总订单量
        order_sum = order.order_sum
        # 总订单消费
        order_amount_sum = order.order_amount_sum

        """维修次数 (运维，换电、挪车、检修车辆数)"""
        # 换电车辆数
        exchange_battery_filters = set()
        exchange_battery_filters.add(XcEbike2ChangeBattery.createdAt.between(begin_date, end_date))
        if op_area_ids:
            exchange_battery_filters.add(XcEbike2ChangeBattery.serviceId.in_(op_area_ids))
        exchange_battery = dao_session.sub_session().query(
            sql.func.count(XcEbike2ChangeBattery.id).label("exchange_battery_sum")).filter(
            *exchange_battery_filters).first()
        exchange_battery_sum = exchange_battery.exchange_battery_sum

        # 维修车辆数
        """xc_ebike_fix_tickets_2没有记录service_id,需关联xc_ebike_2_devices"""
        repair_filters = set()
        repair_filters.add(XcEbikeFixTickets2.createdAt.between(begin_date, end_date))
        repair_filters.add(XcEbikeFixTickets2.type == 1)
        if op_area_ids:
            repair_filters.add(XcEbike2Device.serviceId.in_(op_area_ids))
        repair = dao_session.sub_session().query(
            sql.func.count(XcEbikeFixTickets2.ticketNo).label("repair_sum")
        ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
            *repair_filters).first()
        repair_sum = repair.repair_sum

        # 巡检车辆数
        inspection_filters = set()
        inspection_filters.add(XcEbikeFixTickets2.createdAt.between(begin_date, end_date))
        inspection_filters.add(XcEbikeFixTickets2.type == 0)
        if op_area_ids:
            inspection_filters.add(XcEbike2Device.serviceId.in_(op_area_ids))
        inspection = dao_session.sub_session().query(
            sql.func.count(XcEbikeFixTickets2.ticketNo).label("inspection_sum")
        ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
            *inspection_filters).first()
        inspection_sum = inspection.inspection_sum

        # 挪车车辆数
        move_filters = set()
        move_filters.add(XcEbikeMoveOperation.createdAt.between(begin_date, end_date))
        if op_area_ids:
            move_filters.add(XcEbikeMoveOperation.serviceAreaId.in_(op_area_ids))
        move = dao_session.sub_session().query(
            sql.func.ifnull(sql.func.sum(XcEbikeMoveOperation.movingNumber), 0).label("move_sum")
        ).filter(*move_filters).first()
        move_sum = move.move_sum

        operation_sum = int(exchange_battery_sum + repair_sum + inspection_sum + move_sum)

        """车均单量"""
        # 获取总车辆数
        with dao_session.redis_session.r.pipeline(transaction=False) as pipeline:
            for r in op_area_ids:
                pipeline.scard(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": r}))
            vehicle = pipeline.execute()
        vehicle_sum = sum(vehicle)
        # 车均单量
        average_order_vehicle = 0 if vehicle_sum == 0 else round(order_sum / vehicle_sum, 2)

        """车均收益"""
        average_order_cost_vehicle = 0 if vehicle_sum == 0 else round(float(order_amount_sum) / vehicle_sum, 2)

        """均单时长"""
        order_duration_itinerary_filters = set()
        order_duration_itinerary_filters.add(XcEbikeUserOrder.createdAt.between(begin_date, end_date))
        if op_area_ids:
            order_duration_itinerary_filters.add(XcEbikeUserOrder.serviceId.in_(op_area_ids))
        order_duration_itinerary = dao_session.sub_session().query(
            sql.func.count(XcEbikeUserOrder.orderId).label("order_sum"),
            (sql.func.sum(sql.func.unix_timestamp(XcEbikeDeviceItinerary.endTime) - sql.func.unix_timestamp(
                XcEbikeDeviceItinerary.startTime)) / 60).label("duration_sum"),
            sql.func.sum(XcEbikeDeviceItinerary.itinerary / 1000).label("itinerary_sum")
        ).join(XcEbikeDeviceItinerary, XcEbikeDeviceItinerary.itinId == XcEbikeUserOrder.deviceItineraryId).with_hint(
            XcEbikeUserOrder, 'force index(idx_serviceId_createdAt)',
            'mysql').filter(
            *order_duration_itinerary_filters).first()
        duration_sum = order_duration_itinerary.duration_sum
        itinerary_sum = order_duration_itinerary.itinerary_sum
        average_duration_order = 0 if order_sum == 0 else round(float(duration_sum) / order_sum, 2)

        """均单里程"""
        average_itinerary_order = 0 if order_sum == 0 else round(float(itinerary_sum) / order_sum, 3)
        return order_sum, operation_sum, average_order_vehicle, average_order_cost_vehicle, average_duration_order, \
               average_itinerary_order

    def query_state_info_detail(self, op_area_ids: tuple):
        """
        运营大屏-车辆/电量/闲置状态统计
        :param op_area_ids:服务器id
        :param agent_ids:代理商id
        :return:
        """

        """车辆状态"""
        # total, riding_sum, ready_sum, broken_sum
        vehicle_statistics = self.get_vehicle_statistics(op_area_ids)

        """电量百分比统计"""
        electricity_statistics = self.get_electricity_statistics(op_area_ids)

        """闲置统计"""
        idle_statistics = self.get_idle_statistics(op_area_ids)

        return vehicle_statistics, electricity_statistics, idle_statistics

    def query_user_sunburst_1_detail(self, op_area_ids: tuple):
        """
        运营大屏- 用户旭日图
        :param op_area_ids:服务器id
        :param agent_ids:代理商id
        :return:
        """
        query = dao_session.sub_session().query(sql.func.count(XcEbikeUsrs2.id).label("count"))

        # 实名用户
        authentication = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                                      or_(XcEbikeUsrs2.authed == 1,
                                          XcEbikeUsrs2.deposited == 1
                                          )
                                      ).first().count

        # 非实名用户
        no_authentication = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                                         XcEbikeUsrs2.authed == 0).first().count

        return authentication, no_authentication

    def query_user_sunburst_2_detail(self, op_area_ids: tuple):
        """
        运营大屏- 用户旭日图
        :param op_area_ids:服务器id
        :param agent_ids:代理商id
        :return:
        """

        # 历史会员卡会员
        historical_member = dao_session.sub_session().query(
            sql.func.count(sql.func.distinct(XcEbikeAccount2.objectId)).label("count")
        ).join(XcEbikeUsrs2, XcEbikeAccount2.objectId == XcEbikeUsrs2.id). \
            filter(XcEbikeAccount2.type == 9,
                   XcEbikeUsrs2.serviceId.in_(op_area_ids),
                   XcEbikeUsrs2.deposited == 0,
                   XcEbikeUsrs2.authed == 1
                   ).first().count

        return historical_member

    def query_user_sunburst_3_detail(self, op_area_ids: tuple):
        """
        运营大屏- 用户旭日图
        :param op_area_ids:服务器id
        :param agent_ids:代理商id
        :return:
        """

        """用户旭日图"""
        query = dao_session.sub_session().query(sql.func.count(XcEbikeUsrs2.id).label("count"))

        # 会员用户
        member = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                              XcEbikeUsrs2.deposited == 1
                              ).first().count

        # 非会员
        non_member = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                                  XcEbikeUsrs2.deposited == 0,
                                  XcEbikeUsrs2.authed == 1
                                  ).first().count

        # 待失效会员用户
        invalid_member = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                                      XcEbikeUsrs2.deposited == 1,
                                      XcEbikeUsrs2.haveDepositCard == 1,
                                      XcEbikeUsrs2.depositCardExpiredDate < sql.func.date_format(
                                          sql.func.now(), "%Y-%m-%d %H:%i:%S"),
                                      ).first().count

        # 有效会员 = 会员- 待失效会员
        valid_member = member - invalid_member

        # 学生认证用户
        student = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                               XcEbikeUsrs2.deposited == 1,
                               XcEbikeUsrs2.student == 1,
                               # XcEbikeUsrs2.depositCardExpiredDate > sql.func.date_format(
                               #     sql.func.now(), "%Y-%m-%d %H:%i:%S")
                               ).first().count

        # 押金用户
        deposit = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                               XcEbikeUsrs2.deposited == 1,
                               XcEbikeUsrs2.depositedMount > 0,
                               # XcEbikeUsrs2.depositCardExpiredDate > sql.func.date_format(
                               #     sql.func.now(), "%Y-%m-%d %H:%i:%S")
                               ).first().count

        # 会员卡用户
        member_card = query.filter(XcEbikeUsrs2.serviceId.in_(op_area_ids),
                                   XcEbikeUsrs2.deposited == 1,
                                   XcEbikeUsrs2.haveDepositCard == 1,
                                   XcEbikeUsrs2.depositCardExpiredDate > sql.func.date_format(
                                       sql.func.now(), "%Y-%m-%d %H:%i:%S")
                                   ).first().count

        # 一键免押用户
        free_user = valid_member - student - deposit - member_card

        return member, non_member, invalid_member, valid_member, student, deposit, member_card, free_user

    def query_today_order_info(self, op_area_ids: tuple, begin_date, end_date, date_list):
        """
        实时获取当天订单饼图和折线图信息
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        filters = set()
        filters.add(XcEbikeUserOrder.createdAt.between(begin_date, end_date))
        if op_area_ids:
            filters.add(XcEbikeUserOrder.serviceId.in_(op_area_ids))

        order_query = dao_session.sub_session().query(
            sql.func.count(XcEbikeUserOrder.orderId).label("count"),
            sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.cost) / 100, 0).label("cost")
        ).join(XcEbikeDeviceItinerary, XcEbikeUserOrder.deviceItineraryId == XcEbikeDeviceItinerary.itinId).filter(
            *filters)

        # 普通订单个数,总额
        a = time.time()
        general_order = order_query.filter(sql.func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                                                  XcEbikeDeviceItinerary.endTime).between(2, 120)
                                           ).one()
        logger.info("get_order_info_detail [general_order]diff_0: {}".format(time.time() - a))

        # 超时订单个数,总额
        b = time.time()
        long_order_time = order_query.filter(
            sql.func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                   XcEbikeDeviceItinerary.endTime) > 120).one()
        logger.info("get_order_info_detail [long_order_time]diff_0: {}".format(time.time() - b))

        # 短时订单个数,总额
        c = time.time()
        short_order_time = order_query.filter(
            sql.func.TIMESTAMPDIFF(text("MINUTE"), XcEbikeDeviceItinerary.startTime,
                                   XcEbikeDeviceItinerary.endTime) < 2).one()
        logger.info("get_order_info_detail [short_order_time]diff_0: {}".format(time.time() - c))

        # 总订单
        total = order_query.one()

        order_analysis_query = dao_session.sub_session().query(
            sql.func.count(XcEbikeUserOrder.orderId).label("count"),
            sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.cost) / 100, 0).label("cost")
        ).join(XcEbike2OrderAnalysis, XcEbikeUserOrder.orderId == XcEbike2OrderAnalysis.orderId).filter(*filters)

        # 正常订单个数,总额
        normal_order = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 0).one()

        # 站点外订单订单个数,总额
        is_parking_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 1).one()

        # 服务区外订单个数,总额
        is_out_of_service_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 2).one()

        # 禁停区内订单个数,总额
        is_in_no_parking_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 3).one()

        order_pie = {
            "general_order": general_order.count,
            "long_order_time": long_order_time.count,
            "short_order_time": short_order_time.count,
            "normal_order": normal_order.count,
            "is_parking_zone": is_parking_zone.count,
            "is_out_of_service_zone": is_out_of_service_zone.count,
            "is_in_no_parking_zone": is_in_no_parking_zone.count,
            "total": total.count
        }

        order_amount_pie = {
            "general_order": round(float(general_order.cost), 2),
            "long_order_time": round(float(long_order_time.cost), 2),
            "short_order_time": round(float(short_order_time.cost), 2),
            "normal_order": round(float(normal_order.cost), 2),
            "is_parking_zone": round(float(is_parking_zone.cost), 2),
            "is_out_of_service_zone": round(float(is_out_of_service_zone.cost), 2),
            "is_in_no_parking_zone": round(float(is_in_no_parking_zone.cost), 2),
            "total": round(float(total.cost), 2),
        }

        # 获取总车辆数  xc_ebike_serviceGfence_*_deviceCount
        with dao_session.redis_session.r.pipeline(transaction=False) as pipeline:
            for r in op_area_ids:
                pipeline.scard(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": r}))
            vehicle = pipeline.execute()
        vehicle_sum = sum(vehicle)

        order_line = [
            {
                "date": date_list[0],
                "general_order": general_order.count,
                "long_order_time": long_order_time.count,
                "short_order_time": short_order_time.count,
                "normal_order": normal_order.count,
                "is_parking_zone": is_parking_zone.count,
                "is_out_of_service_zone": is_out_of_service_zone.count,
                "is_in_no_parking_zone": is_in_no_parking_zone.count,
                "total": vehicle_sum
            }
        ]
        return order_pie, order_amount_pie, order_line

    def query_before_order_info(self, op_area_ids: tuple, begin_date, end_date, date_list):
        """
        查询当天前的数据
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """

        # 查询统计表数据 XcMieba2DailyOrderAnalysis
        filters = set()
        filters.add(XcMieba2DailyOrderAnalysis.created_at >= begin_date)
        filters.add(XcMieba2DailyOrderAnalysis.created_at < end_date)
        if op_area_ids:
            filters.add(XcMieba2DailyOrderAnalysis.service_id.in_(op_area_ids))

        daily_order_query = dao_session.sub_session().query(
            sql.func.date(XcMieba2DailyOrderAnalysis.created_at).label("date"),
            sql.func.ifnull(sql.func.sum(XcMieba2DailyOrderAnalysis.total), 0).label("total"),
            sql.func.ifnull(sql.func.sum(XcMieba2DailyOrderAnalysis.cost) / 100, 0).label("cost")
        ).filter(*filters)

        # 普通订单个数,总额
        a = time.time()
        before_general_order = daily_order_query.filter(XcMieba2DailyOrderAnalysis.duration_type == 2).group_by(
            sql.func.date(XcMieba2DailyOrderAnalysis.created_at)).all()
        logger.info("query_before_order_info [general_order]diff_0: {}".format(time.time() - a))

        before_general_order_result = {}
        before_general_order_count = 0
        before_general_order_cost = 0
        for r in before_general_order:
            date = r[0].strftime('%Y-%m-%d')
            before_general_order_result.setdefault(date, {})["count"] = int(r[1])
            before_general_order_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_general_order_count += int(r[1])
            before_general_order_cost += round(float(r[2]), 2) if r[2] else 0

        # 超时订单个数,总额
        b = time.time()
        before_long_order_time = daily_order_query.filter(XcMieba2DailyOrderAnalysis.duration_type == 3).group_by(
            sql.func.date(XcMieba2DailyOrderAnalysis.created_at)).all()
        logger.info("query_before_order_info [long_order_time]diff_0: {}".format(time.time() - b))

        before_long_order_time_result = {}
        before_long_order_time_count = 0
        before_long_order_time_cost = 0
        for r in before_long_order_time:
            date = r[0].strftime('%Y-%m-%d')
            before_long_order_time_result.setdefault(date, {})["count"] = int(r[1])
            before_long_order_time_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_long_order_time_count += int(r[1])
            before_long_order_time_cost += round(float(r[2]), 2) if r[2] else 0

        # 短时订单个数,总额
        c = time.time()
        before_short_order_time = daily_order_query.filter(XcMieba2DailyOrderAnalysis.duration_type == 1).group_by(
            sql.func.date(XcMieba2DailyOrderAnalysis.created_at)).all()
        logger.info("query_before_order_info [short_order_time]diff_0: {}".format(time.time() - c))

        before_short_order_time_result = {}
        before_short_order_time_count = 0
        before_short_order_time_cost = 0
        for r in before_short_order_time:
            date = r[0].strftime('%Y-%m-%d')
            before_short_order_time_result.setdefault(date, {})["count"] = int(r[1])
            before_short_order_time_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_short_order_time_count += int(r[1])
            before_short_order_time_cost += round(float(r[2]), 2) if r[2] else 0

        # 总订单
        before_total = daily_order_query.group_by(sql.func.date(XcMieba2DailyOrderAnalysis.created_at)).all()

        before_total_result = {}
        before_total_count = 0
        before_total_cost = 0
        for r in before_total:
            date = r[0].strftime('%Y-%m-%d')
            before_total_result.setdefault(date, {})["count"] = int(r[1])
            before_total_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_total_count += int(r[1])
            before_total_cost += round(float(r[2]), 2) if r[2] else 0

        order_filters = set()
        order_filters.add(XcEbikeUserOrder.createdAt >= begin_date)
        order_filters.add(XcEbikeUserOrder.createdAt < end_date)
        if op_area_ids:
            order_filters.add(XcEbikeUserOrder.serviceId.in_(op_area_ids))

        order_analysis_query = dao_session.sub_session().query(
            sql.func.date(XcEbikeUserOrder.createdAt).label("date"),
            sql.func.count(XcEbikeUserOrder.orderId).label("count"),
            sql.func.ifnull(sql.func.sum(XcEbikeUserOrder.cost) / 100, 0).label("cost")
        ).join(XcEbike2OrderAnalysis, XcEbikeUserOrder.orderId == XcEbike2OrderAnalysis.orderId).filter(*order_filters)

        # 正常订单个数,总额
        before_normal_order = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 0).group_by(
            sql.func.date(XcEbikeUserOrder.createdAt)).all()

        before_normal_order_result = {}
        before_normal_order_count = 0
        before_normal_order_cost = 0
        for r in before_normal_order:
            date = r[0].strftime('%Y-%m-%d')
            before_normal_order_result.setdefault(date, {})["count"] = int(r[1])
            before_normal_order_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_normal_order_count += int(r[1])
            before_normal_order_cost += round(float(r[2]), 2) if r[2] else 0

        # 正常订单个数,总额
        before_normal_order = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 0).group_by(
            sql.func.date(XcEbikeUserOrder.createdAt)).all()

        before_normal_order_result = {}
        before_normal_order_count = 0
        before_normal_order_cost = 0
        for r in before_normal_order:
            date = r[0].strftime('%Y-%m-%d')
            before_normal_order_result.setdefault(date, {})["count"] = int(r[1])
            before_normal_order_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_normal_order_count += int(r[1])
            before_normal_order_cost += round(float(r[2]), 2) if r[2] else 0

        # 站点外订单订单个数,总额
        before_is_parking_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 1).group_by(
            sql.func.date(XcEbikeUserOrder.createdAt)).all()

        before_is_parking_zone_result = {}
        before_is_parking_zone_count = 0
        before_is_parking_zone_cost = 0
        for r in before_is_parking_zone:
            date = r[0].strftime('%Y-%m-%d')
            before_is_parking_zone_result.setdefault(date, {})["count"] = int(r[1])
            before_is_parking_zone_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_is_parking_zone_count += int(r[1])
            before_is_parking_zone_cost += round(float(r[2]), 2) if r[2] else 0

        # 服务区外订单个数,总额
        before_is_out_of_service_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 2).group_by(
            sql.func.date(XcEbikeUserOrder.createdAt)).all()

        before_is_out_of_service_zone_result = {}
        before_is_out_of_service_zone_count = 0
        before_is_out_of_service_zone_cost = 0
        for r in before_is_out_of_service_zone:
            date = r[0].strftime('%Y-%m-%d')
            before_is_out_of_service_zone_result.setdefault(date, {})["count"] = int(r[1])
            before_is_out_of_service_zone_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_is_out_of_service_zone_count += int(r[1])
            before_is_out_of_service_zone_cost += round(float(r[2]), 2) if r[2] else 0

        # 禁停区内订单个数,总额
        before_is_in_no_parking_zone = order_analysis_query.filter(XcEbike2OrderAnalysis.type == 3).group_by(
            sql.func.date(XcEbikeUserOrder.createdAt)).all()

        before_is_in_no_parking_zone_result = {}
        before_is_in_no_parking_zone_count = 0
        before_is_in_no_parking_zone_cost = 0
        for r in before_is_in_no_parking_zone:
            date = r[0].strftime('%Y-%m-%d')
            before_is_in_no_parking_zone_result.setdefault(date, {})["count"] = int(r[1])
            before_is_in_no_parking_zone_result.setdefault(date, {})["cost"] = round(float(r[2]), 2) if r[2] else 0
            before_is_in_no_parking_zone_count += int(r[1])
            before_is_in_no_parking_zone_cost += round(float(r[2]), 2) if r[2] else 0

        # 折线图总车辆信息
        car_filters = set()
        car_filters.add(XcMieba2CarAnalysis.created_at.between(begin_date, end_date))
        if op_area_ids:
            car_filters.add(XcMieba2CarAnalysis.service_id.in_(op_area_ids))

        car = dao_session.sub_session().query(
            sql.func.date(XcMieba2CarAnalysis.created_at).label("date"),
            sql.func.ifnull(sql.func.sum(XcMieba2CarAnalysis.total), 0).label("total")
        ).filter(
            *car_filters
        ).group_by(sql.func.date(XcMieba2CarAnalysis.created_at)).all()

        car_result = {}
        for r in car:
            date = r[0].strftime('%Y-%m-%d')
            car_result.setdefault(date, {})["total"] = int(r[1])

        order_line = []
        for r in date_list:
            date_dict = {}
            date_dict["date"] = r
            date_dict["general_order"] = before_general_order_result.get(r). \
                get("count", 0) if before_general_order_result.get(r) else 0
            date_dict["long_order_time"] = before_long_order_time_result.get(r). \
                get("count", 0) if before_long_order_time_result.get(r) else 0
            date_dict["short_order_time"] = before_short_order_time_result.get(r). \
                get("count", 0) if before_short_order_time_result.get(r) else 0
            date_dict["normal_order"] = before_normal_order_result.get(r). \
                get("count", 0) if before_normal_order_result.get(r) else 0
            date_dict["is_parking_zone"] = before_is_parking_zone_result.get(r). \
                get("count", 0) if before_is_parking_zone_result.get(r) else 0
            date_dict["is_out_of_service_zone"] = before_is_out_of_service_zone_result.get(r). \
                get("count", 0) if before_is_out_of_service_zone_result.get(r) else 0
            date_dict["is_in_no_parking_zone"] = before_is_in_no_parking_zone_result.get(r). \
                get("count", 0) if before_is_in_no_parking_zone_result.get(r) else 0
            date_dict["total"] = car_result.get(r).get("total", 0) if car_result.get(r) else 0
            order_line.append(date_dict)

        order_pie = {
            "general_order": before_general_order_count,
            "long_order_time": before_long_order_time_count,
            "short_order_time": before_short_order_time_count,
            "normal_order": before_normal_order_count,
            "is_parking_zone": before_is_parking_zone_count,
            "is_out_of_service_zone": before_is_out_of_service_zone_count,
            "is_in_no_parking_zone": before_is_in_no_parking_zone_count,
            "total": before_total_count
        }

        order_amount_pie = {
            "general_order": round(before_general_order_cost, 2),
            "long_order_time": round(before_long_order_time_cost, 2),
            "short_order_time": round(before_short_order_time_cost, 2),
            "normal_order": round(before_normal_order_cost, 2),
            "is_parking_zone": round(before_is_parking_zone_cost, 2),
            "is_out_of_service_zone": round(before_is_out_of_service_zone_cost, 2),
            "is_in_no_parking_zone": round(before_is_in_no_parking_zone_cost, 2),
            "total": round(before_total_cost, 2),
        }

        return order_pie, order_amount_pie, order_line

    def query_total_order_info(self, op_area_ids: tuple, begin_date, end_date, date_list):
        """
        查询累计数据
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        # 当天的0点日期
        before_end_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
        before_date_list = date_list[:-1]
        # 查询之前的数据
        before_order_pie, before_order_amount_pie, before_order_line = \
            self.query_before_order_info(op_area_ids, begin_date, end_date=before_end_date, date_list=before_date_list)
        # 查询当天的数据
        today_order_pie, today_order_amount_pie, today_order_line = \
            self.query_today_order_info(op_area_ids, begin_date=before_end_date, end_date=end_date,
                                        date_list=[date_list[-1]])

        logger.info("before_order_pie {}".format(before_order_pie))
        logger.info("before_order_amount_pie {}".format(before_order_amount_pie))
        logger.info("before_order_line {}".format(before_order_line))
        logger.info("today_order_pie {}".format(today_order_pie))
        logger.info("today_order_amount_pie {}".format(today_order_amount_pie))
        logger.info("today_order_line {}".format(today_order_line))

        order_pie = {
            "general_order": before_order_pie.get("general_order", 0) + today_order_pie.get("general_order", 0),
            "long_order_time": before_order_pie.get("long_order_time", 0) + today_order_pie.get("long_order_time", 0),
            "short_order_time": before_order_pie.get("short_order_time", 0) + today_order_pie.get("short_order_time",
                                                                                                  0),
            "normal_order": before_order_pie.get("normal_order", 0) + today_order_pie.get("normal_order", 0),
            "is_parking_zone": before_order_pie.get("is_parking_zone", 0) + today_order_pie.get("is_parking_zone", 0),
            "is_out_of_service_zone": before_order_pie.get("is_out_of_service_zone", 0) + today_order_pie.get(
                "is_out_of_service_zone", 0),
            "is_in_no_parking_zone": before_order_pie.get("is_in_no_parking_zone", 0) + today_order_pie.get(
                "is_in_no_parking_zone", 0),
            "total": before_order_pie.get("total", 0) + today_order_pie.get("total", 0)
        }

        order_amount_pie = {
            "general_order": round(before_order_amount_pie.get("general_order", 0) + today_order_amount_pie.get(
                "general_order", 0), 2),
            "long_order_time": round(before_order_amount_pie.get("long_order_time", 0) + today_order_amount_pie.get(
                "long_order_time", 0), 2),
            "short_order_time": round(before_order_amount_pie.get("short_order_time", 0) + today_order_amount_pie.get(
                "short_order_time", 0), 2),
            "normal_order": round(before_order_amount_pie.get("normal_order", 0) + today_order_amount_pie.get(
                "normal_order", 0), 2),
            "is_parking_zone": round(before_order_amount_pie.get("is_parking_zone", 0) + today_order_amount_pie.get(
                "is_parking_zone", 0), 2),
            "is_out_of_service_zone": round(
                before_order_amount_pie.get("is_out_of_service_zone", 0) + today_order_amount_pie.get(
                    "is_out_of_service_zone", 0), 2),
            "is_in_no_parking_zone": round(
                before_order_amount_pie.get("is_in_no_parking_zone", 0) + today_order_amount_pie.get(
                    "is_in_no_parking_zone", 0), 2),
            "total": round(before_order_amount_pie.get("total", 0) + today_order_amount_pie.get("total", 0), 2)
        }
        order_line = before_order_line + today_order_line
        return order_pie, order_amount_pie, order_line

    def query_custom_order_info(self, op_area_ids: tuple, begin_date, end_date, date_list):
        """
        自定义查询
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        today_start_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
        today_end_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d 23:59:59"), "%Y-%m-%d %H:%M:%S")
        if begin_date.__gt__(today_end_date) or end_date.__lt__(today_start_date):
            return self.query_before_order_info(op_area_ids, begin_date, end_date, date_list)
        if begin_date.__eq__(today_start_date):
            return self.query_today_order_info(op_area_ids, begin_date, end_date, date_list)
        if begin_date.__lt__(today_start_date) and end_date.__ge__(today_end_date):
            end_date = today_end_date
            date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                         range((end_date - begin_date).days + 1)]
            return self.query_total_order_info(op_area_ids, begin_date, end_date, date_list)

    def get_order_info_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-（总订单、总订单收益）饼图统计，（订单）折线图
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:
        """
        begin_time, end_time = valid_data[0:2]
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)
        type = valid_data[2]
        if type in [1, 2]:
            # 查询第一笔订单时间
            order_begin_date_info = dao_session.sub_session().query(XcEbikeUserOrder.createdAt).filter(
                XcEbikeUserOrder.serviceId.in_(op_area_ids)).order_by(
                XcEbikeUserOrder.createdAt.asc()).limit(1).first()
            if order_begin_date_info:
                order_begin_date = order_begin_date_info.createdAt
                begin_date = order_begin_date if begin_date.__le__(order_begin_date
                                                                   ) else begin_date
                begin_date = datetime.strptime(begin_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
            else:
                begin_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")

        date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                     range((end_date - begin_date).days + 1)]

        logger.debug("get_order_info_detail date_list:{}".format(date_list))
        # 实时查询当天数据
        if type == 0:
            return self.query_today_order_info(op_area_ids, begin_date, end_date, date_list)
        # 查询累计数据
        elif type == 1:
            return self.query_total_order_info(op_area_ids, begin_date, end_date, date_list)
        # 自定义数据
        elif type == 2:
            logger.debug("get_order_info_detail type == 2 start")
            logger.debug("get_order_info_detail res{}".format(
                self.query_custom_order_info(op_area_ids, begin_date, end_date, date_list)))
            return self.query_custom_order_info(op_area_ids, begin_date, end_date, date_list)

    def get_operation_info_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏-（总运维）饼图/折线图统计
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:
        """
        begin_time, end_time = valid_data[0:2]
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)
        if valid_data[2] in [1, 2]:
            # 查询第一笔挪车单
            move_begin_date_info = dao_session.sub_session().query(XcEbikeMoveOperation.createdAt).filter(
                XcEbikeMoveOperation.serviceAreaId.in_(op_area_ids)).order_by(
                XcEbikeMoveOperation.createdAt.asc()).limit(1).first()
            if move_begin_date_info:
                move_begin_date = move_begin_date_info.createdAt
            else:
                move_begin_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")

            # 查询第一笔换电单
            exchange_battery_begin_date_info = dao_session.sub_session().query(XcEbike2ChangeBattery.createdAt).filter(
                XcEbike2ChangeBattery.serviceId.in_(op_area_ids)).order_by(
                XcEbike2ChangeBattery.createdAt.asc()).limit(1).first()
            if exchange_battery_begin_date_info:
                exchange_battery_begin_date = exchange_battery_begin_date_info.createdAt
            else:
                exchange_battery_begin_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"),
                                                                "%Y-%m-%d %H:%M:%S")

            # 查询第一笔维修单
            inspection_begin_date_info = dao_session.sub_session().query(
                XcEbikeFixTickets2.createdAt
            ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
                XcEbike2Device.serviceId.in_(op_area_ids)).order_by(
                XcEbikeFixTickets2.createdAt.asc()).limit(1).first()
            if inspection_begin_date_info:
                inspection_begin_date = inspection_begin_date_info.createdAt
            else:
                inspection_begin_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"),
                                                          "%Y-%m-%d %H:%M:%S")

            object_list = [move_begin_date, exchange_battery_begin_date, inspection_begin_date]

            object_list.sort()

            if begin_date.__le__(object_list[0]):
                begin_date = object_list[0]

        date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                     range((end_date - begin_date).days + 1)]

        # 挪车车辆数,按天统计
        move_filters = set()
        move_filters.add(XcEbikeMoveOperation.createdAt.between(begin_date, end_date))
        if op_area_ids:
            move_filters.add(XcEbikeMoveOperation.serviceAreaId.in_(op_area_ids))
        move_info = dao_session.sub_session().query(
            sql.func.date(XcEbikeMoveOperation.createdAt).label("date"),
            sql.func.ifnull(sql.func.sum(XcEbikeMoveOperation.movingNumber), 0).label("count")
        ).filter(*move_filters).group_by(sql.func.date(XcEbikeMoveOperation.createdAt)).all()

        move_result = {}
        move_count = 0
        for r in move_info:
            date = r[0].strftime('%Y-%m-%d')
            move_result.setdefault(date, {})["count"] = int(r[1])
            move_count += int(r[1])

        # 换电车辆数,按天统计
        exchange_battery_filters = set()
        exchange_battery_filters.add(XcEbike2ChangeBattery.createdAt.between(begin_date, end_date))
        if op_area_ids:
            exchange_battery_filters.add(XcEbike2ChangeBattery.serviceId.in_(op_area_ids))
        exchange_battery_info = dao_session.sub_session().query(
            sql.func.date(XcEbike2ChangeBattery.createdAt).label("date"),
            sql.func.count(XcEbike2ChangeBattery.id).label("count")
        ).filter(*exchange_battery_filters).group_by(sql.func.date(XcEbike2ChangeBattery.createdAt)).all()

        exchange_battery_result = {}
        exchange_battery_count = 0
        for r in exchange_battery_info:
            date = r[0].strftime('%Y-%m-%d')
            exchange_battery_result.setdefault(date, {})["count"] = int(r[1])
            exchange_battery_count += int(r[1])

        # 维修车辆数,按天统计
        """xc_ebike_fix_tickets_2无service_id"""
        inspection_filters = set()
        inspection_filters.add(XcEbikeFixTickets2.createdAt.between(begin_date, end_date))
        inspection_filters.add(XcEbikeFixTickets2.state == 2)
        if op_area_ids:
            inspection_filters.add(XcEbike2Device.serviceId.in_(op_area_ids))
        inspection_info = dao_session.sub_session().query(
            sql.func.date(XcEbikeFixTickets2.createdAt).label("date"),
            sql.func.count(XcEbikeFixTickets2.ticketNo).label("count")
        ).join(XcEbike2Device, XcEbike2Device.imei == XcEbikeFixTickets2.imei).filter(
            *inspection_filters).group_by(sql.func.date(XcEbikeFixTickets2.createdAt)).all()

        inspection_result = {}
        inspection_count = 0
        for r in inspection_info:
            date = r[0].strftime('%Y-%m-%d')
            inspection_result.setdefault(date, {})["count"] = int(r[1])
            inspection_count += int(r[1])

        operation_line = []
        for r in date_list:
            date_dict = {}
            date_dict["date"] = r
            move = move_result.get(r).get("count", 0) if move_result.get(r) else 0
            date_dict["move"] = move
            exchange_battery = exchange_battery_result.get(r).get("count", 0) if exchange_battery_result.get(r) else 0
            date_dict["exchange_battery"] = exchange_battery
            inspection = inspection_result.get(r).get("count", 0) if inspection_result.get(r) else 0
            # alarm = alarm_result.get(r).get("count", 0) if alarm_result.get(r) else 0
            date_dict["inspection"] = inspection
            date_dict["total"] = move + exchange_battery + inspection
            operation_line.append(date_dict)

        operation_pie = {
            "move": move_count,
            "exchange_battery": exchange_battery_count,
            "inspection": inspection_count,
            "total": move_count + exchange_battery_count + inspection_count
        }

        return operation_pie, operation_line

    def query_today_user_info(self, op_area_ids, begin_date, end_date, date_list):
        """
        实时获取当天有无骑行资格用户
        :param op_area_ids:
        :return:
        """
        # 有骑行资格
        with dao_session.redis_session.r.pipeline(transaction=False) as have_pipeline:
            for r in UserState.have_riding_users():
                have_pipeline.smembers("xc_ebike_2_userStateCount_{}".format(r))
            have_res = have_pipeline.execute()
        have = set()
        for i in have_res:
            have = have.union(i)
        have_list = tuple(have)

        if have_list:
            have_sql = """
                SELECT
                    count(id)
                FROM
                    xc_ebike_usrs_2 
                WHERE id in :id AND serviceId in :service_ids
            """

            res = dao_session.sub_session().execute(have_sql, {"id": have_list, "service_ids": op_area_ids})
            have_count = res.scalar()
        else:
            have_count = 0

        # 无骑行资格
        with dao_session.redis_session.r.pipeline(transaction=False) as no_pipeline:
            for r in UserState.no_riding_users():
                no_pipeline.smembers("xc_ebike_2_userStateCount_{}".format(r))
            no_res = no_pipeline.execute()
        no = set()
        for i in no_res:
            no = no.union(i)
        no_list = tuple(no)

        if no_list:
            no_sql = """
                    SELECT
                        count(id)
                    FROM
                        xc_ebike_usrs_2 
                    WHERE id in :id AND serviceId in :service_ids
                """

            res = dao_session.sub_session().execute(no_sql, {"id": no_list, "service_ids": op_area_ids})
            no_count = res.scalar()
        else:
            no_count = 0

        user_analysis_line = [{
            "date": date_list[0],
            "riding_qualification": have_count,
            "no_riding_qualification": no_count,
            "total": have_count + no_count
        }]

        return user_analysis_line

    def query_before_user_info(self, op_area_ids, begin_date, end_date, date_list):
        """
        查询当天前的数据
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        user_analysis_filters = set()
        user_analysis_filters.add(XcMieba2UserAnalysis.created_at.between(begin_date, end_date))
        if op_area_ids:
            user_analysis_filters.add(XcMieba2UserAnalysis.service_id.in_(op_area_ids))
        user_analysis_info = dao_session.sub_session().query(
            sql.func.date(XcMieba2UserAnalysis.created_at).label("date"),
            sql.func.ifnull(sql.func.sum(XcMieba2UserAnalysis.riding_qualification), 0).label("riding_qualification"),
            sql.func.ifnull(sql.func.sum(XcMieba2UserAnalysis.no_riding_qualification), 0).label(
                "no_riding_qualification"),
            sql.func.ifnull(sql.func.sum(XcMieba2UserAnalysis.total), 0).label("total"),
        ).filter(*user_analysis_filters).group_by(sql.func.date(XcMieba2UserAnalysis.created_at)).all()

        user_analysis_result = {}
        for r in user_analysis_info:
            date = r[0].strftime('%Y-%m-%d')
            user_analysis_result.setdefault(date, {})["riding_qualification"] = int(r[1])
            user_analysis_result.setdefault(date, {})["no_riding_qualification"] = int(r[2])
            user_analysis_result.setdefault(date, {})["total"] = int(r[3])

        user_analysis_line = []
        for r in date_list:
            date_dict = {}
            date_dict["date"] = r
            date_dict["riding_qualification"] = user_analysis_result.get(r).get("riding_qualification",
                                                                                0) if user_analysis_result.get(r) else 0
            date_dict["no_riding_qualification"] = user_analysis_result.get(r).get("no_riding_qualification",
                                                                                   0) if user_analysis_result.get(
                r) else 0
            date_dict["total"] = user_analysis_result.get(r).get("total",
                                                                 0) if user_analysis_result.get(
                r) else 0
            user_analysis_line.append(date_dict)
        return user_analysis_line

    def query_total_user_info(self, op_area_ids, begin_date, end_date, date_list):
        """
        查询累计数据
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        # 当天的0点日期
        before_end_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
        before_date_list = date_list[:-1]
        # 查询之前的数据
        before_order_line = \
            self.query_before_user_info(op_area_ids, begin_date, end_date=before_end_date, date_list=before_date_list)
        # 查询当天的数据
        today_order_line = \
            self.query_today_user_info(op_area_ids, begin_date=before_end_date, end_date=end_date,
                                       date_list=[date_list[-1]])
        return before_order_line + today_order_line

    def query_custom_user_info(self, op_area_ids: tuple, begin_date, end_date, date_list):
        """
        自定义查询
        :param op_area_ids:
        :param begin_date:
        :param end_date:
        :param date_list:
        :return:
        """
        today_start_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
        today_end_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d 23:59:59"), "%Y-%m-%d %H:%M:%S")
        if begin_date.__gt__(today_end_date) or end_date.__lt__(today_start_date):
            return self.query_before_user_info(op_area_ids, begin_date, end_date, date_list)
        if begin_date.__eq__(today_start_date):
            return self.query_today_user_info(op_area_ids, begin_date, end_date, date_list)
        if begin_date.__lt__(today_start_date) and end_date.__ge__(today_end_date):
            end_date = today_end_date
            date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                         range((end_date - begin_date).days + 1)]
            return self.query_total_user_info(op_area_ids, begin_date, end_date, date_list)

    def get_user_line_detail(self, valid_data: tuple, op_area_ids: tuple):
        """
        运营大屏 - 用户每天折线图统计
        :param valid_data:开始时间，结束时间
        :param op_area_ids:服务区id
        :return:
        """
        begin_time, end_time = valid_data[0:2]
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)
        type = valid_data[2]
        if type in [1, 2]:
            # 查询第一次用户的信息
            user_analysis_begin_date_info = dao_session.sub_session().query(XcMieba2UserAnalysis.created_at).filter(
                XcMieba2UserAnalysis.service_id.in_(op_area_ids)).order_by(
                XcMieba2UserAnalysis.created_at.asc()).limit(1).first()
            if user_analysis_begin_date_info:
                user_analysis_begin_date = user_analysis_begin_date_info.created_at
                begin_date = user_analysis_begin_date if begin_date.__le__(user_analysis_begin_date
                                                                           ) else begin_date
            else:
                begin_date = datetime.strptime(end_date.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")

        date_list = [(begin_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in
                     range((end_date - begin_date).days + 1)]

        # 实时查询当天数据
        if type == 0:
            return self.query_today_user_info(op_area_ids, begin_date, end_date, date_list)
        # 查询累计数据
        elif type == 1:
            return self.query_total_user_info(op_area_ids, begin_date, end_date, date_list)
        # 自定义数据
        elif type == 2:
            return self.query_custom_user_info(op_area_ids, begin_date, end_date, date_list)

    def get_user_info_detail(self, op_area_ids: tuple, state: list):
        """
        运营大屏- 用户树状图
        :param op_area_ids:服务器id
        :param state:无骑行资格 [1,2]   有骑行资格 [3,4,5,6,7]
        :return:
        """

        # 获取所有服务区当前有资格/无资格用户
        a = time.time()
        with dao_session.redis_session.r.pipeline(transaction=False) as have_pipeline:
            for r in state:
                have_pipeline.smembers(USER_STATE_COUNT.format(**{"state": r}))
            have_res = have_pipeline.execute()
        logger.info("get_user_info_detail diff_0: {}".format(time.time() - a))
        have = set()
        for i in have_res:
            have = have.union(i)
        have_list = tuple(have)
        logger.info("get_user_info_detail have_list: {}".format(len(have_list)))
        logger.info("get_user_info_detail diff_1: {}".format(time.time() - a))

        qualification = {
            "male": {
                "16_to_22": 0,
                "23_to_28": 0,
                "29_to_40": 0,
                "41_to_60": 0,
                "total": 0
            },
            "female": {
                "16_to_22": 0,
                "23_to_28": 0,
                "29_to_40": 0,
                "41_to_60": 0,
                "total": 0
            },
            "total": 0
        }

        if have_list:
            b = time.time()
            have_sql = """
                SELECT
                    count(*) as count,
                    gender,
                    CASE 
                        WHEN LENGTH(IDCardNo)=18 THEN YEAR (now()) - YEAR (substring(IDCardNo, 7, 8))
                        WHEN LENGTH(IDCardNo)=15 THEN YEAR (now()) - YEAR(CONCAT('19',substring(IDCardNo, 7, 6)))
                        ELSE NULL 
                    END AS age 
                FROM
                    xc_ebike_usrs_2
                WHERE `gender` is not null and `serviceId` in :serviceId and `id` in :id
                GROUP BY gender,age
            """
            have_result = dao_session.sub_session().execute(have_sql,
                                                            {"serviceId": op_area_ids, "id": have_list}).fetchall()
            logger.info("get_user_info_detail diff_2: {}".format(time.time() - b))
            c = time.time()
            have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, have_male_between_41_60 = 0, 0, 0, 0
            have_female_between_16_22, have_female_between_23_28, have_female_between_29_40, have_female_between_41_60 = 0, 0, 0, 0
            for h in have_result:
                r = dict(h)
                count = r.get("count", 0)
                age = r.get("age", 0)
                gender = r.get("gender", 0)
                if not age or not gender:
                    continue
                if gender == 1:
                    if 16 <= age <= 22:
                        have_male_between_16_22 += count
                    elif 23 <= age <= 28:
                        have_male_between_23_28 += count
                    elif 29 <= age <= 40:
                        have_male_between_29_40 += count
                    elif 41 <= age <= 60:
                        have_male_between_41_60 += count
                elif gender == 2:
                    if 16 <= age <= 22:
                        have_female_between_16_22 += count
                    elif 23 <= age <= 28:
                        have_female_between_23_28 += count
                    elif 29 <= age <= 40:
                        have_female_between_29_40 += count
                    elif 41 <= age <= 60:
                        have_female_between_41_60 += count

            have_male_total = have_male_between_16_22 + have_male_between_23_28 + have_male_between_29_40 + have_male_between_41_60
            have_female_total = have_female_between_16_22 + have_female_between_23_28 + have_female_between_29_40 + have_female_between_41_60
            qualification = {
                "male": {
                    "16_to_22": have_male_between_16_22,
                    "23_to_28": have_male_between_23_28,
                    "29_to_40": have_male_between_29_40,
                    "41_to_60": have_male_between_41_60,
                    "total": have_male_total
                },
                "female": {
                    "16_to_22": have_female_between_16_22,
                    "23_to_28": have_female_between_23_28,
                    "29_to_40": have_female_between_29_40,
                    "41_to_60": have_female_between_41_60,
                    "total": have_female_total
                },
                "total": have_male_total + have_female_total
            }
            logger.info("get_user_info_detail diff_3: {}".format(time.time() - c))
        return qualification

    def get_user_info_detail_state(self, op_area_ids: tuple, state: list):
        """
        运营大屏- 用户树状图
        :param op_area_ids:服务器id
        :param state:无骑行资格 0， 1
        :return:
        """

        qualification = {
            "male": {
                "16_to_22": 0,
                "23_to_28": 0,
                "29_to_40": 0,
                "41_to_60": 0,
                "total": 0
            },
            "female": {
                "16_to_22": 0,
                "23_to_28": 0,
                "29_to_40": 0,
                "41_to_60": 0,
                "total": 0
            },
            "total": 0
        }
        if op_area_ids:
            have_sql = """
                SELECT
                    count(*) as count,
                    gender,
                    CASE 
                        WHEN LENGTH(IDCardNo)=18 THEN YEAR (now()) - YEAR (substring(IDCardNo, 7, 8))
                        WHEN LENGTH(IDCardNo)=15 THEN YEAR (now()) - YEAR(CONCAT('19',substring(IDCardNo, 7, 6)))
                        ELSE NULL 
                    END AS age 
                FROM
                    xc_ebike_usrs_2
                WHERE `gender` is not null and `serviceId` in :serviceId and `authed` = 1 and `deposited` = :deposited
                GROUP BY gender,age
            """
            have_result = dao_session.sub_session().execute(have_sql,
                                                            {"serviceId": op_area_ids, "deposited": state}).fetchall()
            have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, have_male_between_41_60 = 0, 0, 0, 0
            have_female_between_16_22, have_female_between_23_28, have_female_between_29_40, have_female_between_41_60 = 0, 0, 0, 0
            for h in have_result:
                r = dict(h)
                count = r.get("count", 0)
                age = r.get("age", 0)
                gender = r.get("gender", 0)
                if not age or not gender:
                    continue
                if gender == 1:
                    if 16 <= age <= 22:
                        have_male_between_16_22 += count
                    elif 23 <= age <= 28:
                        have_male_between_23_28 += count
                    elif 29 <= age <= 40:
                        have_male_between_29_40 += count
                    elif 41 <= age <= 60:
                        have_male_between_41_60 += count
                elif gender == 2:
                    if 16 <= age <= 22:
                        have_female_between_16_22 += count
                    elif 23 <= age <= 28:
                        have_female_between_23_28 += count
                    elif 29 <= age <= 40:
                        have_female_between_29_40 += count
                    elif 41 <= age <= 60:
                        have_female_between_41_60 += count

            have_male_total = have_male_between_16_22 + have_male_between_23_28 + have_male_between_29_40 + have_male_between_41_60
            have_female_total = have_female_between_16_22 + have_female_between_23_28 + have_female_between_29_40 + have_female_between_41_60
            qualification = {
                "male": {
                    "16_to_22": have_male_between_16_22,
                    "23_to_28": have_male_between_23_28,
                    "29_to_40": have_male_between_29_40,
                    "41_to_60": have_male_between_41_60,
                    "total": have_male_total
                },
                "female": {
                    "16_to_22": have_female_between_16_22,
                    "23_to_28": have_female_between_23_28,
                    "29_to_40": have_female_between_29_40,
                    "41_to_60": have_female_between_41_60,
                    "total": have_female_total
                },
                "total": have_male_total + have_female_total
            }
        return qualification

    def get_user_info_tree(self, op_area_ids: tuple, deposit: int):
        user_info_tree_dict = dao_session.redis_session.r.hgetall(BIG_SCREEN_USER_INFO_TREE)
        user_info_tree_list = []
        if op_area_ids:
            user_info_tree_list = [json.loads(user_info_tree_dict.get(str(s), '{}')) for s in op_area_ids if
                                   json.loads(user_info_tree_dict.get(str(s), '{}'))]
        else:
            user_info_tree_list = [json.loads(i) for i in user_info_tree_dict.values() if i]
        have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, have_male_between_41_60 = 0, 0, 0, 0
        have_female_between_16_22, have_female_between_23_28, have_female_between_29_40, have_female_between_41_60 = 0, 0, 0, 0
        for user_Info in user_info_tree_list:
            user_info_count = self.user_info_tree_service(user_Info, deposit)
            have_male_between_16_22 += user_info_count[0]
            have_male_between_23_28 += user_info_count[1]
            have_male_between_29_40 += user_info_count[2]
            have_male_between_41_60 += user_info_count[3]
            have_female_between_16_22 += user_info_count[4]
            have_female_between_23_28 += user_info_count[5]
            have_female_between_29_40 += user_info_count[6]
            have_female_between_41_60 += user_info_count[7]
        have_male_total = have_male_between_16_22 + have_male_between_23_28 + have_male_between_29_40 + have_male_between_41_60
        have_female_total = have_female_between_16_22 + have_female_between_23_28 + have_female_between_29_40 + have_female_between_41_60
        qualification = {
            "male": {
                "16_to_22": have_male_between_16_22,
                "23_to_28": have_male_between_23_28,
                "29_to_40": have_male_between_29_40,
                "41_to_60": have_male_between_41_60,
                "total": have_male_total
            },
            "female": {
                "16_to_22": have_female_between_16_22,
                "23_to_28": have_female_between_23_28,
                "29_to_40": have_female_between_29_40,
                "41_to_60": have_female_between_41_60,
                "total": have_female_total
            },
            "total": have_male_total + have_female_total
        }
        return qualification

    def user_info_tree_service(self, user_info_tree, deposit):
        #  通过是否有押金资格
        deposit_info = user_info_tree.get(str(deposit), {})
        have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, \
        have_male_between_41_60, have_female_between_16_22, have_female_between_23_28, \
        have_female_between_29_40, have_female_between_41_60 = self.user_info_tree_deposit(deposit_info)
        return (have_male_between_16_22, have_male_between_23_28, have_male_between_29_40,
                have_male_between_41_60, have_female_between_16_22, have_female_between_23_28,
                have_female_between_29_40, have_female_between_41_60)

    @staticmethod
    def user_info_tree_deposit(user_info):
        # 通过性别（gender）获取数据
        male_info = user_info.get(str(1), {})
        female_info = user_info.get(str(2), {})
        have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, have_male_between_41_60 = 0, 0, 0, 0
        have_female_between_16_22, have_female_between_23_28, have_female_between_29_40, have_female_between_41_60 = 0, 0, 0, 0
        for i in range(16, 23):
            have_male_between_16_22 += male_info.get(str(i), 0)
            have_female_between_16_22 += female_info.get(str(i), 0)
        for i in range(23, 29):
            have_male_between_23_28 += male_info.get(str(i), 0)
            have_female_between_23_28 += female_info.get(str(i), 0)
        for i in range(29, 41):
            have_male_between_29_40 += male_info.get(str(i), 0)
            have_female_between_29_40 += female_info.get(str(i), 0)
        for i in range(41, 61):
            have_male_between_41_60 += male_info.get(str(i), 0)
            have_female_between_41_60 += female_info.get(str(i), 0)
        return have_male_between_16_22, have_male_between_23_28, have_male_between_29_40, \
               have_male_between_41_60, have_female_between_16_22, have_female_between_23_28, \
               have_female_between_29_40, have_female_between_41_60

    def get_car_info_detail(self, op_area_ids: tuple):
        """
        运营大屏- 四条曲线(总车辆数、骑行中车辆数、故障检修车辆数、电量40%以上车辆数)
        :param op_area_ids:服务器id
        :return:
        """
        op_area_ids = op_area_ids

        h_date = [(datetime.now() - timedelta(hours=i)).strftime("%Y-%m-%d %H:00:00") for i in range(23, -1, -1)]

        filters = set()
        filters.add(XcMieba2HourCarAnalysis.created_at.between(h_date[0], h_date[-1]))
        if op_area_ids:
            filters.add(XcMieba2HourCarAnalysis.service_id.in_(op_area_ids))
        car_analysis_info = dao_session.sub_session().query(
            XcMieba2HourCarAnalysis.created_at.label("date"),
            sql.func.ifnull(sql.func.sum(XcMieba2HourCarAnalysis.total), 0).label("total"),
            sql.func.ifnull(sql.func.sum(XcMieba2HourCarAnalysis.riding), 0).label("riding"),
            sql.func.ifnull(sql.func.sum(XcMieba2HourCarAnalysis.broken), 0).label("broken"),
            sql.func.ifnull(sql.func.sum(XcMieba2HourCarAnalysis.gt_40), 0).label("gt_40"),
        ).filter(*filters).group_by(XcMieba2HourCarAnalysis.created_at).all()

        car_analysis_result = {}
        for r in car_analysis_info:
            date = r[0].strftime("%Y-%m-%d %H:%M:%S")
            car_analysis_result.setdefault(date, {})["total"] = int(r[1])
            car_analysis_result.setdefault(date, {})["riding"] = int(r[2])
            car_analysis_result.setdefault(date, {})["broken"] = int(r[3])
            car_analysis_result.setdefault(date, {})["gt_40"] = int(r[4])

        car_analysis_line = []
        for r in h_date:
            date_dict = {}
            date_dict["date"] = r
            date_dict["total"] = car_analysis_result.get(r).get("total", 0) if car_analysis_result.get(r) else 0
            date_dict["riding"] = car_analysis_result.get(r).get("riding", 0) if car_analysis_result.get(r) else 0
            date_dict["broken"] = car_analysis_result.get(r).get("broken", 0) if car_analysis_result.get(r) else 0
            date_dict["gt_40"] = car_analysis_result.get(r).get("gt_40", 0) if car_analysis_result.get(r) else 0
            car_analysis_line.append(date_dict)
        return car_analysis_line

    def export_bill_record(self, file_name: str):
        """
        向xc_ebike_bill_record插入一条记录
        :param file_name: 文件名
        :return:
        """
        result = dao_session.redis_session.r.set(EXPORT_SETTLEMENT_REPORT_LOCK, int(time.time()), nx=True,
                                                 px=10 * 60 * 1000)
        if not result:
            return EXPORT_SETTLEMENT_REPORT_LOCK
        params = {
            "name": file_name,
            "fileType": 1,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
        record = XcEbikeBillRecord(**params)
        dao_session.session().add(record)
        dao_session.session().commit()

    def update_bill_record(self, file_name: str):
        """
        更新文件记录
        :param file_name: 文件名
        :return:
        """
        bill_record = {
            "status": 3,
            "complateDate": datetime.now(),
            "updatedAt": datetime.now()
        }
        dao_session.session().query(XcEbikeBillRecord). \
            filter(XcEbikeBillRecord.name == file_name, XcEbikeBillRecord.fileType == 1).update(bill_record)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def new_func_request(self, start_time: str, end_time: str, file_name: str, service_ids):
        """
        调用函数计算生成settlement报表
        :param start_time: 毫秒时间戳
        :param end_time: 毫秒时间戳
        :param file_name: 文件名
        :param service_ids: 服务区id
        :return:
        """
        try:
            aliyun_info = cfg.get("aliyun")
            ops_config = cfg.get("OpsConfig")

            fc_body = {"start_time": start_time, "end_time": end_time, "file_name": file_name,
                       "service_ids": service_ids}

            # 函数计算region
            fc_region = ops_config.get("FCregion")
            fc_server_name = ops_config.get("FcServerName", None)
            service_name = "{}-{}".format(fc_server_name, "script4py") if fc_server_name else "script4py"
            func_client = AliyunFunc(
                account_id=aliyun_info.get("accountId"),
                region=fc_region,
                access_key_id=aliyun_info.get("accessKeyId"),
                access_key_secret=aliyun_info.get("secretAccessKey"),
                service_name=service_name,
                function_name="dashboardReport",
                body=fc_body
            )
            func_client.do_http_request()
        except Exception as ex:
            # 更新文件状态为失败
            self.update_bill_record(file_name=file_name)
            logger.exception(ex)
        finally:
            logger.info("revenue_export_func_request finally: {}".format(time.time()))
            dao_session.redis_session.r.delete(EXPORT_SETTLEMENT_REPORT_LOCK)

    def func_request(self, begin_time: str, end_time: str, file_name: str):
        """
        调用函数计算
        :param begin_time: 开始时间,毫秒级时间戳
        :param end_time: 结束时间,毫秒级时间戳
        :param file_name: 文件名
        :return:
        """
        try:
            aliyun_info = cfg.get("aliyun")
            func_client = AliyunFunc(
                account_id=aliyun_info.get("accountId"),
                region=aliyun_info.get("region"),
                access_key_id=aliyun_info.get("accessKeyId"),
                access_key_secret=aliyun_info.get("secretAccessKey"),
                service_name="script4py",
                function_name="dashboardReport",
                body={"start_time": begin_time, "end_time": end_time, "file_name": file_name}
            )
            func_client.do_http_request()
        except Exception as ex:
            logger.exception(ex)
        finally:
            dao_session.redis_session.r.delete(EXPORT_EXCEL_LOCK)

    def func_request_one(self, t: list):
        """
        新版本请求函数计算
        :param t: 参数列表 [datetime.datetime(2020, 8, 1, 0, 0), datetime.datetime(2020, 8, 7, 23, 59, 59), 'dashboardReport',
        '共享系统数据报表-2020-09-22 14:19:31[0]']
        :return:
        """
        begin_time = int(t[0].timestamp() * 1000)
        end_time = int(t[1].timestamp() * 1000)
        function_name = t[2]
        file_name = t[3]

        aliyun_info = cfg.get("aliyun")
        func_client = AliyunFunc(
            account_id=aliyun_info.get("accountId"),
            region=aliyun_info.get("region"),
            access_key_id=aliyun_info.get("accessKeyId"),
            access_key_secret=aliyun_info.get("secretAccessKey"),
            service_name="script4py",
            function_name=function_name,
            body={"start_time": begin_time, "end_time": end_time, "file_name": file_name}
        )
        func_client.do_http_request()

    def func_request_many(self, time_list: list):
        """
        future处理并发
        :param time_list:
        :return:
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.func_request_one, time_list)

    def auto_func_request(self, begin_time: str, end_time: str, file_name: str):
        """
        调用函数计算
        :param begin_time: 开始时间,毫秒级时间戳
        :param end_time: 结束时间,毫秒级时间戳
        :param file_name: 文件名
        :return:
        """
        # 根据时间分配函数计算,拆分时间段
        print(begin_time, end_time)
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)

        diff_days = (end_date - begin_date).days
        # 函数列表
        func_list = ['dashboardReport', 'dashboardReport1', 'dashboardReport2', 'dashboardReport3', 'dashboardReport4',
                     'dashboardReport5']
        time_list = []
        file_list = []
        count = diff_days // 7

        for r in range(count + 1):
            begin = datetime.strptime(
                (begin_date + timedelta(days=r * 7)).strftime('%Y-%m-%d 00:00:00'),
                "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime((begin + timedelta(days=6)).strftime('%Y-%m-%d 23:59:59'),
                                    "%Y-%m-%d %H:%M:%S")
            time_list.append([begin, end, func_list[r], "{}[{}]".format(file_name, r)])
            file_list.append("{}[{}]".format(file_name, r))

        time_list[-1][1] = end_date

        logger.info("auto_func_request time_list {}".format(time_list))
        logger.info("auto_func_request file_list {}".format(file_list))

        # 分配函数计算
        try:
            # 函数计算生成文件到oss,文件格式    '共享系统数据报表-2020-09-19 16:56:38[0]'
            # start_time = time.time()
            # for t in time_list:
            #     begin_time = int(t[0].timestamp() * 1000)
            #     end_time = int(t[1].timestamp() * 1000)
            #     function_name = t[2]
            #     file = t[3]
            #
            #     a = time.time()
            #     aliyun_info = cfg.get("aliyun")
            #     func_client = AliyunFunc(
            #         account_id=aliyun_info.get("accountId"),
            #         region=aliyun_info.get("region"),
            #         access_key_id=aliyun_info.get("accessKeyId"),
            #         access_key_secret=aliyun_info.get("secretAccessKey"),
            #         service_name="script4py",
            #         function_name=function_name,
            #         body={"start_time": begin_time, "end_time": end_time, "file_name": file}
            #     )
            #     func_client.do_http_request()
            #     b = time.time()
            #     logger.info("{} func finished normal_diff_time {}".format(t[2], (b - a)))
            #     print("{} func finished normal_diff_time {}".format(t[2], (b - a)))
            # end_time = time.time()
            # logger.info("func finished normal_diff_time {}".format(end_time - start_time))
            # print("func finished normal_diff_time {}".format(end_time - start_time))

            start_time = time.time()
            self.func_request_many(time_list)
            end_time = time.time()
            logger.info("func finished future_diff_time{}".format(end_time - start_time))
            print("func finished future_diff_time{}".format(end_time - start_time))

            # 将生成的oss文件转成bytes
            oss_config = cfg.get("OSSConfig")
            oss_client = AliyunOSS(oss_config)
            object_list = []
            for f in file_list:
                # res.get_object('共享系统数据报表-2020-09-15 09:38:31.zip', '共享系统数据报表-2020-09-15 09_38_31.zip')
                oss_file = f + ".xlsx"
                # local_file = f.replace(":", "_") + ".xlsx"
                object_list.append(oss_client.get_object(oss_file))

            print(object_list)
            logger.info("object_list: {}".format(object_list))
            in_memory_zip = BytesIO()
            zf = zipfile.ZipFile(in_memory_zip, 'a', zipfile.ZIP_DEFLATED, False)

            for k, v in enumerate(object_list):
                zf.writestr(file_list[k] + ".xlsx", v)

            in_memory_zip.seek(0)
            zip_bytes = in_memory_zip.read()
            logger.info("zip_bytes size: {}".format(sys.getsizeof(zip_bytes)))
            # 上传到oss
            oss_client.put_object_bytes(file_name + ".zip", zip_bytes)

            # 更新文件状态
            self.update_bill_record(file_name=file_name)
        except Exception as ex:
            logger.exception(ex)
        else:
            dao_session.redis_session.r.delete(EXPORT_EXCEL_LOCK)
