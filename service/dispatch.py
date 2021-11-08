import json
import random
from datetime import timedelta
import time
import sqlalchemy
from collections import Iterable
from typing import Dict, Any, Callable
from model.all_model import *
from service.datahub_api import workman_send_2_datahub
from service.mqtt_pub import mqtt_client
from mbutils import AGENT_NAME, logger, DefaultMaker
from mbutils import dao_session, MbException
from utils.constant.dispatch import DispatchTaskType, ActualType, distance_two_points
from utils.constant.ticket import FixState
from utils.constant.redis_key import *
from utils.constant.topic import *
from . import MBService
from .dashboard import DashboardService
from .task import DispatchTaskService


class TicketStatusService(MBService):
    def base(self, ticket_id, origin_type, current_status, last_status):
        if not ticket_id:
            return
        if origin_type == OriginType.FIX.value:
            if isinstance(ticket_id, Iterable):
                dao_session.session().execute(
                    "update xc_ebike_fix_tickets_2 set state=:current_status, updatedAt=:updatedAt where ticketNo in :ticket_id and state=:last_status",
                    {"current_status": current_status, "ticket_id": tuple(ticket_id), "last_status": last_status,
                     "updatedAt": datetime.now()})
            else:
                dao_session.session().execute(
                    "update xc_ebike_fix_tickets_2 set state=:current_status, updatedAt=:updatedAt where ticketNo=:ticket_id and state=:last_status",
                    {"current_status": current_status, "ticket_id": ticket_id, "last_status": last_status,
                     "updatedAt": datetime.now()})
        elif origin_type == OriginType.ALARM.value:
            if isinstance(ticket_id, Iterable):
                dao_session.session().execute(
                    "update xc_ebike_alarm_tickets_2 set state=:current_status, updatedAt=:updatedAt where ticketNo in :ticket_id and state=:last_status",
                    {"current_status": current_status, "ticket_id": tuple(ticket_id), "last_status": last_status,
                     "updatedAt": datetime.now()})
            else:
                dao_session.session().execute(
                    "update xc_ebike_alarm_tickets_2 set state=:current_status, updatedAt=:updatedAt where ticketNo=:ticket_id and state=:last_status",
                    {"current_status": current_status, "ticket_id": ticket_id, "last_status": last_status,
                     "updatedAt": datetime.now()})
        dao_session.session().commit()

    def cancel(self, ticket_id, origin_type):
        # self.base(ticket_id, origin_type, FixState.TO_FIX.value, FixState.FIXING.value)
        pass

    def finish(self, ticket_id, origin_type):
        self.base(ticket_id, origin_type, FixState.FIXED.value, FixState.TO_FIX.value)

    def working(self, ticket_id, origin_type):
        self.base(ticket_id, origin_type, FixState.FIXING.value, FixState.TO_FIX.value)


def task_recycle(imei, service_id, ticket_id, origin_type):
    """
    todo 批量回收的方法
    :param imei:
    :param service_id:
    :param ticket_id:
    :param origin_type:
    :return:
    """
    logger.info("task_recycle:", imei)
    dao_session.redis_session.r.zrem(DISPATCH_IMEI_ON_TASK.format(service_id=service_id), imei)
    TicketStatusService().cancel(ticket_id, origin_type)


def same_path_num(js_task: dict):
    js_task["same_path"] = 1
    js_task["end_position"] = ""
    dispatch_reason = js_task["dispatch_reason"]
    position = json.loads(dispatch_reason).get("search", None)
    if position:
        try:
            position = "_".join(position.split("_")[2:4])
            same_path = dao_session.redis_session.r.scard(DISPATCH_SAME_PATH.format(position=position))
            js_task["same_path"] = same_path
            js_task["end_position"] = position
        except Exception:
            pass


def same_path_list(dispatch_reason: str, dispatch_id: int):
    position = json.loads(dispatch_reason).get("search", None)
    # logger.info("same_path_list:", dispatch_reason, dispatch_id, position)
    if position:
        try:
            position = "_".join(position.split("_")[2:4])
            other_set = dao_session.redis_session.r.smembers(DISPATCH_SAME_PATH.format(position=position))
            if str(dispatch_id) in other_set:
                other_set.remove(str(dispatch_id))
            return [int(i) for i in other_set]
        except Exception:
            return []
    else:
        return []


class DispatchWorkmanService(MBService):
    TIMEOUT = 24 * 60 * 60

    def refresh_task_num(self, workman_id, amount=1):
        """
        用于短时间内更新缓存内的任务数目
        :param workman_id:
        :param amount:
        :return:
        """
        redis_key = DISPATCH_WORKMAN_TASK_NUM.format(workman_id=workman_id)
        dao_session.redis_session.r.incr(redis_key, amount)

    def task_num(self, workman_id):
        """获取订单数目"""
        try:
            redis_key = DISPATCH_WORKMAN_TASK_NUM.format(workman_id=workman_id)
            query = dao_session.session().query(func.count(XcMieba2Dispatch.id)).filter(
                XcMieba2Dispatch.workman_id == workman_id,
                XcMieba2Dispatch.status.in_(
                    DispatchTaskType.workman_process_list()),
                XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)))
            return int(self.double_storage_4_str(redis_key, query, expire=self.TIMEOUT))
        except MbException as e:
            logger.debug(str(e))
            return 0

    def query_list(self, tp, workman_id, status_list, limit, offset):
        """
        工人进行中、结束列表
        :param tp:
        :param workman_id:
        :param status_list:
        :param limit:
        :param offset:
        :return:
        """
        """
        {
            "id": 10001,
            "origin_type":1,
            "ticket_id":156511,
            "actual_type":2,
            "expect_expend_time":600,
            "deadline":"2020-11-07 23:11:11",
            "start_lat":129.000,
            "start_lng":28.111,
            "end_lat":129.004,
            "end_lng":28.222,
            "expect_achievement":1.2,
            "is_force": True,
            "status":1,
            "dispatch_reason":"一日无单",
            "imei":12211331,
            "cancel_type":1,
            "cancel_detail":"朋友请客"
        }
        """
        filters = set()
        filters.add(XcMieba2Dispatch.workman_id == workman_id)
        filters.add(XcMieba2Dispatch.status.in_(status_list))
        filters.add(XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)))
        now = datetime.now()
        today_start_date = now.strftime("%Y-%m-%d 00:00:00")
        today_end_date = now.strftime("%Y-%m-%d 23:59:59")
        if tp == 1:
            filters.add(XcMieba2Dispatch.created_at.between(today_start_date, today_end_date))

        many = dao_session.session().query(XcMieba2Dispatch, XcMieba2DispatchWorkmanCancel.cancel_type,
                                           XcMieba2DispatchWorkmanCancel.cancel_detail,
                                           XcMieba2DispatchWorkmanCancel.disagree_type,
                                           XcMieba2DispatchWorkmanCancel.disagree_detail,
                                           XcOpman.name).outerjoin(
            XcMieba2DispatchWorkmanCancel,
            XcMieba2DispatchWorkmanCancel.dispatch_id == XcMieba2Dispatch.id).outerjoin(
            XcOpman, XcOpman.opManId == XcMieba2Dispatch.workman_id).filter(
            *filters).order_by(XcMieba2Dispatch.updated_at.desc()).limit(limit).offset(
            offset).all()
        res = []
        for one in many:
            finish_time = one.XcMieba2Dispatch.finish_time
            accept_time = one.XcMieba2Dispatch.accept_time
            res.append({
                "id": one.XcMieba2Dispatch.id,
                "origin_type": one.XcMieba2Dispatch.origin_type,
                "ticket_id": one.XcMieba2Dispatch.ticket_id,
                "imei": one.XcMieba2Dispatch.imei,
                "car_id": dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=one.XcMieba2Dispatch.imei)),
                "actual_type": one.XcMieba2Dispatch.actual_type,
                "expect_expend_time": one.XcMieba2Dispatch.expect_expend_time,
                "deadline": one.XcMieba2Dispatch.deadline.strftime("%Y-%m-%d %H:%M:%S"),
                # "start_lat": float(one.XcMieba2Dispatch.start_lat),
                # "start_lng": float(one.XcMieba2Dispatch.start_lng),
                "start_lat": float(
                    dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=one.XcMieba2Dispatch.imei),
                                                     "lat")),
                "start_lng": float(
                    dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=one.XcMieba2Dispatch.imei),
                                                     "lng")),
                "end_lat": float(one.XcMieba2Dispatch.end_lat),
                "end_lng": float(one.XcMieba2Dispatch.end_lng),
                "expect_achievement": round(one.XcMieba2Dispatch.expect_achievement,
                                            2) if one.XcMieba2Dispatch.expect_achievement else 0,
                "actual_achievement": round(one.XcMieba2Dispatch.actual_achievement,
                                            2) if one.XcMieba2Dispatch.actual_achievement else 0,
                "is_force": one.XcMieba2Dispatch.is_force,
                "status": one.XcMieba2Dispatch.status,
                "work_progress": one.XcMieba2Dispatch.work_progress,
                "dispatch_reason": one.XcMieba2Dispatch.dispatch_reason,
                "cancel_type": one.cancel_type,
                "cancel_detail": one.cancel_detail,
                "disagree_type": one.disagree_type,
                "disagree_detail": one.disagree_detail,
                "finish_time": finish_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(finish_time, datetime) else '',
                "finish_expend_time": (finish_time - accept_time).seconds if isinstance(
                    finish_time, datetime) and isinstance(accept_time, datetime) else 0,
                "workman_name": one.name
            })
        return res

    def query_detail(self, valid_data):
        dispatch_id, = valid_data
        try:
            one = dao_session.session().query(XcMieba2Dispatch, XcMieba2DispatchWorkmanCancel.cancel_type,
                                              XcMieba2DispatchWorkmanCancel.cancel_detail,
                                              XcMieba2DispatchWorkmanCancel.disagree_type,
                                              XcMieba2DispatchWorkmanCancel.disagree_detail,
                                              XcOpman.name).outerjoin(
                XcMieba2DispatchWorkmanCancel,
                XcMieba2DispatchWorkmanCancel.dispatch_id == XcMieba2Dispatch.id).outerjoin(
                XcOpman, XcOpman.opManId == XcMieba2Dispatch.workman_id).filter(
                XcMieba2Dispatch.id == dispatch_id).one()
            finish_time = one.XcMieba2Dispatch.finish_time
            accept_time = one.XcMieba2Dispatch.accept_time
            return {
                "id": one.XcMieba2Dispatch.id,
                "origin_type": one.XcMieba2Dispatch.origin_type,
                "ticket_id": one.XcMieba2Dispatch.ticket_id,
                "actual_type": one.XcMieba2Dispatch.actual_type,
                "expect_expend_time": one.XcMieba2Dispatch.expect_expend_time,
                "deadline": one.XcMieba2Dispatch.deadline.strftime("%Y-%m-%d %H:%M:%S"),
                # "start_lat": float(one.XcMieba2Dispatch.start_lat),
                # "start_lng": float(one.XcMieba2Dispatch.start_lng),
                "start_lat": float(
                    dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=one.XcMieba2Dispatch.imei),
                                                     "lat")),
                "start_lng": float(
                    dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=one.XcMieba2Dispatch.imei),
                                                     "lng")),
                "end_lat": float(one.XcMieba2Dispatch.end_lat),
                "end_lng": float(one.XcMieba2Dispatch.end_lng),
                "expect_achievement": round(one.XcMieba2Dispatch.expect_achievement,
                                            2) if one.XcMieba2Dispatch.expect_achievement else 0,
                "actual_achievement": round(one.XcMieba2Dispatch.actual_achievement,
                                            2) if one.XcMieba2Dispatch.actual_achievement else 0,
                "is_force": one.XcMieba2Dispatch.is_force,
                "status": one.XcMieba2Dispatch.status,
                "work_progress": one.XcMieba2Dispatch.work_progress,
                "imei": one.XcMieba2Dispatch.imei,
                "car_id": dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=one.XcMieba2Dispatch.imei)),
                "dispatch_reason": one.XcMieba2Dispatch.dispatch_reason,
                "cancel_type": one.cancel_type,
                "cancel_detail": one.cancel_detail,
                "disagree_type": one.disagree_type,
                "disagree_detail": one.disagree_detail,
                "finish_time": finish_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(finish_time, datetime) else '',
                "finish_expend_time": (finish_time - accept_time).seconds if isinstance(
                    finish_time, datetime) and isinstance(accept_time, datetime) else 0,
                "workman_name": one.name
            }
        except Exception:
            raise MbException("派单id不存在")

    def cancel_task(self, valid_data):
        dispatch_id, reason_type, reason = valid_data
        # 1如果任务在,进行中才能取消
        one = dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id,
                                                                      status=DispatchTaskType.Process.value).first()
        if not one:
            raise MbException("订单不存在,或者不是进行中状态")
        # 2增加取消表
        params = {
            "dispatch_id": dispatch_id,
            "cancel_type": reason_type,
            "cancel_detail": reason,
            "agree_time": datetime.now()
        }
        workman_cancel = XcMieba2DispatchWorkmanCancel(**params)
        dao_session.session().add(workman_cancel)
        try:
            if not one.is_force:
                one.status = DispatchTaskType.Canceled.value  # 直接取消
                one.actual_achievement = 0
                dao_session.session().commit()
                logger.info(
                    "Change the Process state to Canceled, dispatch_id is:{}, imei is:{}".format(dispatch_id, one.imei))
                task_recycle(imei=one.imei, service_id=one.service_id, ticket_id=one.ticket_id,
                             origin_type=one.origin_type)
                DispatchWorkmanService().refresh_task_num(one.workman_id, -1)
            else:
                # 3通知城市经理手机
                one.status = DispatchTaskType.Canceling.value  # 取消中
                dao_session.session().commit()
                logger.info(
                    "Change the Process state to Canceling, dispatch_id is:{}, imei is:{}".format(dispatch_id,
                                                                                                  one.imei))
                data = {
                    "dispatch_id": dispatch_id,
                    "reason_type": reason_type,
                    "reason": reason
                }
                mqtt_client.publish(TOPIC_DISPATCH_TASK_CANCEL.format(agent_name=AGENT_NAME, service_id=one.service_id),
                                    json.dumps(data))
        except sqlalchemy.exc.IntegrityError:
            raise MbException("派单之前已经取消过一次")

    def cancel_task_revoke(self, valid_data):
        dispatch_id, = valid_data
        # 1如果任务在,进行中才能取消
        one = dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id,
                                                                      status=DispatchTaskType.Canceling.value).first()
        if not one:
            raise MbException("订单不存在,或者不是取消中状态")
        dao_session.session().query(XcMieba2DispatchWorkmanCancel). \
            filter(XcMieba2DispatchWorkmanCancel.dispatch_id == one.id).delete()
        one.status = DispatchTaskType.Process.value  # 取消中
        dao_session.session().commit()
        logger.info("Change the Canceling state to Process, dispatch_id is:{}".format(dispatch_id))
        return {"promt": "撤销取消任务成功"}

    def remove_workman_person(self, service_id, workman_id, dispatch_id):
        """
        接单失败只删除个人
        :param service_id:
        :param workman_id:
        :param dispatch_id:
        :param workman_ids:
        :return:
        """
        try:
            # logger.info("强制派单后workman remove2", dispatch_id, workman_id)
            dao_session.redis_session.r.zrem(
                DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=service_id, workman_id=workman_id), dispatch_id)
            dao_session.redis_session.r.hdel(
                DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=workman_id), dispatch_id)
            dao_session.redis_session.r.zrem(DISPATCH_READY_SEND_TO_MANAGER, dispatch_id)  # 不需要派发到城市经理手里
            mqtt_client.publish(
                TOPIC_DISPATCH_TASK_GONE.format(agent_name=AGENT_NAME, service_id=service_id,
                                                workman_id=workman_id),
                json.dumps({"dispatch_id": dispatch_id}))
        except Exception as e:
            logger.info("Exception:", e)

    def remove_workman_person_all(self, service_id, workman_ids, dispatch_id):
        """
        接单成功,删除所有
        :param service_id:
        :param dispatch_id:
        :param workman_ids:
        :return:
        """
        try:
            dao_session.redis_session.r.zrem(DISPATCH_READY_SEND_TO_MANAGER, dispatch_id)  # 不需要派发到城市经理手里
            for w in workman_ids:
                # logger.info("强制派单后workman remove3", dispatch_id, w)
                dao_session.redis_session.r.zrem(
                    DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=service_id, workman_id=w), dispatch_id)
                dao_session.redis_session.r.hdel(
                    DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=w), dispatch_id)
                # 防止接到同一个单,及时让其他工人上不可见,包括自己
                mqtt_client.publish(
                    TOPIC_DISPATCH_TASK_GONE.format(agent_name=AGENT_NAME, service_id=service_id,
                                                    workman_id=w),
                    json.dumps({"dispatch_id": dispatch_id}))
        except Exception as e:
            logger.info("Exception:", e)

    def accept(self, workman_id, valid_data):
        data, = valid_data
        error_list = []
        success_list = []
        fail_dict = {}
        for item in data:
            dispatch_id = item["dispatch_id"]
            workman_ids = item.get("workman_ids", [workman_id])
            expect_expend_time = item["expect_expend_time"]
            deadline = item["deadline"]
            expect_achievement = item["expect_achievement"]
            service_id = 0
            # 当前状态
            try:
                first = dao_session.session().query(XcMieba2Dispatch).filter(XcMieba2Dispatch.id == dispatch_id).first()
                service_id = first.service_id
                if first.status == DispatchTaskType.Init.value and not first.workman_id:
                    actual_type = first.actual_type
                    # 判断是否具有接单能力
                    ability_info = DispatchTaskService().get_workman_ability([workman_id], actual_type)

                    if not ability_info:
                        fail_dict.setdefault(dispatch_id, 1)
                        logger.info("不能接取这种类型订单")
                        self.remove_workman_person(service_id, workman_id, dispatch_id)
                        continue

                    # 获取当前工人任务数
                    task_num = dao_session.session().query(func.count(XcMieba2Dispatch.id)).filter(
                        XcMieba2Dispatch.service_id == service_id,
                        XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)),
                        XcMieba2Dispatch.workman_id == workman_id,
                        XcMieba2Dispatch.status.in_(
                            DispatchTaskType.workman_process_list()),
                        XcMieba2Dispatch.actual_type == actual_type).scalar()
                    if task_num <= ability_info[0][1] - 1:
                        rowcount = dao_session.session().query(XcMieba2Dispatch).filter(
                            XcMieba2Dispatch.id == dispatch_id,
                            XcMieba2Dispatch.status == DispatchTaskType.Init.value
                        ).update({"status": DispatchTaskType.Process.value, "workman_id": workman_id,
                                  "expect_expend_time": expect_expend_time, "deadline": deadline,
                                  "expect_achievement": expect_achievement, "accept_time": datetime.now(),
                                  "updated_at": datetime.now()}, synchronize_session='fetch')
                        dao_session.session().commit()
                        success_list.append(dispatch_id)
                        logger.info(
                            "Change the Init state to Process, dispatch_id is: {}, imei is:{}".format(dispatch_id,
                                                                                                      first.imei))
                        # 任务数+1
                        if rowcount:
                            DispatchWorkmanService().refresh_task_num(workman_id, 1)
                        self.remove_workman_person_all(service_id, workman_ids, dispatch_id)
                        # 如果有同路径的单子,全部接单
                        if rowcount and actual_type == ActualType.MOVE_CAR.value:
                            other_dispatch_id_list = same_path_list(first.dispatch_reason, dispatch_id)
                            for other_dispatch_id in other_dispatch_id_list:
                                rowcount = dao_session.session().query(XcMieba2Dispatch).filter(
                                    XcMieba2Dispatch.id == other_dispatch_id,
                                    XcMieba2Dispatch.status == DispatchTaskType.Init.value
                                ).update({"status": DispatchTaskType.Process.value, "workman_id": workman_id,
                                          "expect_expend_time": expect_expend_time, "deadline": deadline,
                                          "expect_achievement": expect_achievement, "accept_time": datetime.now(),
                                          "updated_at": datetime.now()}, synchronize_session='fetch')
                                dao_session.session().commit()
                                logger.info(
                                    "Change the Init state to Process, dispatch_id is: {}, imei is:{}".format(
                                        other_dispatch_id, None))
                                # 任务数+1
                                if rowcount:
                                    DispatchWorkmanService().refresh_task_num(workman_id, 1)
                                first = dao_session.session().query(XcMieba2Dispatch).filter(
                                    XcMieba2Dispatch.id == other_dispatch_id).first()
                                workman_ids = first.members.split(";")
                                self.remove_workman_person_all(service_id, workman_ids, other_dispatch_id)
                    else:
                        fail_dict.setdefault(dispatch_id, 2)
                        logger.info("接单超出最大任务数了")
                        self.remove_workman_person(service_id, workman_id, dispatch_id)
                else:
                    # 已经派发过,或者被接单了
                    fail_dict.setdefault(dispatch_id, 3)
                    logger.info("已经派发过,或者被接单了")
                    error_list.append(str(dispatch_id))
                    self.remove_workman_person_all(service_id, workman_ids, dispatch_id)
            except Exception as e:
                logger.info("Exception:", e)
                # 已经派发过,或者被接单了
                fail_dict.setdefault(dispatch_id, 3)
                error_list.append(str(dispatch_id))
                self.remove_workman_person_all(service_id, workman_ids, dispatch_id)
        return {"success_ids": success_list, "fail_ids": fail_dict}

    def finish_task(self, workman_id, valid_data):
        """
        验证任务完成,完成时间,完成位置判断,绩效判定
        :param workman_id:
        :param valid_data:
        :return:
        """
        dispatch_id, lat, lng, images = valid_data
        first = dao_session.session().query(XcMieba2Dispatch).filter(XcMieba2Dispatch.id == dispatch_id).first()
        if not first:
            raise MbException("任务不存在")
        # 判断完成的任务位置：判断人和车是否在那个地点80米以内
        if first.end_lat and first.end_lng:
            dispatch_lat, dispatch_lng = first.end_lat, first.end_lng
        else:
            if first.start_lat and first.start_lng:
                dispatch_lat, dispatch_lng = first.start_lat, first.start_lng
            else:
                raise MbException("位置有误")
        device_lat = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(**{"imei": first.imei}), "lat")
        device_lng = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(**{"imei": first.imei}), "lng")
        if lat and lng:
            p_distance = distance_two_points(dispatch_lat, dispatch_lng, lat, lng)
            if p_distance > 80:
                logger.info("人距离任务结束点超过限制:", dispatch_lat, dispatch_lng, lat, lng)
                raise MbException("人距离结束点超过限制")
        else:
            raise MbException("获取所在地，地点有误")
        if device_lat and device_lng:
            d_distance = distance_two_points(dispatch_lat, dispatch_lng, float(device_lat), float(device_lng))
            if d_distance > 80:
                logger.info("设备距离结束点超过限制:", dispatch_lat, dispatch_lng, float(device_lat), float(device_lng))
                raise MbException("设备距离任务结束点超过限制")
        else:
            raise MbException("设备所在地地点有误")
        images = ",".join(i for i in images)
        finish_time = datetime.now()
        # 判断任务完成绩效
        first.actual_achievement = DispatchTaskService().cal_actual_achievement(first.expect_achievement, finish_time,
                                                                                first.deadline)
        alarm = dao_session.session().query(XcEbikeAlarmTickets2). \
            filter(XcEbikeAlarmTickets2.ticketNo == first.ticket_id).first()
        if alarm:
            alarm.images = images
        # 判断任务完成状态
        first.status = DispatchTaskType.Finish.value if finish_time.__le__(
            first.deadline) else DispatchTaskType.ExpiredFinish.value
        first.is_finish = 1
        first.finish_time = finish_time
        first.work_progress = DispatchWorkProgress.Finish.value
        dao_session.session().commit()
        TicketStatusService().finish(first.ticket_id, first.origin_type)
        logger.info("Change the state to Finish, dispatch_id is: {}, imei is {}".format(dispatch_id, first.imei))
        DispatchWorkmanService().refresh_task_num(workman_id, -1)
        dao_session.redis_session.r.zrem(DISPATCH_IMEI_ON_TASK.format(service_id=first.service_id), first.imei)
        # 如果完成挪车后, 判断车辆还没有锁, 则提示给app让用户继续锁车
        acc = dao_session.redis_session.r.hget(IMEI_BINDING_DEVICE_INFO.format(imei=first.imei), "acc")
        if acc == "false":
            return {"acc": 0}

    def fetch_task(self, workman_id, valid_data):
        """
        工人抓取接单列表
        :param workman_id:
        :param valid_data:
        :return:
        """
        service_id, num = valid_data
        # 准备给工人看的任务
        dispatch_ids = dao_session.redis_session.r.zrevrange(
            DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=service_id, workman_id=workman_id), 0, num - 1)
        # 查询下任务的状态
        res = dao_session.session().query(XcMieba2Dispatch.id).filter(XcMieba2Dispatch.id.in_(dispatch_ids),
                                                                      XcMieba2Dispatch.status == DispatchTaskType.Init.value).all()

        filter_dispatch_ids = [str(r[0]) for r in res]
        # 取出这些任务的详细数据
        task_list = filter_dispatch_ids and dao_session.redis_session.r.hmget(
            DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=workman_id),
            list(filter_dispatch_ids))

        # 待删除dispatch_ids
        del_dispatch_ids = list(set(dispatch_ids) ^ set(filter_dispatch_ids))

        with dao_session.redis_session.r.pipeline(transaction=False) as rem_pipeline:
            if del_dispatch_ids:
                logger.info("强制派单后workman remove4", del_dispatch_ids, workman_id)
                rem_pipeline.zrem(
                    DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=service_id, workman_id=workman_id),
                    *del_dispatch_ids)
                rem_pipeline.hdel(
                    DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=workman_id),
                    *del_dispatch_ids)
                rem_pipeline.execute()
        res = []
        for task in task_list:
            js_task = json.loads(task)
            same_path_num(js_task)
            res.append(json.dumps(js_task))

        if str(workman_id) == "15172416933":
            res.extend(
                [json.dumps(
                    {"dispatch_id": 2476, "service_id": 1022, "start_lat": "27.303583", "start_lng": "115.420277",
                     "end_position": "27.317_115.423", "same_path": 3,
                     "end_lat": "27.317000", "end_lng": "115.423000", "expect_expend_time": 359,
                     "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.12, "ticket_id": 1,
                     "imei": 866039045642833,
                     "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                     "report_time": 1608768217,
                     "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                     "members": [15172416933]}),
                    json.dumps(
                        {"dispatch_id": 2477, "service_id": 1022, "start_lat": "27.304583", "start_lng": "115.420277",
                         "end_position": "27.317_115.423", "same_path": 3,
                         "end_lat": "27.317000", "end_lng": "115.423000", "expect_expend_time": 359,
                         "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.13, "ticket_id": 1,
                         "imei": 867567044919090,
                         "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                         "report_time": 1608768217,
                         "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                         "members": [15172416933]})
                    , json.dumps(
                    {"dispatch_id": 2478, "service_id": 1022, "start_lat": "27.305583", "start_lng": "115.420277",
                     "end_position": "27.317_115.423", "same_path": 3,
                     "end_lat": "27.317000", "end_lng": "115.423000", "expect_expend_time": 359,
                     "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.14, "ticket_id": 1,
                     "imei": 866039041879736,
                     "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                     "report_time": 1608768217,
                     "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                     "members": [15172416933]}),
                    json.dumps(
                        {"dispatch_id": 2479, "service_id": 1022, "start_lat": "27.309583", "start_lng": "115.426277",
                         "end_position": "27.318_115.425", "same_path": 2,
                         "end_lat": "27.318000", "end_lng": "115.425000", "expect_expend_time": 359,
                         "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.25, "ticket_id": 1,
                         "imei": 867567044631075,
                         "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                         "report_time": 1608768217,
                         "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                         "members": [15172416933]})
                    , json.dumps(
                    {"dispatch_id": 2480, "service_id": 1022, "start_lat": "27.308583", "start_lng": "115.425277",
                     "end_position": "27.318_115.425", "same_path": 2,
                     "end_lat": "27.318000", "end_lng": "115.425000", "expect_expend_time": 359,
                     "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.25, "ticket_id": 1,
                     "imei": 867567044539682,
                     "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                     "report_time": 1608768217,
                     "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                     "members": [15172416933]}),
                    json.dumps(
                        {"dispatch_id": 2481, "service_id": 1022, "start_lat": "27.358583", "start_lng": "115.455277",
                         "end_position": "27.328_115.435", "same_path": 1,
                         "end_lat": "27.328000", "end_lng": "115.435000", "expect_expend_time": 359,
                         "deadline": "2020-12-31 09:09:36", "expect_achievement": 1.35, "ticket_id": 1,
                         "imei": 867567044915627,
                         "car_id": "100604531", "origin_type": 3, "actual_type": 3, "is_force": False,
                         "report_time": 1608768217,
                         "dispatch_reason": "{\"start\": {\"cost\": 255.9748427672956, \"origin_heat\": \"0_3\", \"heat\": 100000}, \"end\": {\"end_cost\": 236.8421052631579, \"origin_heat\": \"4_0\", \"heat\": -4.0}}",
                         "members": [15172416933]})
                ]

            )
        return {"tasks": res}

    def get_workman_status(self, workman_id, valid_data):
        """
        获得工人的状态
        :param workman_id:
        :param valid_data:
        :return:
        """
        service_id, = valid_data
        res = dao_session.redis_session.r.sismember(DISPATCH_WORKMAN_START_LIST.format(**{"service_id": service_id}),
                                                    workman_id)
        return res

    def change_work_progress(self, valid_data):
        """
        更改工人进行状态
        :param valid_data:
        :return:
        """
        dispatch_id, = valid_data
        first = dao_session.session().query(XcMieba2Dispatch).filter(XcMieba2Dispatch.id == dispatch_id).first()
        if not first:
            raise MbException("任务不存在")

        # 更新状态
        params = {"work_progress": DispatchWorkProgress.Processing.value}
        dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).update(params)
        dao_session.session().commit()


class DispatchWorkmanConfigService(MBService):
    def query_one(self, workman_id):
        dao_session.redis_session.r.delete(DISPATCH_WORKMAN_TASK_NUM.format(workman_id=workman_id))  # 强制刷新一下数量
        DispatchWorkmanService().task_num(workman_id)
        redis_key = DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id)
        query = dao_session.session().query(XcMieba2WorkmanConfig).filter_by(workman_id=workman_id)
        serialize_func: Callable[[Any], Dict[str, Any]] = lambda x: {
            "workman_id": x.workman_id,
            "can_change_battery": x.can_change_battery,
            "can_fix": x.can_fix,
            "can_move_car": x.can_move_car,
            "can_inspect": x.can_inspect,
            "move_car_capacity": x.move_car_capacity,
            "change_battery_capacity": x.change_battery_capacity,
            "order_type": x.order_type
        }
        return self.double_storage_4_hash(redis_key, query, serialize_func, expire=7 * 24 * 60 * 60)

    def report_location(self, workman_id, valid_data):
        lat, lng, service_id, open_flag = valid_data
        hour = datetime.now().hour
        dao_session.redis_session.r.hset(DISPATCH_WORKMAN_HOUR_POSITION.format(service_id=service_id, hour=hour),
                                         workman_id, "{}_{}".format(lat, lng))
        dao_session.redis_session.r.expire(DISPATCH_WORKMAN_HOUR_POSITION.format(service_id=service_id, hour=hour),
                                           23 * 60 * 60)
        lat = round(lat, 8)
        lng = round(lng, 8)
        if open_flag:
            # 更新接单工人的列表
            dao_session.redis_session.r.srem(DISPATCH_WORKMAN_STOP_LIST.format(service_id=service_id), workman_id)
            dao_session.redis_session.r.sadd(DISPATCH_WORKMAN_START_LIST.format(service_id=service_id), workman_id)
        else:
            if dao_session.redis_session.r.sadd(DISPATCH_WORKMAN_STOP_LIST.format(service_id=service_id), workman_id):
                # 停止接单时候,上报到blink
                # 如果插入成功,则返回1,第一次插入上报停工状态;如果插入失败,已经上报过停工状态不需要重复上报
                workman_send_2_datahub([AGENT_NAME, service_id, workman_id, lat, lng, int(time.time()), 0])
            dao_session.redis_session.r.srem(DISPATCH_WORKMAN_START_LIST.format(service_id=service_id), workman_id)
            return "停工成功"

        # 上报位置到redis
        dao_session.redis_session.r.hset(DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id), mapping={
            "lat": lat,
            "lng": lng
        })

        # 读取工人配置
        cfg = self.query_one(workman_id)
        if not cfg:
            return '配置为空'
        # 准备上报信息
        move_car_capacity = cfg["move_car_capacity"]
        change_battery_capacity = cfg["change_battery_capacity"]
        current_move_car = dao_session.session().query(func.count(XcMieba2Dispatch.id)).filter(
            XcMieba2Dispatch.workman_id == workman_id,
            XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)),
            XcMieba2Dispatch.actual_type == ActualType.MOVE_CAR.value,
            XcMieba2Dispatch.status.in_(
                DispatchTaskType.workman_process_list())).scalar()
        current_change_battery = dao_session.session().query(func.count(XcMieba2Dispatch.id)).filter(
            XcMieba2Dispatch.workman_id == workman_id,
            XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)),
            XcMieba2Dispatch.actual_type == ActualType.CHANGE_BATTERY.value,
            XcMieba2Dispatch.status.in_(
                DispatchTaskType.workman_process_list())).scalar()
        # logger.debug(current_change_battery, int(change_battery_capacity), current_move_car, int(move_car_capacity))
        # 权限进行二进制排列
        capacity = 0
        if current_change_battery < 10:
            capacity += 1 * int(cfg["can_change_battery"])
        capacity += 2 * int(cfg["can_fix"])
        if current_move_car < 10:
            capacity += 4 * int(cfg["can_move_car"])
        capacity += 8 * int(cfg["can_inspect"])
        if capacity:
            record = [AGENT_NAME, service_id, workman_id, lat, lng, int(time.time()), capacity]
            workman_send_2_datahub(record)
            return record
        else:
            # 同时存在任务数限制
            workman_send_2_datahub([AGENT_NAME, service_id, workman_id, lat, lng, int(time.time()), 0])
            return "超过容量"

    def insert_update_one(self, workman_id, valid_data: tuple):
        """
        如果工人关闭权限,则需要把工人待接单的任务中,不符合权限的任务都干掉
        """
        can_change_battery, can_fix, can_move_car, can_inspect, move_car_capacity, change_battery_capacity, order_type, service_id = valid_data
        params = {
            "workman_id": workman_id,
            "can_change_battery": can_change_battery,
            "can_fix": can_fix,
            "can_move_car": can_move_car,
            "can_inspect": can_inspect,
            "move_car_capacity": move_car_capacity,
            "change_battery_capacity": change_battery_capacity,
            "order_type": order_type,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        params = self.remove_empty_param(params)
        dao_session.session().query(XcMieba2WorkmanConfig).filter_by(workman_id=workman_id).update(params)
        dao_session.session().commit()

        cfg = dao_session.session().query(XcMieba2WorkmanConfig).filter_by(workman_id=workman_id).first()
        if not cfg:
            cfg = XcMieba2WorkmanConfig(**params)
            dao_session.session().add(cfg)
            dao_session.session().commit()

        # 更新配置到redis
        dao_session.redis_session.r.hset(DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id), mapping={
            "can_change_battery": cfg.can_change_battery,
            "can_fix": cfg.can_fix,
            "can_move_car": cfg.can_move_car,
            "can_inspect": cfg.can_inspect,
            "move_car_capacity": cfg.move_car_capacity,
            "change_battery_capacity": cfg.change_battery_capacity,
            "order_type": cfg.order_type
        })
        dao_session.redis_session.r.expire(DISPATCH_WORKMAN_CONFIG.format(workman_id=workman_id), 7 * 24 * 60 * 60)
        # 权限调整而删除的单
        person_hash = dao_session.redis_session.r.hgetall(
            DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=workman_id))
        delete_tasks = []
        for key, value in person_hash.items():
            actual_type = json.loads(value).get("actual_type", 0)
            if not (actual_type == ActualType.CHANGE_BATTERY.value and cfg.can_change_battery
                    or actual_type == ActualType.FIX.value and cfg.can_fix
                    or actual_type == ActualType.MOVE_CAR.value and cfg.can_move_car
                    or actual_type == ActualType.INSPECT.value and cfg.can_inspect):
                delete_tasks.append(key)
        if delete_tasks:
            with dao_session.redis_session.r.pipeline(transaction=False) as rem_pipeline:
                # 从工人待列表中删除任务
                logger.info("强制派单后workman remove5", delete_tasks, workman_id)
                rem_pipeline.zrem(
                    DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=service_id, workman_id=workman_id), *delete_tasks)
                rem_pipeline.hdel(
                    DISPATCH_WORKMAN_PERSON_HASH.format(service_id=service_id, workman_id=workman_id), *delete_tasks)
                rem_pipeline.execute()


class DispatchManagerService(MBService):

    def task_num(self, valid_data):
        """
        城市经理待审批的任务数
        :param valid_data:service_id 服务区id
        :return:
        """
        try:
            service_id = valid_data
            to_audit_task_num = dao_session.session().query(func.count(XcMieba2Dispatch.id)).filter(
                XcMieba2Dispatch.service_id == service_id,
                XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)),
                XcMieba2Dispatch.status.in_(DispatchTaskType.manager_to_audit_list())).scalar()
            return to_audit_task_num
        except MbException:
            return 0

    def query_list(self, valid_data):
        """
        城市经理任务列表
        :param valid_data:
        :param type: 1待审核, 2已处理(审批),3已处理(派单)
        :return:
        """

        service_id, type, size, page = valid_data

        params = {}

        if type == 1:
            """待审核"""
            dispatch_sql = """
                SELECT
                    xm2b.id,
                    xm2b.origin_type,
                    xm2b.actual_type,
                    xm2b.ticket_id,
                    xm2b.dispatch_reason,
                    xm2b.expect_expend_time,
                    xm2b.deadline,
                    xm2b.start_lat,
                    xm2b.start_lng,
                    xm2b.end_lat,
                    xm2b.end_lng,
                    xm2b.expect_achievement,
                    xm2b.is_force,
                    xm2b.`status`,
                    xm2b.workman_id,
                    a.workman_task_num,
                    xo.`name` AS workman_name,
                    xm2b.imei,
                    xm2dwc.cancel_type,
                    xm2dwc.cancel_detail,
                    xm2dwc.agree_time
                FROM
                    xc_mieba_2_dispatch AS xm2b
                    LEFT JOIN xc_mieba_2_dispatch_workman_cancel AS xm2dwc ON xm2b.id = xm2dwc.dispatch_id
                    LEFT JOIN xc_opman AS xo ON xm2b.workman_id = xo.opManId 
                    LEFT JOIN (SELECT count(*) AS workman_task_num,workman_id FROM xc_mieba_2_dispatch WHERE service_id 
                    = :service_id AND `status` in :status AND `created_at`>(NOW()-interval 24 hour) GROUP BY workman_id) AS a 
                    ON xm2b.workman_id = a.workman_id
                WHERE
                    xm2b.service_id = :service_id AND xm2b.`status`in :status AND xm2b.`created_at`>(NOW()-interval 24 hour)
                ORDER BY xm2b.updated_at DESC
                limit :limit offset :offset;
            """
            params = {"service_id": service_id, "status": DispatchTaskType.manager_to_audit_list(),
                      "limit": size, "offset": (page - 1) * size}
        elif type == 2:
            """已处理(审批)"""
            dispatch_sql = """
                SELECT
                    xm2b.id,
                    xm2b.origin_type,
                    xm2b.actual_type,
                    xm2b.ticket_id,
                    xm2b.dispatch_reason,
                    xm2b.expect_expend_time,
                    xm2b.deadline,
                    xm2b.start_lat,
                    xm2b.start_lng,
                    xm2b.end_lat,
                    xm2b.end_lng,
                    xm2b.expect_achievement,
                    xm2b.is_force,
                    xm2b.`status`,
                    xm2b.workman_id,
                    a.workman_task_num,
                    xo.`name` AS workman_name,
                    xm2b.imei,
                    xm2dwc.cancel_type,
                    xm2dwc.cancel_detail,
                    xm2dwc.agree_time
                FROM
                    xc_mieba_2_dispatch AS xm2b
                    LEFT JOIN xc_mieba_2_dispatch_workman_cancel AS xm2dwc ON xm2b.id = xm2dwc.dispatch_id
                    LEFT JOIN xc_opman AS xo ON xm2b.workman_id = xo.opManId 
                    LEFT JOIN (SELECT count(*) AS workman_task_num,workman_id FROM xc_mieba_2_dispatch WHERE service_id 
                    = :service_id AND `status` in :status AND `created_at`>(NOW()-interval 24 hour) 
                    GROUP BY workman_id) AS a 
                    ON xm2b.workman_id = a.workman_id
                WHERE
                    xm2b.service_id = :service_id AND xm2b.`status`in :status AND xm2b.`created_at`>(NOW()-interval 24 hour) AND is_force=1
                ORDER BY xm2b.updated_at DESC
                limit :limit offset :offset;
            """
            params = {"service_id": service_id, "status": DispatchTaskType.manager_to_approve_list(),
                      "limit": size, "offset": (page - 1) * size}
        elif type == 3:
            """已处理(派单)"""
            dispatch_sql = """
                SELECT
                    xm2b.id,
                    xm2b.origin_type,
                    xm2b.actual_type,
                    xm2b.ticket_id,
                    xm2b.dispatch_reason,
                    xm2b.expect_expend_time,
                    xm2b.deadline,
                    xm2b.start_lat,
                    xm2b.start_lng,
                    xm2b.end_lat,
                    xm2b.end_lng,
                    xm2b.expect_achievement,
                    xm2b.is_force,
                    xm2b.`status`,
                    xm2b.workman_id,
                    a.workman_task_num,
                    xo.`name` AS workman_name,
                    xm2b.imei,
                    xm2dwc.cancel_type,
                    xm2dwc.cancel_detail,
                    xm2b.accept_time as agree_time
                FROM
                    xc_mieba_2_dispatch AS xm2b
                    LEFT JOIN xc_mieba_2_dispatch_workman_cancel AS xm2dwc ON xm2b.id = xm2dwc.dispatch_id
                    LEFT JOIN xc_opman AS xo ON xm2b.workman_id = xo.opManId 
                    LEFT JOIN (SELECT count(*) AS workman_task_num,workman_id FROM xc_mieba_2_dispatch WHERE service_id 
                    = :service_id AND `status` in :status AND `created_at`>(NOW()-interval 24 hour) GROUP BY workman_id) AS a 
                    ON xm2b.workman_id = a.workman_id
                WHERE
                    xm2b.service_id = :service_id AND xm2b.`status`in :status AND xm2b.`created_at`>(NOW()-interval 24 hour)
                ORDER BY xm2b.updated_at DESC
                limit :limit offset :offset;
            """
            params = {"service_id": service_id, "status": DispatchTaskType.manager_to_dispatch_list(),
                      "limit": size, "offset": (page - 1) * size}

        dispatch_result = dao_session.session().execute(dispatch_sql, params).fetchall()
        res = []
        for r in dispatch_result:
            d = dict(r)
            if d["actual_type"] == 3:
                try:
                    cost = round(json.loads(d["dispatch_reason"])["start"]["cost"] / 100, 2)
                except:
                    cost = 0
            else:
                cost = 0
            # 查询工人经纬度
            workman_lat = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=d["workman_id"]), "lat")
            workman_lng = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=d["workman_id"]), "lng")
            res.append({
                "id": d["id"],
                "origin_type": d["origin_type"],
                "ticket_id": d["ticket_id"],
                "actual_type": d["actual_type"],
                "expect_expend_time": d["expect_expend_time"],
                "deadline": d["deadline"].strftime("%Y-%m-%d %H:%M:%S"),
                "start_lat": d["start_lat"],
                "start_lng": d["start_lng"],
                "end_lat": d["end_lat"],
                "end_lng": d["end_lng"],
                "expect_achievement": round(d["expect_achievement"], 2),
                "is_force": d["is_force"],
                "status": d["status"],
                "workman_id": d["workman_id"],
                "workman_task_num": d["workman_task_num"],
                "workman_name": d["workman_name"],
                "dispatch_reason": d["dispatch_reason"],
                "cost": cost,  # 挪车时返回预计收益cost
                "imei": d["imei"],
                "car_id": dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=d["imei"])),
                "cancel_type": d["cancel_type"],
                "cancel_detail": d["cancel_detail"],
                "cancel_time": d.get("cancel_at", datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                "agree_time": (d.get("agree_time") or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),  # 城市经理处理时间
                "workman_lat": float(workman_lat) if workman_lat else 0,
                "workman_lng": float(workman_lng) if workman_lng else 0
            })
        return res

    def query_detail(self, valid_data):
        """
        城市经理查看任务详情
        :param valid_data:
        :return:
        """
        dispatch_id, workman_id = valid_data
        try:
            params = {"dispatch_id": dispatch_id}
            dispatch_sql = """
                SELECT
                    xm2b.id,
                    xm2b.origin_type,
                    xm2b.actual_type,
                    xm2b.dispatch_reason,
                    xm2b.ticket_id,
                    xm2b.expect_expend_time,
                    xm2b.deadline,
                    xm2b.start_lat,
                    xm2b.start_lng,
                    xm2b.end_lat,
                    xm2b.end_lng,
                    xm2b.expect_achievement,
                    xm2b.is_force,
                    xm2b.`status`,
                    xm2b.workman_id,
                    xo.`name` AS workman_name,
                    xm2b.imei,
                    xm2dwc.is_agree,
                    xm2dwc.agree_time,
                    xm2dwc.cancel_type,
                    xm2dwc.cancel_detail
                FROM
                    xc_mieba_2_dispatch AS xm2b
                    LEFT JOIN xc_mieba_2_dispatch_workman_cancel AS xm2dwc ON xm2b.id = xm2dwc.dispatch_id
                    LEFT JOIN xc_opman AS xo ON xm2b.workman_id = xo.opManId 
                WHERE
                    xm2b.id = :dispatch_id;
            """
            dispatch_result = dao_session.session().execute(dispatch_sql, params).fetchone()
            d = dict(dispatch_result)
            if d["actual_type"] == 3:
                try:
                    cost = round(json.loads(d["dispatch_reason"])["start"]["cost"] / 100, 2)
                except:
                    cost = 0
            else:
                cost = 0

            # 查询工人经纬度
            workman_lat = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=d["workman_id"]), "lat")
            workman_lng = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=d["workman_id"]), "lng")

            # 获取任务详情
            res = {
                "id": d["id"],
                "origin_type": d["origin_type"],
                "ticket_id": d["ticket_id"],
                "actual_type": d["actual_type"],
                "expect_expend_time": d["expect_expend_time"],
                "deadline": d["deadline"].strftime("%Y-%m-%d %H:%M:%S"),
                "start_lat": d["start_lat"],
                "start_lng": d["start_lng"],
                "end_lat": d["end_lat"],
                "end_lng": d["end_lng"],
                "expect_achievement": round(d["expect_achievement"], 2),
                "is_force": d["is_force"],
                "status": d["status"],
                "workman_id": d["workman_id"],
                "workman_task_num": DispatchWorkmanService().task_num(d["workman_id"]),
                "workman_name": d["workman_name"],
                "dispatch_reason": d["dispatch_reason"],
                "cost": cost,
                "imei": d["imei"],
                "car_id": dao_session.redis_session.r.get(IMEI_2_CAR_KEY.format(imei=d["imei"])),
                "agree_time": (d.get("agree_time") or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                "cancel_type": d["cancel_type"],
                "cancel_detail": d["cancel_detail"],
                "workman_process_list": [],  # 工人开工列表
                "workman_lat": float(workman_lat) if workman_lat else 0,
                "workman_lng": float(workman_lng) if workman_lng else 0
            }
            if workman_id:
                workman_process_info = dao_session.session().query(XcMieba2Dispatch,
                                                                   XcMieba2DispatchWorkmanCancel.cancel_type,
                                                                   XcMieba2DispatchWorkmanCancel.cancel_detail).outerjoin(
                    XcMieba2DispatchWorkmanCancel,
                    XcMieba2DispatchWorkmanCancel.dispatch_id == XcMieba2Dispatch.id).filter(
                    XcMieba2Dispatch.workman_id == workman_id,
                    XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list())).all()
                for one in workman_process_info:
                    res["workman_process_list"].append({
                        "id": one.XcMieba2Dispatch.id,
                        "origin_type": one.XcMieba2Dispatch.origin_type,
                        "ticket_id": one.XcMieba2Dispatch.ticket_id,
                        "imei": one.XcMieba2Dispatch.imei,
                        "car_id": dao_session.redis_session.r.get(
                            IMEI_2_CAR_KEY.format(imei=one.XcMieba2Dispatch.imei)),
                        "actual_type": one.XcMieba2Dispatch.actual_type,
                        "expect_expend_time": one.XcMieba2Dispatch.expect_expend_time,
                        "deadline": one.XcMieba2Dispatch.deadline.strftime("%Y-%m-%d %H:%M:%S"),
                        "start_lat": float(one.XcMieba2Dispatch.start_lat),
                        "start_lng": float(one.XcMieba2Dispatch.start_lng),
                        "end_lat": float(one.XcMieba2Dispatch.end_lat),
                        "end_lng": float(one.XcMieba2Dispatch.end_lng),
                        "expect_achievement": round(one.XcMieba2Dispatch.expect_achievement, 2),
                        "is_force": one.XcMieba2Dispatch.is_force,
                        "status": one.XcMieba2Dispatch.status,
                        "dispatch_reason": one.XcMieba2Dispatch.dispatch_reason,
                        "cancel_type": one.cancel_type,
                        "cancel_detail": one.cancel_detail
                    })
            return res
        except Exception:
            raise MbException("派单id不存在")

    def task_cancel_check(self, valid_data):
        """
        待审批页 拒绝、同意
        :param valid_data: dispatch_id, agree,reason_type, reason
        :return:
        """

        dispatch_id, agree, reason_type, reason = valid_data
        # 1 判断任务是否是待审批状态
        result = dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id,
                                                                         status=DispatchTaskType.Canceling.value).first()
        if not result:
            raise MbException("订单不存在,或者不是待审核状态")

        if agree:
            """同意"""
            dispatch_update = {"status": DispatchTaskType.Canceled.value, "updated_at": datetime.now()}
            dispatch_workman_cancel_update = {"is_agree": 1, "agree_time": datetime.now()}
            task_recycle(imei=result.imei, service_id=result.service_id, ticket_id=result.ticket_id,
                         origin_type=result.origin_type)
            DispatchWorkmanService().refresh_task_num(result.workman_id, -1)
            logger.info("Change the Canceling state to Canceled, dispatch_id is:{}, imei is:{}".format(dispatch_id,
                                                                                                       result.imei))
        else:
            """拒绝"""
            dispatch_update = {"status": DispatchTaskType.CancelRefuse.value, "updated_at": datetime.now()}
            dispatch_workman_cancel_update = {"is_agree": 0, "agree_time": datetime.now(), "disagree_type": reason_type,
                                              "disagree_detail": reason}
            logger.info("Change the Canceling state to CancelRefuse, dispatch_id is:{}, imei is:{}".format(dispatch_id,
                                                                                                           result.imei))
        dispatch_update = self.remove_empty_param(dispatch_update)
        dispatch_workman_cancel_update = self.remove_empty_param(dispatch_workman_cancel_update)
        dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).update(dispatch_update)

        dao_session.session().query(XcMieba2DispatchWorkmanCancel).filter_by(dispatch_id=dispatch_id).update(
            dispatch_workman_cancel_update)
        try:
            dao_session.session().commit()
            # 审批后通知工人
            mqtt_client.publish(TOPIC_DISPATCH_TASK_CHECK.format(agent_name=AGENT_NAME, service_id=result.service_id,
                                                                 workman_id=result.workman_id),
                                json.dumps({"dispatch_id": dispatch_id,
                                            "is_agree": agree,
                                            "reason_type": reason_type if self.exists_param(reason_type) else 1,
                                            "reason": reason}))
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def task_worth_approve(self, valid_data, recursion=True):
        """
        待处理 修改、拒绝、同意
        :param recursion: 是否递归调用, 递归退出条件
        :param valid_data:
        :return:
        """

        dispatch_id, agree, workman_id, deadline, reason_type, reason, report_time = valid_data
        result = dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).first()
        if not result:
            raise MbException("订单不存在")
        # 1 判断任务是否是待处理状态
        if recursion:
            other_dispatch_id_list = same_path_list(result.dispatch_reason, dispatch_id)
            for other_dispatch_id in other_dispatch_id_list:
                self.task_worth_approve(
                    (other_dispatch_id, agree, workman_id, deadline, reason_type, reason, report_time), recursion=False)
        if report_time:
            # 根据score,删除已经处理的value
            values = dao_session.redis_session.r.zrangebyscore(
                DISPATCH_MANAGER_PERSON_ZSET.format(service_id=result.service_id), min=report_time, max=report_time)
            for value in values:
                json_value = json.loads(value)
                value_dispatch_id = json_value.get("dispatch_id", 0)
                if value_dispatch_id == dispatch_id:
                    dao_session.redis_session.r.zrem(DISPATCH_MANAGER_PERSON_ZSET.format(service_id=result.service_id),
                                                     value)
                    break
        if result.status != DispatchTaskType.Init.value:
            # 已经被处理的单子也要通知下去,其他地方删除,可能是重复操作
            # logger.info("强制派单后workman remove6", dispatch_id, workman_id)
            dao_session.redis_session.r.zrem(
                DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=result.service_id, workman_id=workman_id), dispatch_id)
            dao_session.redis_session.r.hdel(
                DISPATCH_WORKMAN_PERSON_HASH.format(service_id=result.service_id, workman_id=workman_id), dispatch_id)
            mqtt_client.publish(
                TOPIC_DISPATCH_TASK_DECISION_FINISH.format(agent_name=AGENT_NAME, service_id=result.service_id,
                                                           workman_id=workman_id),
                json.dumps({"dispatch_id": dispatch_id}))
            raise MbException("订单不是待处理状态")

        if agree:
            """修改 or 同意"""
            dispatch_update = {"status": DispatchTaskType.Process.value,
                               "updated_at": datetime.now(),
                               "workman_id": workman_id,
                               "deadline": deadline,
                               "accept_time": datetime.now(),
                               "is_force": 1}
            dispatch_update = self.remove_empty_param(dispatch_update)
            dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).update(dispatch_update)
            # 强制派单后通知工人
            mqtt_client.publish(TOPIC_DISPATCH_TASK_FORCE.format(agent_name=AGENT_NAME, service_id=result.service_id,
                                                                 workman_id=workman_id),
                                json.dumps({"dispatch_id": dispatch_id}))
            logger.info(
                "Change the Init state to Process, dispatch_id is:{}, imei is:{}".format(dispatch_id, result.imei))
            DispatchWorkmanService().refresh_task_num(workman_id, 1)
        else:
            """拒绝"""
            dispatch_update = {"status": DispatchTaskType.Refuse.value,
                               "updated_at": datetime.now(),
                               "workman_id": workman_id,
                               "deadline": deadline,
                               "accept_time": datetime.now()}
            dispatch_update = self.remove_empty_param(dispatch_update)
            dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).update(dispatch_update)

            manager_cancel_record = {
                "dispatch_id": dispatch_id,
                "refuse_type": reason_type,
                "refuse_detail": reason
            }
            manager_cancel = XcMieba2DispatchManagerCancel(**manager_cancel_record)
            dao_session.session().add(manager_cancel)
            task_recycle(imei=result.imei, service_id=result.service_id, ticket_id=result.ticket_id,
                         origin_type=result.origin_type)
            logger.info(
                "Change the Init state to Refuse, dispatch_id is:{}, imei is:{}".format(dispatch_id, result.imei))
        try:
            dao_session.session().commit()
            # logger.info("强制派单后workman remove7", dispatch_id, workman_id)
            dao_session.redis_session.r.zrem(
                DISPATCH_WORKMAN_PERSON_ZSET.format(service_id=result.service_id, workman_id=workman_id), dispatch_id)
            dao_session.redis_session.r.hdel(
                DISPATCH_WORKMAN_PERSON_HASH.format(service_id=result.service_id, workman_id=workman_id), dispatch_id)
            mqtt_client.publish(
                TOPIC_DISPATCH_TASK_DECISION_FINISH.format(agent_name=AGENT_NAME, service_id=result.service_id,
                                                           workman_id=workman_id),
                json.dumps({"dispatch_id": dispatch_id}))

        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def query_workman_choose(self, valid_data):
        """
        城市经理选择哪个工人接任务, 所有开工的工人
        :param valid_data:
        :return:
        """
        service_id, actual_type = valid_data

        # 当前服务区的开工列表
        workman_set = dao_session.redis_session.r.smembers(DISPATCH_WORKMAN_START_LIST.format(service_id=service_id))
        workman_data = []
        for w in workman_set:
            if actual_type == ActualType.CHANGE_BATTERY.value:
                workman_config = dao_session.session().query(XcMieba2WorkmanConfig.workman_id).filter(
                    XcMieba2WorkmanConfig.workman_id == w,
                    XcMieba2WorkmanConfig.can_change_battery == 1).first()
            elif actual_type == ActualType.FIX.value:
                workman_config = dao_session.session().query(XcMieba2WorkmanConfig.workman_id).filter(
                    XcMieba2WorkmanConfig.workman_id == w,
                    XcMieba2WorkmanConfig.can_fix == 1).first()
            elif actual_type == ActualType.MOVE_CAR.value:
                workman_config = dao_session.session().query(XcMieba2WorkmanConfig.workman_id).filter(
                    XcMieba2WorkmanConfig.workman_id == w,
                    XcMieba2WorkmanConfig.can_move_car == 1).first()
            elif actual_type == ActualType.INSPECT.value:
                workman_config = dao_session.session().query(XcMieba2WorkmanConfig.workman_id).filter(
                    XcMieba2WorkmanConfig.workman_id == w,
                    XcMieba2WorkmanConfig.can_inspect == 1).first()
            if not workman_config:
                continue
            workman_info = dao_session.session().query(XcOpman.name).filter(XcOpman.opManId == w).first()
            name = workman_info.name if workman_info else ""
            task_num = DispatchWorkmanService().task_num(w)
            if name:
                # 查询工人经纬度
                workman_lat = dao_session.redis_session.r.hget(
                    DISPATCH_WORKMAN_CONFIG.format(workman_id=w), "lat")
                workman_lng = dao_session.redis_session.r.hget(
                    DISPATCH_WORKMAN_CONFIG.format(workman_id=w), "lng")
                if workman_lat and workman_lng:
                    workman_data.append(
                        {"workman_id": w, "name": name, "task_num": task_num, "lat": float(workman_lat),
                         "lng": float(workman_lng)})
        return workman_data

    def fetch_task(self, valid_data):
        service_id, num = valid_data
        task_list = dao_session.redis_session.r.zrevrange(DISPATCH_MANAGER_PERSON_ZSET.format(service_id=service_id), 0,
                                                          num - 1)
        tasks = []
        for t in list(task_list):
            t_dict = json.loads(t)
            t_dict["expect_achievement"] = round(t_dict["expect_achievement"], 2)
            if t_dict["actual_type"] == 3:
                try:
                    t_dict["cost"] = round(json.loads(t_dict["dispatch_reason"])["start"]["cost"] / 100, 2)
                except:
                    t_dict["cost"] = 0
            else:
                t_dict["cost"] = 0
            workman_lat = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=t_dict["workman_id"]), "lat")
            workman_lng = dao_session.redis_session.r.hget(
                DISPATCH_WORKMAN_CONFIG.format(workman_id=t_dict["workman_id"]), "lng")
            t_dict["workman_lat"] = float(workman_lat) if workman_lat else 0
            t_dict["workman_lng"] = float(workman_lng) if workman_lat else 0
            same_path_num(t_dict)
            tasks.append(json.dumps(t_dict))
        return {"tasks": tasks}


class DispatchStatisticsService(MBService):

    def __init__(self, valid_data):
        begin_time, end_time, self.service_id = valid_data
        if begin_time and isinstance(begin_time, int):
            self.begin_time = self.num2datetime(begin_time / 1000)
        else:
            self.begin_time = begin_time
        if end_time and isinstance(end_time, int):
            self.end_time = self.num2datetime(end_time / 1000)
        else:
            self.end_time = end_time

    def parsing_dispatch_data(self, dispatch_obj):
        return {
            "date": dispatch_obj.statistics_date.strftime("%Y-%m-%d"),
            "orders": dispatch_obj.battery_total + dispatch_obj.fix_total +
                      dispatch_obj.move_car_total + dispatch_obj.inspect_total,
            "finish_orders": dispatch_obj.finish_total,
            "benefits": dispatch_obj.car_accept * dispatch_obj.order_growth,
            "gain_time": round((dispatch_obj.battery_total * 1 + dispatch_obj.fix_total * 3 +
                                dispatch_obj.move_car_total * 2 + dispatch_obj.inspect_total * 3) / 60, 2)
        }

    def query_yesterday_data(self):
        params = {
            "statistics_date": self.begin_time,
            "service_id": self.service_id
        }
        statistics_data = dao_session.session().query(XcMieba2DispatchDailyStatistics).filter_by(**params).first()
        if statistics_data:
            return {
                "date": statistics_data.statistics_date.strftime("%Y-%m-%d"),
                "orders": statistics_data.battery_total + statistics_data.fix_total +
                          statistics_data.move_car_total + statistics_data.inspect_total,
                "finish_orders": statistics_data.finish_total,
                "benefits": statistics_data.car_accept * statistics_data.order_growth,
                "gain_time": round((statistics_data.battery_total * 1 + statistics_data.fix_total * 3 +
                                    statistics_data.move_car_total * 2 + statistics_data.inspect_total * 3) / 60, 2)
            }
        else:
            return {}

    def insert_yesterday_data(self):
        # yesterday_begin = self.begin_time - timedelta(hours=24)
        before_yesterday_begin = self.begin_time - timedelta(hours=24)
        # three_days_ago_begin = before_yesterday_begin - timedelta(hours=24)
        order = dao_session.session().query(func.count(XcEbikeUserOrder.orderId).label("order_sum"),
                                            func.ifnull(func.sum(XcEbikeUserOrder.cost) / 100, 0).label(
                                                "order_amount_sum"))
        yesterday_orders = order.filter(
            XcEbikeUserOrder.createdAt.between(self.begin_time, self.end_time)).first()
        three_days_ago_orders = order.filter(
            XcEbikeUserOrder.createdAt.between(before_yesterday_begin, self.begin_time)).first()
        actual_type_num = dao_session.session().query(XcMieba2Dispatch.actual_type,
                                                      func.count(XcMieba2Dispatch.id).label("dispatch_num")).filter(
            XcMieba2Dispatch.service_id == self.service_id, XcMieba2Dispatch.created_at.between(
                self.begin_time, self.end_time)).group_by(XcMieba2Dispatch.actual_type).all()
        finish_dispatch = dao_session.session().query(func.count(XcMieba2Dispatch.id).label("dispatch_num")).filter(
            XcMieba2Dispatch.service_id == self.service_id, XcMieba2Dispatch.is_finish == 1,
            XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time)).scalar()
        battery_total, fix_total, move_car_total, inspect_total = 0, 0, 0, 0
        for i in actual_type_num:
            if i.actual_type == ActualType.CHANGE_BATTERY.value:
                battery_total = i.dispatch_num
            elif i.actual_type == ActualType.FIX.value:
                fix_total = i.dispatch_num
            elif i.actual_type == ActualType.MOVE_CAR.value:
                move_car_total = i.dispatch_num
            elif i.actual_type == ActualType.INSPECT.value:
                inspect_total = i.dispatch_num
        order_sum = yesterday_orders.order_sum
        order_amount_sum = yesterday_orders.order_amount_sum
        car_num = dao_session.redis_session.r.scard(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": self.service_id}))
        # car_order = round(order_sum / car_num, 2) if car_num else 0
        car_achievement = round(float(order_amount_sum) / car_num, 2) if car_num else 0
        dispatch_statistics = XcMieba2DispatchDailyStatistics()
        dispatch_statistics.service_id = self.service_id
        dispatch_statistics.statistics_date = self.begin_time
        dispatch_statistics.car_accept = car_achievement
        dispatch_statistics.car_total = car_num
        dispatch_statistics.order_growth = yesterday_orders.order_sum - three_days_ago_orders.order_sum
        dispatch_statistics.battery_total = battery_total
        dispatch_statistics.fix_total = fix_total
        dispatch_statistics.move_car_total = move_car_total
        dispatch_statistics.inspect_total = inspect_total
        dispatch_statistics.finish_total = finish_dispatch
        dispatch_statistics.created_at = datetime.now()
        dispatch_statistics.updated_at = datetime.now()
        dao_session.session().add(dispatch_statistics)
        try:
            dao_session.session().commit()
            return {
                "date": self.begin_time.strftime("%Y-%m-%d"),
                "orders": battery_total + fix_total + move_car_total + inspect_total,
                "finish_orders": dispatch_statistics.finish_total,
                "benefits": round(car_achievement * (yesterday_orders.order_sum - three_days_ago_orders.order_sum), 2),
                "gain_time": round((battery_total * 1 + fix_total * 3 + move_car_total * 2 + inspect_total * 3) / 60, 2)
            }
        except Exception as ex:
            logger.error("add dispatch daily data is error", ex)
            dao_session.session().rollback()
            return {}

    def query_car_average_order(self):
        car_num = dao_session.redis_session.r.scard(SERVICE_DEVICE_All_IMEIS.format(**{"service_id": self.service_id}))
        # car_num = 21
        if car_num:
            statistics_data = self.query_car_orders()
            if statistics_data:
                return {k: round(v / car_num, 2) for k, v in statistics_data.items() if k}
        return {}

    def query_car_orders(self):
        statistics_data = dao_session.session().query(
            func.count(XcEbikeUserOrder.orderId),
            func.date_format(XcEbikeUserOrder.createdAt, "%Y-%m-%d").label("ctime")
        ).filter(XcEbikeUserOrder.createdAt.between(self.begin_time, self.end_time),
                 XcEbikeUserOrder.serviceId == self.service_id).group_by("ctime").all()
        if statistics_data:
            return {j: i for i, j in statistics_data if j}
        else:
            return {}

    def query_car_dispatch(self):
        dispatch_data = dao_session.session().query(
            func.count(XcMieba2Dispatch.id),
            func.date_format(XcMieba2Dispatch.created_at, "%Y-%m-%d").label("ctime")
        ).filter(XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                 XcMieba2Dispatch.service_id == self.service_id,
                 XcMieba2Dispatch.is_force == 1).group_by("ctime").all()
        if dispatch_data:
            return {j: i for i, j in dispatch_data if j}
        else:
            return {}

    def query_car_dispatch_finish(self):
        dispatch_data = dao_session.session().query(
            func.count(XcMieba2Dispatch.id),
            func.date_format(XcMieba2Dispatch.created_at, "%Y-%m-%d").label("ctime")
        ).filter(XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                 XcMieba2Dispatch.service_id == self.service_id, XcMieba2Dispatch.is_finish == 1). \
            group_by("ctime").all()
        if dispatch_data:
            return {j: i for i, j in dispatch_data if j}
        else:
            return {}

    def query_dispatch_task(self, origin_type: tuple, is_force):
        dispatch_data = dao_session.session().query(
            func.ifnull(func.count(XcMieba2Dispatch.id), 0)
        ).filter(XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                 XcMieba2Dispatch.service_id == self.service_id,
                 XcMieba2Dispatch.is_force == is_force,
                 XcMieba2Dispatch.workman_id.isnot(None),
                 XcMieba2Dispatch.origin_type.in_(origin_type)).scalar()
        return dispatch_data

    def query_accept_dispatch_task(self, status, is_force):
        if status:
            status = (6, 8)
        else:
            status = (0, 1, 2, 3, 4, 5, 7, 9)
        dispatch_data = dao_session.session().query(
            func.ifnull(func.count(XcMieba2Dispatch.id), 0)
        ).filter(XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                 XcMieba2Dispatch.service_id == self.service_id,
                 XcMieba2Dispatch.is_force == is_force,
                 XcMieba2Dispatch.status.in_(status)).scalar()
        return dispatch_data

    def query_dispatch_achievement(self, origin_type: tuple, is_force):
        dispatch_data = dao_session.session().query(
            func.ifnull(func.sum(XcMieba2Dispatch.expect_achievement), 0)
        ).filter(XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                 XcMieba2Dispatch.service_id == self.service_id,
                 XcMieba2Dispatch.is_force == is_force,
                 XcMieba2Dispatch.origin_type.in_(origin_type)).scalar()
        return dispatch_data

    def query_dispatch_accept_finish(self, is_timeout, is_force):
        """完成接（派）单"""
        finish = dao_session.session().query(
            func.ifnull(func.count(XcMieba2Dispatch.id), 0)).filter(
            XcMieba2Dispatch.is_force == is_force,
            XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
            XcMieba2Dispatch.service_id == self.service_id
        )
        if is_timeout:
            finish_num = finish.filter(XcMieba2Dispatch.status == DispatchTaskType.Finish.value).scalar()
        else:
            finish_num = finish.filter(XcMieba2Dispatch.status == DispatchTaskType.ExpiredFinish.value).scalar()
        return finish_num

    def query_dispatch_accept_cancel(self, is_timeout, is_force):
        """取消接（派）单"""
        cancel = dao_session.session().query(
            func.ifnull(func.count(XcMieba2Dispatch.id), 0)). \
            join(XcMieba2DispatchWorkmanCancel, XcMieba2DispatchWorkmanCancel.dispatch_id == XcMieba2Dispatch.id). \
            filter(XcMieba2Dispatch.status == DispatchTaskType.Canceled.value,
                   XcMieba2Dispatch.is_force == is_force,
                   XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                   XcMieba2Dispatch.service_id == self.service_id)
        if is_timeout:
            cancel_num = cancel.filter(XcMieba2DispatchWorkmanCancel.created_at <= XcMieba2Dispatch.deadline).scalar()
        else:
            cancel_num = cancel.filter(XcMieba2DispatchWorkmanCancel.created_at > XcMieba2Dispatch.deadline).scalar()
        return cancel_num

    def query_dispatch_not_cancel(self, is_force):
        """未取消接（派）单"""
        cancel_num = dao_session.session().query(
            func.ifnull(func.count(XcMieba2Dispatch.id), 0)). \
            filter(XcMieba2Dispatch.status == DispatchTaskType.Expired.value,
                   XcMieba2Dispatch.is_force == is_force,
                   XcMieba2Dispatch.created_at.between(self.begin_time, self.end_time),
                   XcMieba2Dispatch.service_id == self.service_id).scalar()
        return cancel_num

    def query_dispatch_manager_cancel(self):
        """城市经理取消接（派）单"""
        cancel_list = dao_session.session().query(
            XcMieba2DispatchManagerCancel.refuse_type, func.count(XcMieba2DispatchManagerCancel.dispatch_id)). \
            join(XcMieba2Dispatch, XcMieba2Dispatch.id == XcMieba2DispatchManagerCancel.dispatch_id). \
            filter(XcMieba2DispatchManagerCancel.created_at.between(self.begin_time, self.end_time),
                   XcMieba2Dispatch.service_id == self.service_id).group_by(
            XcMieba2DispatchManagerCancel.refuse_type).all()
        return {str(i): j for i, j in cancel_list if i and j}

    def query_dispatch_workman_cancel(self):
        """工人取消接（派）单的原因"""
        cancel_list = dao_session.session().query(
            XcMieba2DispatchWorkmanCancel.cancel_type, func.count(XcMieba2DispatchWorkmanCancel.dispatch_id)). \
            join(XcMieba2Dispatch, XcMieba2Dispatch.id == XcMieba2DispatchWorkmanCancel.dispatch_id). \
            filter(XcMieba2DispatchWorkmanCancel.created_at.between(self.begin_time, self.end_time),
                   XcMieba2Dispatch.service_id == self.service_id).group_by(
            XcMieba2DispatchWorkmanCancel.cancel_type).all()
        return {str(i): j for i, j in cancel_list if i and j}


class DispatchExternalService(MBService):

    def cancel_dispatch(self, dispatch_id, reason_type=WorkmanCancelType.RIDING_CANCAL_REASON_TYPE.value):
        filters = set()
        filters.add(XcMieba2Dispatch.id == dispatch_id)
        filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))

        one = dao_session.session().query(XcMieba2Dispatch).filter(*filters).first()
        status_map = {
            2: "Process",
            3: "Canceling",
            5: "CancelRefuse"
        }
        now_status = status_map.get(one.status)
        # 2增加取消表
        params = {
            "dispatch_id": dispatch_id,
            "cancel_type": reason_type,
            "agree_time": datetime.now()
        }
        workman_cancel = XcMieba2DispatchWorkmanCancel(**params)
        dao_session.session().add(workman_cancel)
        one.status = DispatchTaskType.Canceled.value  # 直接取消
        one.actual_achievement = 0
        dao_session.session().commit()
        logger.info(
            "Change the {} state to Canceled, dispatch_id is:{}, imei is:{}".format(now_status, dispatch_id, one.imei))
        task_recycle(imei=one.imei, service_id=one.service_id, ticket_id=one.ticket_id,
                     origin_type=one.origin_type)
        DispatchWorkmanService().refresh_task_num(one.workman_id, -1)

    def cancel_task(self, valid_data):
        """
        :param valid_data: imei 准备骑车的imei
        :return:
        """
        imei, = valid_data

        now = datetime.now()
        today_start_date = now.strftime("%Y-%m-%d 00:00:00")
        today_end_date = now.strftime("%Y-%m-%d 23:59:59")

        # 查询当天此imei当天正在进行的任务
        dispatch_filters = set()
        dispatch_filters.add(XcMieba2Dispatch.imei == imei)
        dispatch_filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))
        dispatch_filters.add(XcMieba2Dispatch.created_at.between(today_start_date, today_end_date))

        dispatch_info = dao_session.session().query(XcMieba2Dispatch).filter(*dispatch_filters).all()

        # 当前设备没有进行中的任务
        if not dispatch_info:
            return False

        for d in dispatch_info:
            dispatch_id = d.id
            dispatch_reason = d.dispatch_reason
            service_id = d.service_id
            if not dispatch_reason:
                # 取消任务
                self.cancel_dispatch(dispatch_id)
                continue
            dispatch_reason_dict = json.loads(dispatch_reason)
            start = dispatch_reason_dict.get("start", None)
            if not start:
                # 取消任务
                self.cancel_dispatch(dispatch_id)
                continue
            heat = start.get("heat", None)
            if not heat or heat < 3:
                """
                    没有热度或者热度 < 3,取消任务
                """
                self.cancel_dispatch(dispatch_id)
                continue
            else:
                # 评估热度 >= 3, 更换一辆车(选择车的范围, 附近, 闲置, 有电, 不在别人任务中的车辆), 没有合适的也取消任务
                """
                    热度heat >= 3 
                    1、找出当前服务区的闲置车辆(ready)
                    2、找出不在别人当天进行中的任务的imei
                    3、找出任务附近100米内的车辆
                    4、找出电量大于40%的
                    5、更换车辆
                """
                # 任务起始位置
                start_lat, start_lng = d.start_lat, d.start_lng

                """获取当前服务区的所有ready的imei"""
                with dao_session.redis_session.r.pipeline(transaction=False) as ready_vehicle_pipeline:
                    for r in [service_id]:
                        ready_vehicle_pipeline.smembers(SERVICE_DEVICE_STATE.format(**{"service_id": r, "state": 1}))
                    ready_vehicle_res = ready_vehicle_pipeline.execute()
                print(ready_vehicle_res)
                ready_imei = set()
                for i in ready_vehicle_res:
                    ready_imei = ready_imei.union(i)
                ready_imei_list = list(ready_imei)

                if not ready_imei_list:
                    # 取消任务
                    self.cancel_dispatch(dispatch_id)
                    continue

                """找出不在别人进行中的任务的imei"""
                process_imei_filters = set()
                process_imei_filters.add(XcMieba2Dispatch.imei.in_(ready_imei_list))
                process_imei_filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))
                process_imei_filters.add(XcMieba2Dispatch.created_at.between(today_start_date, today_end_date))

                process_imei_info = dao_session.session().query(XcMieba2Dispatch.imei).filter(
                    *process_imei_filters).all()

                process_imei = list(process_imei_info[0]) if process_imei_info else []

                # 闲置的且不在别人进行中的imei
                diff_imei = list(set(ready_imei_list) ^ set(process_imei))

                if not diff_imei:
                    # 取消任务
                    self.cancel_dispatch(dispatch_id)
                    continue

                # 根据imei找出位置   xc_ebike_device_info_{imei} HASH
                with dao_session.redis_session.r.pipeline(transaction=False) as device_pipeline:
                    for i in diff_imei:
                        device_pipeline.hmget(IMEI_BINDING_DEVICE_INFO.format(**{"imei": i}), ['lat', 'lng'])
                    voltage_res = device_pipeline.execute()

                # imei与位置绑定
                imei_location_map = dict(zip(diff_imei, voltage_res))

                # 筛选有上报位置的imei
                filter_imei_location_map = dict(filter(lambda e: e[1][0] and e[1][1], imei_location_map.items()))
                # a = {k: v for k, v in imei_location_map.items() if v[0] and v[1]}
                if not filter_imei_location_map:
                    # 取消任务
                    self.cancel_dispatch(dispatch_id)
                    continue

                """找出任务附近100米内的车辆"""
                distance_imei_list = []
                for k, v in filter_imei_location_map.items():
                    distance = distance_two_points(start_lat, start_lng, float(v[0]), float(v[1]))
                    if distance < 100:
                        distance_imei_list.append(k)

                if not distance_imei_list:
                    # 取消任务
                    self.cancel_dispatch(dispatch_id)
                    continue

                print(distance_imei_list)

                """找出电量大于40%的"""
                with dao_session.redis_session.r.pipeline(transaction=False) as device_info_pipeline:
                    for i in distance_imei_list:
                        device_info_pipeline.hget(IMEI_BINDING_DEVICE_INFO.format(**{"imei": i}), "voltage")
                    voltage_res = device_info_pipeline.execute()

                with dao_session.redis_session.r.pipeline(transaction=False) as car_pipeline:
                    for i in distance_imei_list:
                        car_pipeline.get(IMEI_2_CAR_KEY.format(**{"imei": i}))
                    car_res = car_pipeline.execute()

                with dao_session.redis_session.r.pipeline(transaction=False) as battery_name_pipeline:
                    for c in car_res:
                        battery_name_pipeline.get(CAR_BINDING_BATTERY_NAME.format(**{"car_id": c}))
                    battery_name_res = battery_name_pipeline.execute()
                battery_name_list = list(
                    map(lambda a: json.loads(a).get("batteryType", None) if a else a, battery_name_res))
                logger.info("[get_electricity_statistics] battery_name_list:{}".format(battery_name_list))

                rest_battery = []
                for k, v in enumerate(voltage_res):
                    if not v:
                        rest_battery.insert(k, 0)
                        continue
                    battery = 100 * (int(v) - 3366 * 13) / (839 * 13)
                    # 根据电池品牌
                    if battery_name_list[k]:
                        battery = DashboardService().battery_type_4cell_num(float(v), battery_name_list[k])
                    if battery > 100:
                        rest_battery.insert(k, 100)
                    elif battery <= 0:
                        rest_battery.insert(k, 0)
                    else:
                        rest_battery.insert(k, battery)

                rest_battery_imei_map = dict(zip(distance_imei_list, rest_battery))

                print(rest_battery_imei_map)

                # 筛选电量大于40%的
                filter_final_imei = dict(filter(lambda e: e[1] < 40, rest_battery_imei_map.items()))

                if not filter_final_imei:
                    # 取消任务
                    self.cancel_dispatch(dispatch_id)
                    continue

                # 满足条件,随机更换一辆车
                random_imei = random.choice(list(filter_final_imei))
                dispatch_update = {"imei": random_imei}
                dao_session.session().query(XcMieba2Dispatch).filter_by(id=dispatch_id).update(dispatch_update)
                dao_session.session().commit()


class WorkmanWorkTaskDispatchService(MBService):

    def __init__(self):
        self.start_time = time.time()
        self.start_datetime = datetime.now()

    def getroute(self, workman_id, lat_lng=None):
        from scripts.dispatch.vhicle_routes import main as cal_route
        start_date_time = self.start_datetime
        # start_date_time = self.start_datetime - timedelta(days=90)
        max_nodes = 15
        # 通过workman_id获取所有的任务列表
        filters = set()
        filters.add(XcMieba2Dispatch.workman_id == workman_id)
        filters.add(XcMieba2Dispatch.status.in_(DispatchTaskType.workman_process_list()))
        filters.add(XcMieba2Dispatch.created_at > (datetime.now() - timedelta(hours=24)))
        filters.add(XcMieba2Dispatch.deadline >= start_date_time)
        try:
            res = dao_session.session().query(
                XcMieba2Dispatch.id,
                XcMieba2Dispatch.start_lat,
                XcMieba2Dispatch.start_lng,
                XcMieba2Dispatch.end_lat,
                XcMieba2Dispatch.end_lng,
                XcMieba2Dispatch.deadline,
                XcMieba2Dispatch.status,
                XcMieba2Dispatch.actual_type).filter(
                *filters).order_by(
                XcMieba2Dispatch.deadline.asc()).limit(
                max_nodes).all()
        except Exception as e:
            dao_session.session().rollback()
            raise MbException("没有可用任务")
        move_car_list = []
        other_list = []
        other_list_id = []
        move_car_start_list_id = []
        move_car_end_list_id = []
        for i in res:
            # 设定了最大节点数
            if (len(other_list) + 2 * len(move_car_list)) >= max_nodes:
                break
            start_lat = float(i.start_lat)
            start_lng = float(i.start_lng)
            dispatch_id = i.id
            tw = self.get_tw(i.deadline)
            # tw = 3600
            # 如果时间窗小于0那么不添加到当前的任务，TODO 对时间窗的设置还需要斟酌
            if tw < 0:
                continue
            if i.actual_type == ActualType.MOVE_CAR.value:
                end_lat = float(i.end_lat)
                end_lng = float(i.end_lng)
                move_car_list.append((start_lat, start_lng, end_lat, end_lng, tw))
                move_car_start_list_id.append((start_lat, start_lng, dispatch_id))
                move_car_end_list_id.append((end_lat, end_lng, dispatch_id))
            else:
                other_list.append((start_lat, start_lng, tw))
                other_list_id.append((start_lat, start_lng, dispatch_id))

        if (len(other_list) + len(move_car_list)) <= 0:
            raise MbException("没有可用任务")
        task_data = {
            "other_list": other_list,
            "move_car_list": move_car_list,
        }
        index2id = other_list_id[:]
        index2id.extend(move_car_start_list_id)
        index2id.extend(move_car_end_list_id)

        try:
            if lat_lng:
                index2id.insert(0, lat_lng)
                best_route, drop_list, steps = cal_route(task_data, lat_lng)
            else:  # 这个分支应该不会走不会走
                index2id.insert(0, (0, 0,))
                best_route, drop_list, steps = cal_route(task_data)
        except Exception as e:
            raise MbException(str(e))

        if best_route is None and drop_list is None:
            return None, None

        return [index2id[i] for i in best_route], [index2id[i] for i in drop_list], steps

    def get_tw(self, date_time):
        return (date_time + timedelta(hours=1)).timestamp() - self.start_time


class FlowService(MBService):
    def get_flow(self, vilid_data):
        """
        从datahub中获取最近一个小时的数据,并且缓存起来5分钟的缓存,从缓存中获取最近一个小时的挪车流向图
        :param vilid_data:
        :return:
        """
        service_id, hour = vilid_data
        if not self.exists_param(hour):
            hour = datetime.now().hour
        res = dao_session.redis_session.r.hget(DISPATCH_FLOW_HASH.format(hour=hour), service_id)
        if res:
            return json.loads(res)
        else:
            return {}

    def get_workman_location(self, vilid_data):

        service_id, hour = vilid_data
        if not self.exists_param(hour):
            hour = datetime.now().hour

        mapping = dao_session.redis_session.r.hgetall(
            DISPATCH_WORKMAN_HOUR_POSITION.format(service_id=service_id, hour=hour))
        if not mapping:
            return {}
        res = dao_session.session().query(XcOpman).filter(XcOpman.opManId.in_(mapping.keys())).all()
        return {one.opManId: {"name": one.name, "lat": float(mapping[one.opManId].split("_")[0]),
                              "lng": float(mapping[one.opManId].split("_")[1])} for one in res} if res else {}

    def actual_list(self, vilid_data):
        """最近一个小时的派单"""
        service_id, hour = vilid_data

        # 查找上一个时段的时间节点的数据，派车单只有[7,22]点有
        if not self.exists_param(hour):
            start_time = time.strftime('%Y-%m-%d %H:00:00', time.localtime(time.time())),
            end_time = time.strftime('%Y-%m-%d %H:00:00', time.localtime(time.time() + 60 * 60))
        else:
            # 如果传递的hour大于现在的hour，那么就是查昨天的
            if datetime.now().replace(hour=hour) > datetime.now():
                start_time = (datetime.now().replace(hour=hour) + timedelta(hours=-24)).strftime("%Y-%m-%d %H:00:00"),
                end_time = (datetime.now().replace(hour=hour) + timedelta(hours=-23)).strftime("%Y-%m-%d %H:00:00")
            else:
                start_time = datetime.now().replace(hour=hour).strftime("%Y-%m-%d %H:00:00"),
                end_time = (datetime.now().replace(hour=hour) + timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
        res = dao_session.session().query(XcMieba2Dispatch.id, XcMieba2Dispatch.imei, XcMieba2Dispatch.workman_id,
                                          XcMieba2Dispatch.start_lat, XcMieba2Dispatch.start_lng,
                                          XcMieba2Dispatch.end_lat, XcMieba2Dispatch.end_lng,
                                          XcMieba2Dispatch.members,
                                          XcOpman.name).outerjoin(
            XcOpman, XcOpman.opManId == XcMieba2Dispatch.workman_id).filter(
            XcMieba2Dispatch.service_id == service_id, XcMieba2Dispatch.origin_type == OriginType.AUTO_MOVE.value,
            XcMieba2Dispatch.created_at.between(start_time, end_time)).all()

        # created_at 是插入该表的时间，比mc里面脚本dispatch_task的report_time满一分钟左右
        return res and [{"dispatch_id": dispatch_id,
                         "imei": imei,
                         "workman_id": workman_id,
                         "start_lat": float(start_lat),
                         "start_lng": float(start_lng),
                         "end_lat": float(end_lat),
                         "end_lng": float(end_lng),
                         "members": members,
                         "workman_name": workman_name}
                        for dispatch_id, imei, workman_id, start_lat, start_lng, end_lat, end_lng, members, workman_name
                        in res]


class UserFindCarService(MBService):
    def find_car(self, valid_data, user_id):
        lat, lng, service_id = valid_data
        # 记住用户短时间内第一次寻车的位置,如果他没有后续骑行,则认为他想用车没有用上是一个潜在热点,记录到表格XcMieba2DispatchFindCar中
        res = dao_session.redis_session.r.zadd(DISPATCH_FIND_CAR, {user_id, int(time.time())}, nx=True)
        if res:
            dao_session.redis_session.r.hset(DISPATCH_USER_FIND_CAR.format(user_id=user_id),
                                             mapping={"lat": lat, "lng": lng, "service_id": service_id})
