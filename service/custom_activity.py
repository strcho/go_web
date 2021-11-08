import json
from datetime import timedelta

from sqlalchemy.sql import func

from model.all_model import *
from mbutils import MbException, ARG_DEFAULT
from mbutils import dao_session
from mbutils import logger
from utils.constant.redis_key import *
from . import MBService
from .message import MsgService
from .user import UserReward


class CustomActivityService(MBService):

    def query_list(self, valid_data):
        service_id, activity_id, name, page, size = valid_data
        params = {"service_id": service_id, "id": activity_id, "name": name}
        params = self.remove_empty_param(params)
        count = dao_session.session().query(func.count(XcMieba2CustomActivity.id)).filter_by(**params).scalar()
        many = dao_session.session().query(XcMieba2CustomActivity).filter_by(**params).order_by(
            XcMieba2CustomActivity.id.desc()).limit(size).offset(
            page * size).all()
        try:
            rows = [{
                "id": first.id,
                "service_id": first.service_id,
                "name": first.name,
                "begin_time": self.datetime2num(first.begin_time),
                "end_time": self.datetime2num(first.end_time),
                "extend_time": (first.extend_time - first.end_time).days,
                "state": first.state,
                "reward_type": first.reward_type,
                "reward_remark": first.reward_remark,
                "remind": json.loads(first.remind),
                "user_filter": json.loads(first.user_filter),
                "finish_condition_type": json.loads(first.finish_type),
                "finish_condition_remark": first.finish_remark,
                "win_condition_type": first.win_type,
                "win_condition_remark": first.win_remark,
                "info": json.loads(first.info)
            } for first in many]
        except Exception as ex:
            logger.exception(ex)
        return count, rows

    def query_one(self, activity_id: int):
        first = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).first()
        if first:
            return {
                "id": first.id,
                "service_id": first.service_id,
                "name": first.name,
                "begin_time": self.datetime2num(first.begin_time),
                "end_time": self.datetime2num(first.end_time),
                "extend_time": (first.extend_time - first.end_time).days,
                "state": first.state,
                "reward_type": first.reward_type,
                "reward_remark": first.reward_remark,
                "remind": json.loads(first.remind),
                "user_filter": json.loads(first.user_filter),
                "finish_condition_type": json.loads(first.finish_type),
                "finish_condition_remark": first.finish_remark,
                "win_condition_type": first.win_type,
                "win_condition_remark": first.win_remark,
                "info": json.loads(first.info)
            }
        else:
            return

    def insert_one(self, valid_data: tuple):
        """
        sendMsg_types 发送消息类型 数组 多选 1为系统通知 2为APP推送 3为短信
        finish_types
        """
        service_id, name, begin_time, end_time, extend_time, state, reward_type, reward_remark, remind, \
        user_filter, finish_type, finish_remark, win_type, win_remark, info = valid_data
        if self.exists_param(begin_time) and state == ActivityStatus.INIT.value \
                and begin_time < datetime.now().timestamp():
            state = ActivityStatus.PROCESSING.value
        if self.exists_param(end_time) and state == ActivityStatus.INIT.value \
                and end_time < datetime.now().timestamp():
            state = ActivityStatus.EXPIRE.value
        params = {
            "name": name,
            "service_id": service_id,
            "begin_time": self.num2datetime(begin_time),
            "end_time": self.num2datetime(end_time),
            "extend_time": self.num2datetime(end_time) + timedelta(days=extend_time),
            "state": state,
            "reward_type": reward_type,
            "reward_remark": reward_remark,
            "remind": json.dumps(remind),
            "user_filter": json.dumps(user_filter),
            "finish_type": json.dumps(finish_type),
            "finish_remark": finish_remark or "1",
            "win_type": win_type,
            "win_remark": win_remark,
            "info": json.dumps(info),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        params = self.remove_empty_param(params)
        activity = XcMieba2CustomActivity(**params)
        dao_session.session().add(activity)
        dao_session.session().commit()

    def update_one(self, valid_data: tuple):
        _id, service_id, name, begin_time, end_time, extend_time, state, reward_type, reward_remark, remind, \
        user_filter, finish_type, finish_remark, win_type, win_remark, info = valid_data
        if self.exists_param(begin_time) and self.exists_param(state) and state == ActivityStatus.INIT.value \
                and begin_time < datetime.now().timestamp():
            state == ActivityStatus.PROCESSING.value
        if self.exists_param(end_time) and self.exists_param(state) and state == ActivityStatus.INIT.value \
                and end_time < datetime.now().timestamp():
            state == ActivityStatus.EXPIRE.value
        params = {
            "name": name,
            "service_id": service_id,
            "begin_time": self.num2datetime(begin_time) if self.exists_param(begin_time) else ARG_DEFAULT,
            "end_time": self.num2datetime(end_time) if self.exists_param(end_time) else ARG_DEFAULT,
            "extend_time": self.num2datetime(end_time) + timedelta(days=extend_time) if self.exists_param(
                end_time) else ARG_DEFAULT,
            "state": state,
            "reward_type": reward_type,
            "reward_remark": reward_remark,
            "remind": json.dumps(remind) if self.exists_param(remind) else ARG_DEFAULT,
            "user_filter": json.dumps(user_filter) if self.exists_param(user_filter) else ARG_DEFAULT,
            "finish_type": json.dumps(finish_type) if self.exists_param(finish_type) else ARG_DEFAULT,
            "finish_remark": finish_remark,
            "win_type": win_type,
            "win_remark": win_remark,
            "info": json.dumps(info) if self.exists_param(info) else ARG_DEFAULT,
            "updated_at": datetime.now()
        }
        params = self.remove_empty_param(params)
        dao_session.session().query(XcMieba2CustomActivity).filter_by(id=_id).update(params)
        dao_session.session().commit()
        # 编辑活动后立即刷一下
        dao_session.session().query(XcMieba2CustomActivity).filter(XcMieba2CustomActivity.end_time < datetime.now(),
                                                                   XcMieba2CustomActivity.state == ActivityStatus.INIT.value).update(
            {"state": ActivityStatus.EXPIRE.value, "updated_at": datetime.now()})
        dao_session.session().query(XcMieba2CustomActivity).filter(XcMieba2CustomActivity.begin_time < datetime.now(),
                                                                   XcMieba2CustomActivity.state == ActivityStatus.INIT.value).update(
            {"state": ActivityStatus.PROCESSING.value, "updated_at": datetime.now()})
        dao_session.session().commit()

    def query_detail(self, valid_data):
        """
            {
                activity_details:
                {
                    "activity_id":1000017,
                    "service_id":1,
                    "name":activity_name,
                    "user_type":"当前全部用户",
                    "reward_type":"",
                    "reward_remark":"",
                    "begin_time":"",
                    "end_time":"",
                    "join_count":"",
                    "win_count":"",
                    "activity_state":3
                },
                "count":100,
                "users":[
                {id,name, phone,reward_type,reward_remark, user_state,join_time,get_reward_time}
                ]
            }
        :param valid_data:
        :return:
        """
        data = {
            "activity_details": {},
            "count": 0,
            "users": []
        }
        activity_id, phone, user_state, begin_time, end_time, page, size = valid_data
        activity = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
        data["activity_details"]["activity_id"] = activity_id
        data["activity_details"]["service_id"] = activity.service_id
        data["activity_details"]["name"] = activity.name
        data["activity_details"]["user_type"] = 0  # 当前全部用户
        data["activity_details"]["reward_type"] = activity.reward_type
        data["activity_details"]["reward_remark"] = activity.reward_remark
        data["activity_details"]["reward_remark"] = activity.reward_remark
        data["activity_details"]["begin_time"] = self.datetime2num(activity.begin_time)
        data["activity_details"]["end_time"] = self.datetime2num(activity.end_time)
        data["activity_details"]["activity_state"] = activity.state
        rows = dao_session.session().execute(
            "select user_state, count(*) from xc_mieba_2_user_custom_activity where activity_id=:activity_id group by user_state",
            {"activity_id": activity_id})
        total = 0
        data["activity_details"]["win_count"] = 0
        for tp, count in rows:
            tp = ActivityUserStatus(tp)
            total += count
            if tp == ActivityUserStatus.FINISH:
                data["activity_details"]["finish_count"] = count
            elif tp == ActivityUserStatus.WIN_GET or tp == ActivityUserStatus.WIN_NOT_GET:
                data["activity_details"]["win_count"] += count
        data["count"] = total
        data["activity_details"]["join_count"] = total

        select_params = {
            "activity_id": activity_id,
            "phone_where": '1=1',
            "user_state_where": "1=1",
            "begin_time_where": "1=1",
            "end_time_where": "1=1",
            "page_where": "limit {}, {}".format(page * size, size),
        }
        if self.exists_param(phone):
            select_params["phone_where"] = "u.phone={}".format(phone)
        if self.exists_param(user_state):
            select_params["user_state_where"] = "a.user_state={}".format(user_state)
        if self.exists_param(begin_time):
            select_params["begin_time_where"] = "a.get_reward_at>={}".format(self.num2datetime(begin_time))
        if self.exists_param(end_time):
            select_params["end_time_where"] = "a.get_reward_at<{}".format(self.num2datetime(end_time))
        user_infos = dao_session.session().execute("""
        select a.id, u.personInfo, u.phone, a.user_state, a.created_at, a.get_reward_at, a.finish_at
        from  xc_mieba_2_user_custom_activity a 
        join xc_ebike_usrs_2 u 
        on u.id=a.object_id
        where a.activity_id={activity_id} and {phone_where} and {user_state_where} and {begin_time_where} and {end_time_where}
        {page_where}""".format(**select_params))
        for user_info in user_infos:
            data["users"].append({
                "id": user_info["id"],
                "name": self.get_user_name(user_info["personInfo"]),
                "phone": user_info["phone"],
                "user_state": user_info["user_state"],
                "join_time": self.datetime2num(user_info["created_at"]),
                "get_reward_time": self.datetime2num(user_info["get_reward_at"]),
                "finish_at": self.datetime2num(user_info["finish_at"]),
                "reward_type": activity.reward_type,
                "reward_remark": activity.reward_remark
            })
        return data


class UserCustomActivityService(MBService):
    def query_all(self, valid_data):
        service_id, = valid_data
        many = dao_session.session().query(XcMieba2CustomActivity).filter(
            XcMieba2CustomActivity.service_id == service_id,
            XcMieba2CustomActivity.state.in_((ActivityStatus.PROCESSING.value, ActivityStatus.EXPIRE.value)),
            XcMieba2CustomActivity.begin_time < func.now(),
            func.now() < XcMieba2CustomActivity.extend_time).order_by(
            XcMieba2CustomActivity.begin_time.desc()).all()
        data = []
        for one in many:
            row = {"id": one.id}
            row.update(json.loads(one.info))
            data.append(row)
        return data

    def query_status(self, activity_id):
        """查询活动状态"""
        try:
            first = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
        except Exception:
            raise MbException(promt="活动不存在")
        return first.state, first.reward_type

    def get_user_status(self, activity_id, user_id):
        """查询用户活动状态"""
        first = dao_session.session().query(XcMieba2UserCustomActivity).filter_by(activity_id=activity_id,
                                                                                  object_id=user_id).first()
        if first:
            return True, first.user_state
        else:
            return False, False

    def insert_user(self, activity_id, user_id):
        """用户参与活动"""
        self.nx_lock(NX_JOIN_CUS_ACT_KEY.format(user_id, activity_id))
        first = dao_session.session().query(XcMieba2UserCustomActivity).filter_by(activity_id=activity_id,
                                                                                  object_id=user_id).first()
        if not first:
            params = {
                "activity_id": activity_id,
                "object_id": user_id,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            user_activity = XcMieba2UserCustomActivity(**params)
            dao_session.session().add(user_activity)
            dao_session.session().commit()

    def process_status(self, activity_id, user_id):
        try:
            activity = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
        except Exception as ex:
            raise MbException("活动不存在")

        user_activity = dao_session.session().query(XcMieba2UserCustomActivity).filter_by(activity_id=activity_id,
                                                                                          object_id=user_id).first()
        if not user_activity:
            # 用户没有参加获取，不用完成判断和获奖判断
            return activity.state, activity.reward_type, activity.win_type
        if dao_session.redis_session.r.get(
                CUSTOM_ACTIVITY_QUOTA_FULL.format(activity_id)) and user_activity.user_state in (
        ActivityUserStatus.FINISH.value, ActivityUserStatus.JOIN.value):
            user_activity.user_state = ActivityUserStatus.FINISH_NOT_WIN.value
            dao_session.session().commit()
            return activity.state, activity.reward_type, activity.win_type
        if activity.state == ActivityStatus.PROCESSING.value:
            try:
                self.nx_lock(CUSTOM_ACTIVITY_QUERY_CONTROL.format(user_id, activity_id), timeout=1 * 60)
                finish_type, finish_remark = activity.finish_type, activity.finish_remark
                win_type, win_remark = activity.win_type, activity.win_remark
                # 2.1 任务完成判定,判断活动期间，用户是否买押金卡，是否有骑行卡，是否有有效骑行次数达到多少次
                finish = True
                if user_activity.user_state == ActivityUserStatus.JOIN.value:
                    params = {"activity_id": activity_id, "user_id": user_id, "deposit_card_where": "1=1",
                              "riding_card_where": "1=1",
                              "orders_where": "1=1"}
                    tps = json.loads(finish_type)
                    # 2.2任务完成，三个条件判定
                    for tp in tps:
                        # 0 购买押金卡，1购买骑行卡， 2完成有效骑行次数
                        if int(tp) == FinishType.DEPOSIT_CARD.value:
                            params[
                                "deposit_card_where"] = """exists (select dc.id from xc_ebike_2_deposit_card as dc 
                                                    where dc.objectId=uc.object_id and dc.createdAt between uc.created_at and ca.end_time 
                                                    and dc.type in (0, 1, 5, 6, 7) )"""
                        elif int(tp) == FinishType.RIDING_CARD.value:
                            params["riding_card_where"] = '''exists (select ra.id from xc_ebike_2_ridingcard_account as ra 
                                                            where ra.objectId=uc.object_id and ra.createdAt between uc.created_at and ca.end_time 
                                                            and ra.type between 6 and 13)'''
                        elif int(tp) == FinishType.RIDING_COUNT.value:
                            params["orders_where"] = '''(select count(1) from xc_ebike_user_orders as uo 
                                                        where uo.userId=uc.object_id and uo.isPaid=1 and uo.originCost > 0 
                                                        and uo.createdAt between uc.created_at and ca.end_time) >={}'''.format(
                                finish_remark)

                    result = dao_session.session().execute('''
                    update xc_mieba_2_user_custom_activity as uc 
                    join xc_mieba_2_custom_activity ca on ca.id = uc.activity_id
                    set uc.user_state=1, uc.finish_at=now()
                    where uc.activity_id={activity_id} and uc.object_id="{user_id}" and uc.user_state=0 
                    and {deposit_card_where} and {riding_card_where} and {orders_where}'''.format(**params))
                    dao_session.session().commit()
                    if result.rowcount > 0:
                        finish = True
                    else:
                        False
                # 2.2 获得奖励判定
                if finish:
                    if RewardWinType(win_type) == RewardWinType.UN_LIMIT:
                        dao_session.session().execute(
                            """
                            update xc_mieba_2_user_custom_activity set user_state=2, win_at=now() 
                            where activity_id=:activity_id and object_id=:user_id and user_state=1""",
                            {"activity_id": activity_id, "user_id": user_id}
                        )
                        dao_session.session().commit()
                    elif RewardWinType(win_type) == RewardWinType.RANK:
                        if dao_session.redis_session.r.get(CUSTOM_ACTIVITY_QUOTA_FULL.format(activity_id)):
                            return activity.state, activity.reward_type, activity.win_type
                        result = dao_session.session().execute(
                            """
                            update xc_mieba_2_user_custom_activity ca2 set user_state=2, win_at=now() 
                            where ca2.activity_id=:activity_id and ca2.object_id=:user_id and ca2.user_state=1 and ca2.id in (
                                select t1.id from (
                                select ca1.id from xc_mieba_2_user_custom_activity ca1 where ca1.activity_id=:activity_id 
                                order by ca1.finish_at, ca1.created_at asc limit :limit_num
                                ) t1
                            )""", {"activity_id": activity_id, "user_id": user_id, "limit_num": int(win_remark)})
                        # 用户没有名额和没有完成都可以造成更新失败，最好直接查询已经获奖数目来迅速停止活动
                        dao_session.session().commit()
                        if result.rowcount == 0:
                            res = dao_session.session().execute(
                                "select count(*) from `xc_mieba_2_user_custom_activity` where  `activity_id` =:activity_id and `user_state` in (2,3)",
                                {"activity_id": activity_id})
                            if res.scalar() >= int(win_remark):
                                # 名额用完了，后续用户都变成未获奖
                                dao_session.session().execute(
                                    """
                                    update xc_mieba_2_user_custom_activity ca2 set user_state=4, win_at=now() 
                                    where ca2.activity_id=:activity_id and ca2.user_state in (0, 1)""",
                                    {"activity_id": activity_id})
                                dao_session.session().commit()
                                timeout = self.datetime2num(activity.end_time) - self.datetime2num(datetime.now())
                                if timeout > 0:
                                    self.nx_lock(CUSTOM_ACTIVITY_QUOTA_FULL.format(activity_id), timeout=timeout)
            except MbException:
                pass
            except Exception as ex:
                logger.exception(ex)
        return activity.state, activity.reward_type, activity.win_type

    def user_fit(self, activity_id, user_id):
        """判断用户是否能参加活动"""
        user_filter = dao_session.redis_session.r.get(CUSTOM_ACTIVITY_USER_FILTER.format(activity_id))
        if not user_filter:
            activity = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
            user_filter = activity.user_filter
            dao_session.redis_session.r.set(CUSTOM_ACTIVITY_USER_FILTER.format(activity_id), user_filter, ex=5 * 60)
        user_filter = json.loads(user_filter)
        valid_data = self.get_or_default(user_filter, "phone_list"), \
                     self.get_or_default(user_filter, "authed"), \
                     self.get_or_default(user_filter, "deposited"), \
                     self.get_or_default(user_filter, "riding_card_type"), \
                     self.get_or_default(user_filter, "register_begin"), \
                     self.get_or_default(user_filter, "register_end"), \
                     self.get_or_default(user_filter, "balance_condition"), \
                     self.get_or_default(user_filter, "balance_amount")
        return self.query_filter_count(valid_data, user_id)

    def query_filter_count(self, valid_data, user_id=None):
        """
        查询符合条件的用户数目
        select count(distinct(u.id)) from xc_ebike_usrs_2 as u join xc_ebike_2_riding_card r
        where u.phone in ("15179232547","15179928540","18365470687","15031030888","18809934354","18000306966","13755276540")
        and u.authed = 1 and u.deposited = 0 and u.balance >= 0 and u.createdAt > 0 and u.createdAt > 0
        and r.ridingCardType=6 and r.cardExpiredDate>now()
        :param valid_data:user_filter所有用户过滤条件的元组
        :param user_id:单个用户是否满足条件
        :return:
        """
        phone_list, authed, deposited, riding_card_type, register_begin, register_end, balance_condition \
            , balance_amount = valid_data
        params = {"phone_where": "1=1", "authed_where": "1=1", "deposited_where": "1=1", "riding_card_where": "1=1",
                  "register_begin_where": "1=1", "register_end_where": "1=1", "balance_where": "1=1",
                  "user_where": "1=1"}
        if self.exists_param(phone_list) and len(phone_list):
            phone_list = phone_list.replace("；", ';').split(";")
            if len(phone_list) <= 1:
                params["phone_where"] = "u.phone = '{}'".format(phone_list[0])
            else:
                params["phone_where"] = "u.phone in {}".format(tuple(phone_list))
        if self.exists_param(authed):
            params["authed_where"] = "u.authed = {}".format(authed)
        if self.exists_param(deposited):
            params["deposited_where"] = "u.deposited = {}".format(deposited)
        if self.exists_param(balance_amount) and self.exists_param(balance_condition):
            if balance_condition > 0:
                params["balance_where"] = "u.balance > {}".format(balance_amount)
            elif balance_condition == 0:
                params["balance_where"] = "u.balance = {}".format(balance_amount)
            else:
                params["balance_where"] = "u.balance < {}".format(balance_amount)
        if self.exists_param(register_begin) and register_begin:
            params["register_begin_where"] = "u.createdAt > '{}'".format(self.num2datetime(register_begin))
        if self.exists_param(register_end) and register_end:
            params["register_end_where"] = "u.createdAt < '{}'".format(self.num2datetime(register_end))
        if self.exists_param(riding_card_type):
            # xc_ebike_2_riding_card.ridingCardType来源于xc_ebike_2_riding_config.content.serialType
            card_id = riding_card_type.split(',')[0]
            riding_config = dao_session.session().query(XcEbike2RidingConfig).filter_by(id=card_id).first()
            card_type = riding_config.type
            params["riding_card_where"] = "r.ridingCardType = {} and r.cardExpiredDate > now()".format(card_type)
        if user_id:
            params["user_where"] = "u.id = '{}'".format(user_id)

        count = dao_session.session().execute('''
            select count(distinct(u.id)) from xc_ebike_usrs_2 as u left join xc_ebike_2_riding_card r 
            on u.id=r.objectId
            where {user_where} and {phone_where}
            and {authed_where} and {deposited_where} and {balance_where} 
            and {register_begin_where} and {register_end_where}
            and {riding_card_where}
        '''.format(**params))
        return count and count.scalar()  # 获取第一行第一列并关闭结果集

    def update_is_get_reward(self, activity_id, user_id):
        try:
            # 0.防止并发的处理
            self.nx_lock(NX_GET_REWARD_CUS_ACT_KEY.format(user_id, activity_id))
            activity = dao_session.session().query(XcMieba2CustomActivity).filter_by(id=activity_id).one()
            # 2. 根据活动奖励发放奖励
            # 2.1 用户领取奖励后，状态改成已经领奖
            res = UserReward().add_reward_2_user(user_id, RewardType(activity.reward_type), activity.reward_remark, 3)
            dao_session.session().query(XcMieba2UserCustomActivity).filter_by(activity_id=activity_id,
                                                                              object_id=user_id).update(
                {"user_state": ActivityUserStatus.WIN_GET.value,
                 "updated_at": datetime.now(),
                 "get_reward_at": datetime.now()})
            if res:
                # 3.记录关键日志
                logger.info('add rewardInfo to user success', user_id, activity.reward_type, activity.reward_remark,
                            activity.remind)
                # 4.记录到固定活动中
                u = dao_session.session().query(XcEbikeUsrs2).filter_by(id=user_id).one()
                params = {
                    "userId": user_id,
                    "serviceId": u.serviceId,
                    "userName": self.get_user_name(u.personInfo),
                    "phoneNo": u.phone,
                    "activityId": activity_id,
                    "receiveState": 0,
                    "consumpteState": 0,
                    "name": activity.name,
                    "rewardInfo": json.dumps({"type": activity.reward_type, "remark": activity.reward_remark}),
                    "receiveTime": datetime.now(),
                    "createAt": datetime.now(),
                    "updateAt": datetime.now()
                }
                regular_activity = XcEbike2UserActivity(**params)
                dao_session.session().add(regular_activity)
                dao_session.session().commit()

                # 5.发送消息给用户
                try:
                    MsgService().send_activity_msg(user_id, activity.name, activity.reward_type, activity.reward_remark,
                                                   activity.remind)
                except Exception as ex:
                    logger.exception(ex)
                return {"activity_id": activity.id,
                        "name": activity.name,
                        "reward_remark": activity.reward_remark,
                        "reward_type": activity.reward_type}
            else:
                logger.info('add rewardInfo to user failed', user_id, activity.reward_type, activity.reward_remark)
        except Exception as ex:
            dao_session.session().rollback()
            logger.exception(ex)
            raise MbException(promt="user {} get reward failed, activity_id is {}".format(user_id, activity_id))
