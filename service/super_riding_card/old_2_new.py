import json
import datetime
import base64
from model.all_model import XcOpman, XcRoleProperty, XcEbike2RidingConfig, XcEbike2RidingCard, XcEbike2SuperRidingCard, \
    XcEbike2RidingCountCard, XcEbikeUsrs2
from service import MBService
from mbutils import dao_session
from utils.constant.account import *
from utils.constant.redis_key import USER_SUPER_CARD, RECE_TIMES_KEY, REVERT_USER_SUPER_CARD
from mbutils import logger, MbException


class OldConvertNewService(MBService):
    def my_old_2_new(self, user_id):
        # 转化个人骑行卡
        # 可能要做的事情是, 删除骑行卡表, 删除次卡表, 添加super骑行卡表, 修改account表的链接顺序.
        # 或者该状态, 弃用老骑行卡, 老次卡, 添加super骑行卡
        value = dao_session.redis_session.r.hget(USER_SUPER_CARD, user_id)
        if not value:
            my_card = dao_session.session.tenant_db().query(XcEbike2RidingCard).filter(XcEbike2RidingCard.objectId == user_id,
                                                                             XcEbike2RidingCard.cardExpiredDate > datetime.datetime.now()).first()
            if my_card:
                content = json.loads(my_card.content)
                params = {
                    "objectId": user_id,
                    "deductionType": DeductionType.TIME.value,
                    "configId": 0,
                    "freeTime": content.get("freeTimeseconds", 0) or float(content.get("freeTime", 0)) * 3600,
                    "freeDistance": 0,
                    "freeMoney": 0,
                    "isTotalTimes": 0,
                    "receTimes": int(content["receTimes"]),
                    "effectiveServiceIds": "",
                    "remainTimes": int(content["receTimes"]) - int(dao_session.redis_session.r.get(
                        RECE_TIMES_KEY.format(user_id=user_id)) or 0),
                    "lastUseTime": datetime.datetime.now(),
                    "startTime": datetime.datetime.now(),
                    "cardExpiredDate": my_card.cardExpiredDate,
                    "content": my_card.content,
                    "state": UserRidingCardState.USING.value,
                    "createdAt": datetime.datetime.now(),
                    "updatedAt": datetime.datetime.now()
                }
                try:
                    my_card.cardExpiredDate = datetime.datetime.now()
                    user_card = XcEbike2SuperRidingCard(**params)
                    dao_session.session.tenant_db().add(user_card)
                    dao_session.session.tenant_db().commit()
                except Exception:
                    dao_session.session.tenant_db().rollback()
                    raise MbException("转换超级骑行卡失败")

            my_count_cards = dao_session.session.tenant_db().query(XcEbike2RidingCountCard).filter(
                XcEbike2RidingCountCard.objectId == user_id,
                XcEbike2RidingCountCard.cardExpiredDate > datetime.datetime.now()).all()
            for my_count_card in my_count_cards:
                content = json.loads(my_count_card.content)
                params = {
                    "objectId": user_id,
                    "deductionType": DeductionType.COUNT.value,
                    "configId": my_count_card.configId,
                    "freeTime": content.get("freeTimeseconds", 0) or float(content.get("freeTime", 0)) * 3600,
                    "freeDistance": 0,
                    "freeMoney": 0,
                    "isTotalTimes": 1,  # TODO, config中区分
                    "receTimes": my_count_card.receTimes,
                    "effectiveServiceIds": "",
                    "remainTimes": my_count_card.receTimes - my_count_card.usedreceTimes,
                    "lastUseTime": datetime.datetime.now(),
                    "startTime": datetime.datetime.now(),
                    "cardExpiredDate": my_count_card.cardExpiredDate,
                    "content": my_count_card.content,
                    "state": UserRidingCardState.USING.value,
                    "createdAt": datetime.datetime.now(),
                    "updatedAt": datetime.datetime.now()
                }
                try:
                    my_count_card.cardExpiredDate = datetime.datetime.now()
                    my_count_card.state = UserRidingCardState.EXPIRED.value
                    user_card = XcEbike2SuperRidingCard(**params)
                    dao_session.session.tenant_db().add(user_card)
                    dao_session.session.tenant_db().commit()
                except Exception:
                    dao_session.session.tenant_db().rollback()
                    raise MbException("转换超级骑行次卡失败")
            dao_session.redis_session.r.hset(USER_SUPER_CARD, user_id, "Y")

    def revert(self, valid_data):
        phone, is_all = valid_data
        if datetime.datetime.now().strftime('%Y-%m-%d') > "2021-08-13":
            return "非法调用"
        if self.exists_param(phone):
            one = dao_session.session.tenant_db().query(XcEbikeUsrs2.id).filter_by(phone=phone).first()
            if not one:
                return "找不到用户"
            dao_session.redis_session.r.hset(REVERT_USER_SUPER_CARD, one.id, "Y")
        elif is_all:
            data = dao_session.redis_session.r.hgetall(USER_SUPER_CARD)
            dao_session.redis_session.r.hset(REVERT_USER_SUPER_CARD, mapping=data)
        return "操作成功"

    def revert_cancel(self, valid_data):
        phone, is_all = valid_data
        if datetime.datetime.now().strftime('%Y-%m-%d') > "2021-08-13":
            return "非法调用"
        if self.exists_param(phone):
            one = dao_session.session.tenant_db().query(XcEbikeUsrs2.id).filter_by(phone=phone).first()
            if not one:
                return "找不到用户"
            dao_session.redis_session.r.hdel(REVERT_USER_SUPER_CARD, one.id)
        elif is_all:
            dao_session.redis_session.r.delete(REVERT_USER_SUPER_CARD)
        return "操作成功"

    def conf_old_2_new(self):
        # 如果数据没有这两个字段, 则增加表字段, content更新
        # dao_session.session.tenant_db().execute("""ALTER TABLE xc_ebike_2_riding_config DROP COLUMN ridingCardName;ALTER TABLE xc_ebike_2_riding_config DROP COLUMN sort_num;""")
        result = dao_session.session.tenant_db().execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'xc_ebike_2_riding_config' and column_name = 'ridingCardName'
        """)
        if not result.rowcount:
            # 如果没有字段,先改表加字段.
            dao_session.session.tenant_db().execute("""
            ALTER TABLE xc_ebike_2_riding_config MODIFY COLUMN content varchar(4096);
            """)
            dao_session.session.tenant_db().execute("""
            alter table `xc_ebike_2_riding_config`
            add column `ridingCardName` varchar(64) null comment '骑行卡名称' after `serviceId`,
            add column `sort_num` integer (10) default 0 not null comment '排序值' after `ridingCardName`
            """)
        all = dao_session.session.tenant_db().query(XcEbike2RidingConfig).filter(
            XcEbike2RidingConfig.type <= SERIAL_TYPE.RIDING_COUNT_CARD.value).all()
        for one in all:  # _id, _type, content, state, serviceId, ridingCardName, sort_num
            try:
                if not one.ridingCardName:
                    logger.info("转换骑行卡配置:{}".format(one.id))
                    old_content = json.loads(one.content)
                    ridingCardName = old_content.get("slogan", "") or old_content.get("ridingCardName")
                    old_content["createdAt"] = old_content.get("createdAt", None) or datetime.datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ")
                    sort_num = int(
                        datetime.datetime.strptime(old_content["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
                    old_content[
                        "deductionType"] = DeductionType.COUNT.value if one.type == SERIAL_TYPE.RIDING_COUNT_CARD.value else DeductionType.TIME.value
                    old_content["freeMoney"] = 0
                    old_content["isTotalTimes"] = one.type == SERIAL_TYPE.RIDING_COUNT_CARD.value
                    old_content["freeDistance"] = 0
                    old_content["effectiveServiceIds"] = ""
                    old_content["autoOpen"] = False
                    old_content["openStartTime"] = 0
                    old_content["openEndTime"] = 0
                    old_content["descriptionTag"] = "限全国"
                    old_content["promotionTag"] = "人气优选"
                    old_content["freeTimeseconds"] = int(float(old_content["freeTime"]) * 3600)
                    # 需要base64加密
                    old_content["detailInfo"] = str(
                        base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                            old_content["expiryDate"],
                            "累计" if one.type == SERIAL_TYPE.RIDING_COUNT_CARD.value else "每日",
                            old_content["receTimes"], round(float(old_content["freeTime"]) * 60), 1).encode("utf-8")),
                        "utf-8")
                    old_content["createdPhone"] = ""
                    old_content["ridingCardName"] = ridingCardName  # ridingCardName 全部变成slogan名称
                    content = json.dumps(old_content)
                    one.content = content
                    one.ridingCardName = ridingCardName
                    one.sort_num = sort_num
                    one.updatedAt = datetime.datetime.now()
                    dao_session.session.tenant_db().commit()
            except Exception as e:
                logger.debug("OldConvertNewService.conf_old_2_new error {}".format(e))
                dao_session.session.tenant_db().rollback()
