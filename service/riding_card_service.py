import base64
import json
from datetime import (
    datetime,
    timedelta,
)

from sqlalchemy import or_

from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    DefaultMaker,
    MbException,
)

from model.all_model import (
    TRidingCard,
)
from service import MBService
from utils.constant.account import UserRidingCardState
from utils.constant.config import ConfigName
from utils.constant.redis_key import (
    USER_WALLET_CACHE,
    ALL_USER_LAST_SERVICE_ID,
)


class RidingCardService(MBService):
    """
    骑行卡
    """

    def query_one(self, card_id: str, tenant_id: str = None):
        """
        通过id查询一张骑行卡
        """
        try:
            filter_param = [TRidingCard.id == card_id]
            if tenant_id is not None:
                filter_param.append(TRidingCard.tenant_id == tenant_id)
            riding_card = dao_session.session.tenant_db().query(TRidingCard)\
                .filter(*filter_param).first()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
            riding_card = None
        return riding_card

    def insert_one(self, pin_id: str, args: dict):
        """
        插入一天骑行卡记录
        :param pin_id:
        :param args:
        :return:
        """
        commandContext = args['commandContext']
        data = self.get_model_common_field(commandContext)

        data['pin_id'] = pin_id
        print(data)
        user_riding_card = TRidingCard(**data)
        dao_session.session.tenant_db().add(user_riding_card)
        try:
            print('insert one')
            dao_session.session.tenant_db().commit()
            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user riding card is error: {}".format(e))
            logger.exception(e)
            return False

    def modify_time(self, args: dict):

        card_id = args.get('card_id')
        remain_times = args.get('remain_times')
        duration = args.get('duration')
        _args = (card_id,)
        tenant_id = args.get('tenant_id')
        if self.exists_param(tenant_id):
            _args = (card_id, tenant_id)

        riding_card: TRidingCard = self.query_one(*_args)
        if not riding_card:
            raise MbException("无效的骑行卡")

        try:
            if self.exists_param(duration):
                riding_card.card_expired_date = datetime.now() + timedelta(days=duration)
            if self.exists_param(remain_times):
                riding_card.remain_times = remain_times
            riding_card.updated_at = datetime.now()
            dao_session.session.tenant_db().commit()
        except Exception:
            raise MbException("修改骑行卡时长失败")

    def user_card_info(self, valid_data):
        """
        获取用户骑行卡信息
        """
        pin_id, _ = valid_data
        try:
            dao_session.session.tenant_db().query(TRidingCard).filter(
                TRidingCard.state == UserRidingCardState.USING.value,
                TRidingCard.pin_id == pin_id,
                TRidingCard.card_expired_date <= datetime.now()).update(
                {"state": UserRidingCardState.EXPIRED.value})
            dao_session.session.tenant_db().commit()
        except Exception:
            pass
        service_id = dao_session.redis_session.r.hget(ALL_USER_LAST_SERVICE_ID, pin_id) or 0
        return self.query_my_list_in_platform((service_id,), pin_id)

    def query_my_list_in_platform(self, valid_data: tuple, pin_id: str) -> dict:
        """
        :return:
    {
        "used":[
            {
                "card_id":10001,
                "name":"7天酷骑卡",
                "image_url":"http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.png",
                "description_tag":"仅限武汉地区;通勤",
                "promotion_tag":"热销",
                "detail_info":"<p>富文本</p>",
                "expired_date":"2021-04-09 11:12:13",
                "remain_times":2
            }
        ],
        "cost_use":10001
    }
        """
        service_id, = valid_data
        dao_session.redis_session.r.hset(ALL_USER_LAST_SERVICE_ID, pin_id, service_id)

        # 暂时移除此字段
        # rule_info = ConfigService().get_router_content(ConfigName.SUPERRIDINGCARD.value, service_id)\
        #     .get("rule_info", "")

        first_id = self.get_current_card_id(service_id, pin_id)
        res_dict = {"used": [], "expired": [], "rule_info": "", "cost_use": first_id}
        result = dao_session.session().query(TRidingCard).filter(
            TRidingCard.pin_id == pin_id,
            TRidingCard.state <= UserRidingCardState.EXPIRED.value,
            TRidingCard.card_expired_date >= datetime.now() - timedelta(weeks=13)).order_by(
            TRidingCard.created_at.desc()).all()
        for one in result:
            one: TRidingCard = one
            car_info = {"card_id": one.id}
            content = json.loads(one.content)
            car_info["name"] = content["ridingCardName"]
            car_info["image_url"] = content["backOfCardUrl"]
            car_info["description_tag"] = content.get("descriptionTag", "限全国")
            car_info["detail_info"] = content.get("detailInfo", "") or str(
                    base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                        content["expiryDate"],
                        "累计" if one.is_total_times else "每日",
                        content["receTimes"], int(float(content["freeTime"]) * 60)).encode("utf-8")),
                    "utf-8")
            car_info["cardExpiredDate"] = self.datetime2num(one.card_expired_date)
            car_info["remain_times"] = one.remain_times
            car_info["is_total_times"] = one.is_total_times
            car_info["rece_times"] = one.rece_times
            car_info["free_time_second"] = one.free_time
            car_info["free_distance_meter"] = one.free_distance
            car_info["free_money_cent"] = one.free_money
            car_info["promotion_tag"] = content.get("promotionTag", "人气优选")
            car_info["deductionType"] = one.deduction_type
            if one.state != UserRidingCardState.EXPIRED.value:
                res_dict["used"].append(car_info)
            else:
                res_dict['expired'].append(car_info)
        return res_dict


    @staticmethod
    def get_current_card_id(service_id: int, pin_id: str) -> int:
        """
        该用户的,没有过期的, 使用中的,  union,
        (无次卡的, deductionType越小, 最后一次使用不是今日的, 剩余次数最多的额)(次卡的, 过期时间最近的, 次数够的)
        服务区没有配置或者在配置服务区里面的
        :param service_id:
        :param pin_id:
        :return:
        """

        # 1.骑行卡过期判定
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin_id == pin_id,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date < datetime.now()
        ).update({"state": UserRidingCardState.EXPIRED.value})

        # 2.骑行卡次数重置, 如果上次使用时间不是今天的, 则把非次卡的, 时间和剩余次数重置到最多再计算
        dao_session.session.tenant_db().query(
            TRidingCard
        ).filter(
            TRidingCard.pin_id == pin_id,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.is_total_times == 0,
            or_(TRidingCard.last_use_time < datetime.now().date(),
                TRidingCard.last_use_time is None),
        ).update(
            {
                "remain_times": TRidingCard.rece_times,
                "last_use_time": datetime.now()
            }
        )
        dao_session.session.tenant_db().commit()
        # 3.选出最佳骑行卡id
        many = dao_session.session().query(
            TRidingCard.id,
            TRidingCard.effective_service_ids
        ).filter(
            TRidingCard.pin_id == pin_id,
            TRidingCard.state == UserRidingCardState.USING.value,
            TRidingCard.card_expired_date >= datetime.now(),
            TRidingCard.remain_times > 0
        ).order_by(TRidingCard.deduction_type.asc(), TRidingCard.is_total_times.asc(),
                   TRidingCard.card_expired_date.asc()).all()
        for _id, effective_service_ids in many:
            if not effective_service_ids or effective_service_ids == "all":
                return _id
            else:
                if str(service_id) in effective_service_ids.split(";"):
                    return _id
        return None
