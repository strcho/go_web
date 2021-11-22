from datetime import (
    datetime,
    timedelta,
)

from mbutils import (
    dao_session,
    MbException,
)
from mbutils import logger
from model.all_model import TFavorableCard
from . import MBService


class FavorableCardUserService(MBService):
    """
    用户骑行卡
    """

    def query_one(self, args: dict) -> TFavorableCard:
        """
        获取当前用户的优惠卡
        """
        service_id = args['service_id']
        pin = args['pin']
        card_info = None
        try:
            card_info = dao_session.session.tenant_db().query(TFavorableCard). \
                filter(TFavorableCard.pin == pin,
                       TFavorableCard.service_id == service_id).first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("show favorable card days is error: {}".format(e))
            logger.exception(e)
        return card_info

    def query_all(self, args: dict):
        """
        获取当前用户的全部优惠卡
        """
        pin = args['pin']
        card_info_list = []
        try:
            card_info_list = dao_session.session.tenant_db().query(TFavorableCard). \
                filter(TFavorableCard.pin == pin,).all()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("show favorable card days is error: {}".format(e))
            logger.exception(e)
        return card_info_list

    # 获取当前用户的优惠卡剩余天数
    def query_one_day(self, valid_data):
        card_info = self.query_one(valid_data)
        day_time, end_time_str = -1, '-'
        if card_info:
            end_time = card_info.end_time
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M")
            day_time = (end_time - datetime.now()).days
        return day_time + 1 if day_time >= 0 else 0, end_time_str

    # 插入一张优惠卡
    def insert_one(self, args):
        commandContext = args['commandContext']
        param = self.get_model_common_field(commandContext)
        param = param.update({
            "pin": args['pin'],
            "begin_time": args['begin_time'],
            "end_time": args['end_time'],
            "config_id": args['config_id'],
            "service_id": args['service_id'],
        })
        try:
            user_card = TFavorableCard(**param)
            dao_session.session.tenant_db().add(user_card)
            dao_session.session.tenant_db().commit()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user favorable card is error: {}".format(e))
            logger.exception(e)
            return False
        return True

    def create_user_favorable_card(self, args: dict):
        """
        创建用户优惠卡
        """
        user_card: TFavorableCard = self.query_one(args)
        if not user_card:
            if self.insert_one(args):
                res = True
            else:
                res = False
        else:
            res = True
        return res

    def modify_time(self, args: dict):
        """
        编辑用户优惠卡时间
        """

        duration = args['duration']

        riding_card: TFavorableCard = self.query_one(args)
        if not riding_card:
            raise MbException("未找到优惠卡")

        try:
            if self.exists_param(duration):
                if duration == 0:
                    riding_card.end_time = datetime.now()
                else:
                    riding_card.end_time = datetime.now() + timedelta(days=duration)
            dao_session.session.tenant_db().commit()

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("modify user favorable card is error: {}".format(e))
            logger.exception(e)
            return False
