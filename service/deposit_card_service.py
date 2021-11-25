from datetime import (
    datetime,
    timedelta,
)

from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TDepositCard
from service import MBService


class DepositCardService(MBService):
    """
    押金卡（会员卡）
    """

    def query_one(self, args: dict):
        """
        查询一张押金卡
        """

        try:
            deposit_card: TDepositCard = (
                dao_session.session.tenant_db().query(TDepositCard).filter(
                    TDepositCard.tenant_id == args['commandContext']['tenant_id'],
                    TDepositCard.pin == args['pin'],
                ).first()
            )
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('查询用户押金卡失败')

        return deposit_card

    def insert_one(self, args: dict):
        """
        插入一张押金卡
        """

        try:
            params = self.get_model_common_field(args['commandContext'])
            params.update({
                "pin": args['pin'],
                # "config_id": args['config_id'],
                # "money": args['money'],
                # "channel": args['channel'],
                # "days": args['days'],
                # "trade_no": args['trade_no'],
                # "content": args['content'],
                # "service_id": args['service_id'],
                "expired_date": datetime.now() + timedelta(days=args['duration']),
            })
            deposit_card = TDepositCard(**params)
            dao_session.session.tenant_db().add(deposit_card)
            dao_session.session.tenant_db().commit()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('用户获取押金卡失败')

        return deposit_card

    def update_one(self, args: dict):
        """
        更新一张押金卡
        """
        pass

    def send_deposit_card(self, args: dict):
        """
        向用户发送一张押金卡
        """

        try:
            deposit_card: TDepositCard = self.query_one(args)
            # 有卡则更新卡过期时间
            if deposit_card:
                self.modify_deposit_card_time(args)
            else:
                deposit_card = self.insert_one(args)
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('用户获取押金卡失败')

        return True if deposit_card else False

    def modify_deposit_card_time(self, args: dict):
        """
        更新押金卡时限
        """

        try:
            deposit_card: TDepositCard = self.query_one(args)

            if not deposit_card:
                raise MbException('用户没有押金卡')

            # 可用时长设置为 0
            days = args['duration']
            if days == 0:
                expired_date = datetime.now()
            else:
                if deposit_card.expired_date > datetime.now():
                    expired_date = deposit_card.expired_date + timedelta(days=days)
                else:
                    expired_date = datetime.now() + timedelta(days=days)

            deposit_card.expired_date = expired_date
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user deposit card is error: {}".format(ex))
            logger.exception(ex)
            raise MbException('更新用户押金卡时限失败')

    def query_one_day(self, args):
        """
        获取当前用户的押金卡剩余天数
        """

        card_info: TDepositCard = self.query_one(args)
        print(card_info)
        day_time, expired_date_str = -1, '-'
        if card_info:
            expired_date = card_info.expired_date
            expired_date_str = expired_date.strftime("%Y-%m-%d %H:%M")
            day_time = (expired_date - datetime.now()).days
            print(day_time)

        data = {
            'days': day_time + 1 if day_time >= 0 else 0,
            'expired_date_str': expired_date_str
        }

        return data
