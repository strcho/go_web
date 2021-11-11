from mbutils import (
    dao_session,
    logger,
)
from mbutils.snowflake import ID_Worker
from model.all_model import UserWallet
from service import MBService


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, valid_data):
        pin_id, _ = valid_data
        try:
            user_wallet = dao_session.session().query(UserWallet).filter_by(pin_id=pin_id).first()
        except Exception as e:
            dao_session.session().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
        return user_wallet

    def insert_one(self, vaild_data):

        data = dict(
            id=ID_Worker(),
            tenant_id="",
            created_pin=None,
            updated_pin=None
        )
        user_wallet = UserWallet(**data)
        dao_session.session().add(user_wallet)
        try:
            dao_session.commit()
            return True

        except Exception as e:
            dao_session.session().rollback()
            logger.error("insert user wallet is error: {}".format(e))
            logger.exception(e)
            return False

    def update_one(self, vaild_data):

        pin_id, _ = vaild_data
        data = dict()
        params = dict()
        # 更新余额考虑使用 update({"balance": UserWallet.balance - change})
        dao_session.session().query(UserWallet)\
            .filter_by(pin_id=pin_id).update(params)\
            .with_for_update(read=True, nowait=False, of=None)
        try:
            dao_session.session().commit()
            return True
        except Exception as e:
            dao_session.session().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            return False

    def query_list(self, valid_data, enable=2):

        pin_ids, _ = valid_data

        user_wallets = dao_session.session().query(UserWallet).filter(UserWallet.pin_id.in_(pin_ids)).all()
        data_list = []
        count = len(data_list)
        try:
            for user_wallet in user_wallets:
                data_list.append(
                    {

                    }
                )
        except Exception as e:
            dao_session.session().rollback()
            logger.error("")
            logger.exception(e)
            return False

        return data_list, count
