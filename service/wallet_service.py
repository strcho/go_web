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
        params = {"pin_id": pin_id}
        try:
            user_wallet = dao_session.session().query(UserWallet).filter_by(**params).first()
        except Exception as e:
            dao_session.session().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
        return user_wallet

    def insert_one(self, vaild_data):

        try:
            data = dict(
                id=ID_Worker(),
                tenant_id="",
                created_pin=None,
                updated_pin=None
            )
        except Exception as e:
            dao_session.session().rollback()
            logger.error("insert user wallet is error: {}".format(e))
            logger.exception(e)