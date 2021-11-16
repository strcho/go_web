import json
from datetime import datetime

from mbutils import (
    dao_session,
    logger,
)
from mbutils.constant import SplitType
from mbutils.snowflake import ID_Worker
from model.all_model import TUserWallet
from service import MBService
from utils.constant.redis_key import USER_WALLET_CACHE


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, pin_id: str, commandContext: dict):
        try:
            tenant_id = commandContext['tenant_id']
            user_wallet = dao_session.session(tp=SplitType.Tenant.value).query(TUserWallet)\
                .filter(TUserWallet.pin_id == pin_id,
                        TUserWallet.tenant_id == tenant_id).first()
            if not user_wallet:
                is_suc = self.insert_one(pin_id, commandContext)
                if is_suc:
                    user_wallet = dao_session.session().query(TUserWallet)\
                        .filter(TUserWallet.pin_id == pin_id,
                                TUserWallet.tenant_id == tenant_id).first()
        except Exception as e:
            dao_session.session().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
        return user_wallet

    def insert_one(self, pin_id: str, commandContext: dict):

        data = self.get_model_common_field(commandContext)

        data['pin_id'] = pin_id
        print(data)
        user_wallet = TUserWallet(**data)
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
        # 更新余额考虑使用 update({"balance": TUserWallet.balance - change})
        dao_session.session().query(TUserWallet)\
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

        user_wallets = dao_session.session().query(TUserWallet).filter(TUserWallet.pin_id.in_(pin_ids)).all()
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

    def get_user_wallet(self, pin_id: str, commandContext: dict):
        """从redis或mysql获取用户钱包信息"""
        print(commandContext)
        tenant_id = commandContext['tenant_id']
        find_user_wallet = dao_session.redis_session.r.hgetall(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin_id=pin_id))
        if find_user_wallet:
            try:
                user_wallet_dict = json.loads(find_user_wallet['content'])
            except Exception:
                user_wallet_dict = find_user_wallet["content"]
        else:
            user_wallet = self.query_one(pin_id=pin_id, commandContext=commandContext)
            user_wallet_dict = dict(user_wallet)

            if user_wallet:
                dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(pin_id, tenant_id),
                                                 mapping={"content": json.dumps(user_wallet_dict),
                                                          "version": datetime.now().timestamp()})
        return user_wallet_dict

    def set_user_wallet(self, pin_id: str, commandContext: dict, ):
        dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(pin_id, user_wallet.tenant_id),
                                         mapping={"content": router_config,
                                                  "version": datetime.now().timestamp()})


        rowcount = dao_session.session().query(XcEbike2Config).filter_by(rootRouter=router,
                                                                         serviceId=service_id).update(params)
        dao_session.session().commit()
        if not rowcount:
            # 没有更新到
            config = XcEbike2Config(**params)
            dao_session.session().add(config)
            dao_session.session().commit()