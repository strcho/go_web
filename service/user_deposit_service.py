import json
from datetime import datetime

from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TUserWallet
from service.wallet_service import WalletService
from utils.constant.redis_key import USER_WALLET_CACHE


class UserDepositService(WalletService):
    """
    用户押金
    """

    def get_user_deposit(self, pin: str, args: dict):
        """从redis或mysql获取用户钱包信息"""
        tenant_id = args['commandContext']['tenant_id']
        find_user_wallet = dao_session.redis_session.r.hgetall(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin))
        if find_user_wallet:
            try:
                user_wallet_dict = json.loads(find_user_wallet['content'])
            except Exception:
                print('user_wallet_dict = find_user_wallet["content"]')
                user_wallet_dict = find_user_wallet["content"]
        else:
            user_wallet: TUserWallet = self.query_one(args=args)
            user_wallet_dict = orm_to_dict(user_wallet, TUserWallet)

            if user_wallet:
                dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin),
                                                 mapping={"content": json.dumps(user_wallet_dict),
                                                          "version": datetime.now().timestamp()})

        return user_wallet_dict

    def set_user_deposit(self, pin: str, args: dict,):

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)
            if self.exists_param(args['change_deposited_mount']):
                user_wallet_dict['deposited_mount'] += args['change_deposited_mount']

            if self.exists_param(args['deposited_stats']):
                user_wallet_dict['deposited_stats'] = args['deposited_stats']

            dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(tenant_id=user_wallet_dict['tenant_id'], pin=pin),
                                             mapping={"content": json.dumps(user_wallet_dict),
                                                      "version": datetime.now().timestamp()})

            self.update_one(pin=pin, args=user_wallet_dict)
            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户押金失败")
