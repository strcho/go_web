import json
from datetime import datetime

from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    DefaultMaker,
)
from model.all_model import TUserWallet
from service import MBService
from utils.constant.redis_key import USER_WALLET_CACHE


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, pin: str, args: dict):
        try:
            tenant_id = args['commandContext']['tenant_id']
            user_wallet = dao_session.session.tenant_db().query(TUserWallet)\
                .filter(TUserWallet.pin == pin,
                        TUserWallet.tenant_id == tenant_id).first()
            print('insert_one', '0000000', user_wallet)
            if not user_wallet:
                print('insert_one', '11111111')
                is_suc = self.insert_one(pin, args)
                if is_suc:
                    print('insert_one', '222222')
                    user_wallet = dao_session.session.tenant_db().query(TUserWallet)\
                        .filter(TUserWallet.pin == pin,
                                TUserWallet.tenant_id == tenant_id).first()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("query user wallet is error: {}".format(e))
            logger.exception(e)
        return user_wallet

    def insert_one(self, pin: str, args: dict):

        commandContext = args['commandContext']
        data = self.get_model_common_field(commandContext)

        data['pin'] = pin
        print(data)
        user_wallet = TUserWallet(**data)
        dao_session.session.tenant_db().add(user_wallet)
        try:
            print('insert one')
            dao_session.session.tenant_db().commit()
            return True

        except Exception as e:
            print('insert one, 222222', e)
            dao_session.session.tenant_db().rollback()
            logger.error("insert user wallet is error: {}".format(e))
            logger.exception(e)
            return False

    def update_one(self, pin: str, args: dict):

        params = dict(
            balance=args['balance'],
            recharge=args['recharge'],
            present=args['present'],
            deposited_mount=args['deposited_mount'],
            deposited_stats=args['deposited_stats'],
        )

        try:
            # 更新余额考虑使用 update({"balance": TUserWallet.balance - change})
            dao_session.session.tenant_db().query(TUserWallet) \
                .filter(TUserWallet.pin == args["pin"], TUserWallet.tenant_id == args["tenant_id"]) \
                .update(params)
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            return False

    def query_list(self, valid_data, enable=2):

        pins, commandContext = valid_data

        user_wallets = dao_session.session.tenant_db()\
            .query(TUserWallet)\
            .filter(TUserWallet.pin.in_(pins), TUserWallet.tenant_id == commandContext['tenant_id'])\
            .all()
        data_list = []
        count = len(data_list)
        try:
            for user_wallet in user_wallets:
                user_wallet: TUserWallet = user_wallet
                data_list.append(
                    dict(
                        id=user_wallet.id,
                        tenant_id=user_wallet.tenant_id,
                        created_at=user_wallet.created,
                        created_pin=user_wallet.created_pin,
                        updated_at=user_wallet.updated_at,
                        updated_pin=user_wallet.updated_pin,
                        version=user_wallet.version,
                        iz_del=user_wallet.iz_del,
                        pin=TUserWallet.pin,
                        balance=TUserWallet.balance,
                        recharge=TUserWallet.recharge,
                        present=TUserWallet.present,
                        deposited_mount=TUserWallet.deposited_mount,
                        deposited_stats=TUserWallet.deposited_stats,
                    )
                )
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("")
            logger.exception(e)
            return False

        return data_list, count

    def get_user_wallet(self, pin: str, args: dict):
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
            user_wallet: TUserWallet = self.query_one(pin=pin, args=args)
            user_wallet_dict = orm_to_dict(user_wallet, TUserWallet)

            if user_wallet:
                dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin),
                                                 mapping={"content": json.dumps(user_wallet_dict),
                                                          "version": datetime.now().timestamp()})

        return user_wallet_dict

    def set_user_wallet(self, pin: str, args: dict,):

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)

            if not isinstance(args.get('change_recharge'), DefaultMaker):
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']

            if not isinstance(args.get('change_present'), DefaultMaker):
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['recharge'] += args['change_present']

            if not isinstance(args.get('change_deposited_mount'), DefaultMaker):
                user_wallet_dict['balance'] += args['change_recharge']

            if not isinstance(args.get('deposited_stats'), DefaultMaker):
                user_wallet_dict['deposited_stats'] = args['deposited_stats']

            dao_session.redis_session.r.hset(USER_WALLET_CACHE.format(tenant_id=user_wallet_dict['tenant_id'], pin=pin),
                                             mapping={"content": json.dumps(user_wallet_dict),
                                                      "version": datetime.now().timestamp()})

            self.update_one(pin=pin, args=user_wallet_dict)
            return True

        except Exception as e:
            return False
