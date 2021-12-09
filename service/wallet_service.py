import json
from datetime import datetime

from mbshort.str_and_datetime import orm_to_dict
from mbutils import (
    dao_session,
    logger,
    DefaultMaker,
    MbException,
)
from model.all_model import TUserWallet
from service import MBService
from utils.constant.redis_key import USER_WALLET_CACHE


class WalletService(MBService):
    """
    钱包
    """

    def query_one(self, args: dict):
        user_wallet = {}
        try:
            pin = args['pin']
            tenant_id = args['commandContext']['tenant_id']
            user_wallet = dao_session.session.tenant_db().query(TUserWallet)\
                .filter(TUserWallet.pin == pin,
                        TUserWallet.tenant_id == tenant_id).first()
            if not user_wallet:
                is_suc = self.insert_one(pin, args)
                if is_suc:
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
        user_wallet = TUserWallet(**data)
        dao_session.session.tenant_db().add(user_wallet)
        try:
            dao_session.session.tenant_db().commit()
            return True
        except Exception as e:
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
                .filter(TUserWallet.pin == pin, TUserWallet.tenant_id == args["tenant_id"]) \
                .update(params)
            dao_session.session.tenant_db().commit()
        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def query_list(self, valid_data, enable=2):

        pin_list, commandContext = valid_data

        print(pin_list, commandContext)
        user_wallets = dao_session.session.tenant_db()\
            .query(TUserWallet)\
            .filter(TUserWallet.pin.in_(pin_list), TUserWallet.tenant_id == commandContext['tenant_id'])\
            .all()
        data_list = []
        try:
            for user_wallet in user_wallets:
                user_wallet: TUserWallet = user_wallet
                data_list.append(
                    dict(
                        pin=user_wallet.pin,
                        balance=user_wallet.balance,
                        recharge=user_wallet.recharge,
                        present=user_wallet.present,
                        deposited_mount=user_wallet.deposited_mount,
                        deposited_stats=user_wallet.deposited_stats,
                    )
                )
            exist_pin = {w.pin for w in user_wallets}
            [data_list.append(dict(
                            pin=pin,
                            balance=0,
                            recharge=0,
                            present=0,
                            deposited_mount=0,
                            deposited_stats=0,
                        )) for pin in pin_list if pin not in exist_pin]

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("")
            logger.exception(e)
            return False

        return data_list

    def get_user_wallet(self, pin: str, args: dict):
        """
        从redis或mysql获取用户钱包信息
        """

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

    def set_user_wallet(self, pin: str, args: dict,):

        try:
            user_wallet_dict = self.get_user_wallet(pin=pin, args=args)
            dao_session.redis_session.r.delete(USER_WALLET_CACHE.format(tenant_id=user_wallet_dict['tenant_id'], pin=pin))
            if self.exists_param(args['change_recharge']):
                user_wallet_dict['balance'] += args['change_recharge']
                user_wallet_dict['recharge'] += args['change_recharge']

            if self.exists_param(args['change_present']):
                user_wallet_dict['present'] += args['change_present']
                user_wallet_dict['recharge'] += args['change_present']

            if self.exists_param(args['change_deposited_mount']):
                user_wallet_dict['deposited_mount'] += args['change_deposited_mount']

            if self.exists_param(args['deposited_stats']):
                user_wallet_dict['deposited_stats'] = args['deposited_stats']

            self.update_one(pin=pin, args=user_wallet_dict)
            return True

        except Exception as e:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(e))
            logger.exception(e)
            raise MbException("更新用户钱包失败")

    def deduction_balance(self, pin: str, args: dict,):

        deduction_amount = args['deduction_amount']
        tenant_id = args['commandContext']['tenant_id']

        try:
            user_wallet = self.get_user_wallet(pin, args)
            dao_session.redis_session.r.delete(USER_WALLET_CACHE.format(tenant_id=tenant_id, pin=pin))
            balance = user_wallet['balance'] - deduction_amount
            # 优先扣减充值余额
            if deduction_amount > user_wallet['recharge']:
                if balance < 0:
                    present = 0
                    recharge = balance
                else:
                    recharge = 0
                    present = balance
            else:
                recharge = user_wallet['recharge'] - deduction_amount
                present = user_wallet['present']

            params = dict(
                pin=pin,
                tenant_id=tenant_id,
                balance=balance,
                recharge=recharge,
                present=present,
                deposited_mount=user_wallet["deposited_mount"],
                deposited_stats=user_wallet["deposited_stats"],
            )

            self.update_one(pin=pin, args=params)
            return True
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("update user wallet is error: {}".format(ex))
            logger.exception(ex)
            raise MbException("更新余额失败")
