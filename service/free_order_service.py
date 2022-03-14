import datetime
import json

from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import TFreeOrderUser
from service import MBService
from utils.constant.user import UserFreeOrderType


class UserFreeOrderService(MBService):
    """
    用户免单
    """

    def query_one(self, args: dict):
        """
        用户免单信息
        """

        user_free_order = None
        try:
            tenant_id = args['commandContext']['tenantId']
            user_free_order = dao_session.session.tenant_db(
            ).query(TFreeOrderUser).filter(
                TFreeOrderUser.tenant_id == tenant_id,
                TFreeOrderUser.pin == args['pin'],
                TFreeOrderUser.free_num > 0,
            ).order_by(TFreeOrderUser.id.asc()).first()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user free order is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)

        return user_free_order

    def query_all(self, args: dict):
        """
        获取用户的全部免单信息
        """

        user_free_order_list = []
        try:
            tenant_id = args['commandContext']['tenantId']
            user_free_order_list = dao_session.session.tenant_db(
            ).query(TFreeOrderUser).filter(
                TFreeOrderUser.tenant_id == tenant_id,
                TFreeOrderUser.pin == args['pin'],
                TFreeOrderUser.free_num > 0,
            ).order_by(TFreeOrderUser.id.asc()).all()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user all free order is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)

        return user_free_order_list

    def client_query_all(self, args: dict):
        """
        获取用户的全部免单信息-c端
        """

        try:
            tenant_id = args['commandContext']['tenantId']
            user_free_order_list = dao_session.session.tenant_db(
            ).query(TFreeOrderUser).filter(
                TFreeOrderUser.tenant_id == tenant_id,
                TFreeOrderUser.pin == args['pin'],
            ).order_by(TFreeOrderUser.id.asc()).all()

            res_dict = {"used": [], "expired": []}
            for i in user_free_order_list:
                if i.free_num > 0:
                    res_dict.get("used").append(i)
                else:
                    res_dict.get("expired").append(i)

        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user all free order is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)

        return res_dict

    def insert_one(self, args: dict):
        """
        插入一条用户免单信息
        """
        try:
            data = self.get_model_common_field(args['commandContext'])
            data.update({
                "pin":  args['pin'],
                "free_second": args['free_second'],
                "free_num": args['free_num'],
            })
            user_free_order = TFreeOrderUser(**data)
            dao_session.session.tenant_db().add(user_free_order)
            dao_session.session.tenant_db().commit()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user free order is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)
            raise MbException("添加用户免单失败")

        return True

    def update_one(self, args: dict, update_pin: str = None):
        """
        更新一条(扣减)
        """

        params = {
            "free_num": TFreeOrderUser.free_num - 1,
            "updated_at": datetime.datetime.now(),
            "updated_pin": update_pin or args["pin"]
        }

        try:
            tenant_id = args['commandContext']['tenantId']
            d = dao_session.session.tenant_db(
            ).query(TFreeOrderUser).filter(
                TFreeOrderUser.tenant_id == tenant_id,
                TFreeOrderUser.pin == args['pin'],
                TFreeOrderUser.free_num > 0,
            ).order_by(TFreeOrderUser.id.asc()).first()
            dao_session.session.tenant_db(
            ).query(TFreeOrderUser).filter(
                TFreeOrderUser.tenant_id == tenant_id,
                TFreeOrderUser.id == d.id
            ).update(params)

            dao_session.session.tenant_db().commit()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user free order is error: {}".format(ex), extra=args['commandContext'])
            logger.exception(ex)
            raise MbException('更新用户免单优惠失败')

        return True

    def update_user_free_order(self, args: dict):
        """
        更新用户免单信息
        """

        # 添加免单次数
        if args['tp'] == UserFreeOrderType.ADD_FREE_ORDER.value:
            self.insert_one(args)
        # 扣减免单次数
        else:
            user_free_order: TFreeOrderUser = self.query_one(args)
            if not user_free_order:
                raise MbException("未找到免单优惠")
            try:
                self.update_one(args,)
            except Exception as ex:

                logger.error("update user free order is error: {}".format(ex), extra=args['commandContext'])
                logger.exception(ex)
                dao_session.session.tenant_db().rollback()
                raise MbException("扣减用户免单优惠失败")

        return True
