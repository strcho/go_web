from mbutils import (
    dao_session,
    logger,
    MbException,
)
from model.all_model import (
    TDiscountsUser,
)
from service import MBService
from utils.constant.user import (
    DiscountsUserType,
)


class UserDiscountService(MBService):
    """
    用户折扣
    """

    def query_one(self, args: dict):
        """
        用户折扣信息
        """

        user_discount = None
        try:
            tenant_id = args['commandContext']['tenant_id']
            user_discount: TDiscountsUser = dao_session.session.tenant_db(
            ).query(TDiscountsUser).filter(
                TDiscountsUser.tenant_id == tenant_id,
                TDiscountsUser.pin == args['pin'],
                TDiscountsUser.iz_del == 0,
            ).order_by(TDiscountsUser.created_at.asc()).first()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user discount is error: {}".format(ex))
            logger.exception(ex)

        return user_discount

    def query_all(self, args: dict):
        """
        获取用户的全部可用折扣信息
        """

        user_discount_list = []
        try:
            tenant_id = args['commandContext']['tenant_id']
            user_discount_list = dao_session.session.tenant_db(
            ).query(TDiscountsUser).filter(
                TDiscountsUser.tenant_id == tenant_id,
                TDiscountsUser.pin == args['pin'],
                TDiscountsUser.iz_del == 0,
            ).order_by(TDiscountsUser.created_at.asc()).all()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("query user all free order is error: {}".format(ex))
            logger.exception(ex)

        return user_discount_list

    def insert_one(self, args: dict):
        """
        插入一条用户折扣信息
        """
        try:
            data = self.get_model_common_field(args['commandContext'])
            data.update({
                "pin":  args['pin'],
                "discount_rate": args['discount_rate']
            })
            user_discount = TDiscountsUser(**data)
            dao_session.session.tenant_db().add(user_discount)
            dao_session.session.tenant_db().commit()
        except Exception as ex:
            dao_session.session.tenant_db().rollback()
            logger.error("insert user free order is error: {}".format(ex))
            logger.exception(ex)
            raise MbException("添加用户折扣失败")

        return True

    def update_user_discount(self, args: dict):
        """
        更新用户折扣信息
        """

        # 添加折扣次数
        if args['tp'] == DiscountsUserType.ADD_DISCOUNT.value:
            self.insert_one(args)
        # 消耗折扣优惠
        else:
            user_discount: TDiscountsUser = self.query_one(args)
            if not user_discount:
                raise MbException("未找到可用折扣优惠")
            try:
                user_discount.iz_del = 1
                dao_session.session.tenant_db().commit()
            except Exception as ex:
                dao_session.session.tenant_db().rollback()
                logger.error("update user discount is error: {}".format(ex))
                logger.exception(ex)
                raise MbException("扣减用户免单次数失败")

        return True
