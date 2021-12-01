from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import (
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.discount.serializers import (
    GetUserDiscountDeserializer,
    UserDiscountSerializer,
    UpdateUserDiscountDeserializer,
    BusUpdateUserDiscountDeserializer,
)
from service.user_discount_service import UserDiscountService


class GetUserDiscountHandler(MBHandler):
    """
    用户折扣
    """

    @coroutine
    @use_args_query(GetUserDiscountDeserializer)
    def post(self, args):
        """
        获取用户的折扣优惠
        ---
        tags: [折扣]
        summary: 获取用户的折扣优惠
        description: 获取用户的折扣优惠

        parameters:
          - in: body
            schema:
                GetUserDiscountDeserializer
        responses:
            200:
                schema:
                    type: object
                    required:
                      - success
                      - code
                      - msg
                      - data
                    properties:
                        success:
                            type: boolean
                        code:
                            type: str
                        msg:
                            type: str
                        data:
                            UserDiscountSerializer
        """

        data = yield mb_async(UserDiscountService().query_one)(args)

        response = UserDiscountSerializer().dump(data)

        self.success(response)


class GetUserAllDiscountHandler(MBHandler):
    """
    用户全部折扣优惠
    """

    @coroutine
    @use_args_query(GetUserDiscountDeserializer)
    def post(self, args):
        """
        获取用户的全部折扣优惠
        ---
        tags: [折扣]
        summary: 获取用户的全部折扣优惠
        description: 获取用户的全部折扣优惠

        parameters:
          - in: body
            schema:
                GetUserDiscountDeserializer
        responses:
            200:
                schema:
                    type: object
                    required:
                      - success
                      - code
                      - msg
                      - data
                    properties:
                        success:
                            type: boolean
                        code:
                            type: str
                        msg:
                            type: str
                        data:
                            UserDiscountSerializer
        """

        data = yield mb_async(UserDiscountService().query_all)(args)

        response = UserDiscountSerializer(many=True).dump(data)

        self.success(response)


class UpdateUserDiscountHandler(MBHandler):
    """
    更新用户的折扣优惠
    """

    @coroutine
    @use_args_query(UpdateUserDiscountDeserializer)
    def post(self, args):
        """
        更新用户的折扣优惠
        ---
        tags: [折扣]
        summary: 更新用户的折扣优惠
        description: 更新用户的折扣优惠

        parameters:
          - in: body
            schema:
                UpdateUserDiscountDeserializer
        responses:
            200:
                schema:
                    type: object
                    required:
                      - success
                      - code
                      - msg
                      - data
                    properties:
                        success:
                            type: boolean
                        code:
                            type: str
                        msg:
                            type: str
                        data:
                            type: boolean
        """

        response = yield mb_async(UserDiscountService().update_user_discount)(args)

        self.success(response)


class BusUpdateUserDiscountHandler(MBHandler):
    """
    更新用户的折扣优惠
    """

    @coroutine
    @use_args_query(BusUpdateUserDiscountDeserializer)
    def post(self, args):
        """
        更新用户的折扣优惠
        ---
        tags: [B端-折扣]
        summary: 更新用户的折扣优惠
        description: 更新用户的折扣优惠

        parameters:
          - in: body
            schema:
                BusUpdateUserDiscountDeserializer
        responses:
            200:
                schema:
                    type: object
                    required:
                      - success
                      - code
                      - msg
                      - data
                    properties:
                        success:
                            type: boolean
                        code:
                            type: str
                        msg:
                            type: str
                        data:
                            type: boolean
        """

        args['commandContext'] = self.get_context()
        response = yield mb_async(UserDiscountService().update_user_discount)(args)

        self.success(response)
