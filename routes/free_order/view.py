from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import (
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.free_order.serializers import (
    GetUserFreeOrderDeserializer,
    UserFreeOrderSerializer,
    UpdateUserFreeOrderDeserializer,
    BusUpdateUserFreeOrderDeserializer,
    ClientGetUserFreeOrderDeserializer,
    ClientUserFreeOrderSerializer,
)
from service.free_order_service import UserFreeOrderService


class GetUserFreeOrderHandler(MBHandler):
    """
    用户免单
    """

    @coroutine
    @use_args_query(GetUserFreeOrderDeserializer)
    def post(self, args):
        """
        获取用户的免单优惠
        ---
        tags: [免单]
        summary: 获取用户的免单优惠
        description: 获取用户的免单优惠

        parameters:
          - in: body
            schema:
                GetUserFreeOrderDeserializer
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
                            UserFreeOrderSerializer
        """

        data = yield mb_async(UserFreeOrderService().query_one)(args)

        response = UserFreeOrderSerializer().dump(data)

        self.success(response)


class GetUserAllFreeOrderHandler(MBHandler):
    """
    用户全部免单优惠
    """

    @coroutine
    @use_args_query(GetUserFreeOrderDeserializer)
    def post(self, args):
        """
        获取用户的全部免单优惠
        ---
        tags: [免单]
        summary: 获取用户的全部免单优惠
        description: 获取用户的全部免单优惠

        parameters:
          - in: body
            schema:
                GetUserFreeOrderDeserializer
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
                            UserFreeOrderSerializer
        """

        data = yield mb_async(UserFreeOrderService().query_all)(args)

        response = UserFreeOrderSerializer(many=True).dump(data)

        self.success(response)


class ClientGetUserAllFreeOrderHandler(MBHandler):
    """
    C端用户全部免单优惠
    """

    @coroutine
    @use_args_query(ClientGetUserFreeOrderDeserializer)
    def post(self, args):
        """
        获取用户的全部免单优惠
        ---
        tags: [C端-免单]
        summary: 获取用户的全部免单优惠
        description: 获取用户的全部免单优惠

        parameters:
          - in: body
            schema:
                ClientGetUserFreeOrderDeserializer
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
                            UserFreeOrderSerializer
        """

        args['commandContext'] = self.get_context()
        data = yield mb_async(UserFreeOrderService().query_all)(args)

        response = ClientUserFreeOrderSerializer(many=True).dump(data)

        self.success(response)


class UpdateUserFreeOrderHandler(MBHandler):
    """
    更新用户的免单优惠
    """

    @coroutine
    @use_args_query(UpdateUserFreeOrderDeserializer)
    def post(self, args):
        """
        更新用户的免单优惠
        ---
        tags: [免单]
        summary: 更新用户的免单优惠
        description: 更新用户的免单优惠

        parameters:
          - in: body
            schema:
                UpdateUserFreeOrderDeserializer
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

        response = yield mb_async(UserFreeOrderService().update_user_free_order)(args)

        self.success(response)


class BusUpdateUserFreeOrderHandler(MBHandler):
    """
    更新用户的免单优惠
    """

    @coroutine
    @use_args_query(BusUpdateUserFreeOrderDeserializer)
    def post(self, args):
        """
        更新用户的免单优惠
        ---
        tags: [B端-免单]
        summary: 更新用户的免单优惠
        description: 更新用户的免单优惠

        parameters:
          - in: body
            schema:
                BusUpdateUserFreeOrderDeserializer
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
        response = yield mb_async(UserFreeOrderService().update_user_free_order)(args)

        self.success(response)
