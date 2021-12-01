from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.deposit_card.serializers import (
    UserDepositCardSerializer,
    SendDepositCardDeserializer,
    ModifyDepositCardDeserializer,
    GetDepositCardDeserializer,
    UserDepositCardDaysSerializer,
    BusModifyDepositCardDeserializer,
)
from service.deposit_card_service import DepositCardService


class GetUserDepositCardHandle(MBHandler):
    """
    用户押金卡
    """

    @coroutine
    @use_args_query(GetDepositCardDeserializer)
    def post(self, args):
        """
        获取用户押金卡
        ---
        tags: [押金卡]
        summary: 获取用户押金卡
        description: 获取用户押金卡

        parameters:
          - in: body
            schema:
                GetDepositCardDeserializer
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
                            UserDepositCardSerializer
        """

        data = yield mb_async(DepositCardService().query_one)(args)

        response = UserDepositCardSerializer().dump(data)

        self.success(response)


class GetUserDepositDaysHandle(MBHandler):
    """
    押金卡天数
    """

    @coroutine
    @use_args_query(GetDepositCardDeserializer)
    def post(self, args):
        """
        获取用户押金卡剩余天数
        ---
        tags: [押金卡]
        summary: 获取用户押金卡剩余天数
        description: 获取用户押金卡剩余天数

        parameters:
          - in: body
            schema:
                GetDepositCardDeserializer
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
                            UserDepositCardDaysSerializer
        """

        data = yield mb_async(DepositCardService().query_one_day)(args)

        response = UserDepositCardDaysSerializer().dump(data)

        self.success(response)


class SendUserDepositCardHandle(MBHandler):
    """
    为用户添加押金卡
    """

    @coroutine
    @use_args_query(SendDepositCardDeserializer)
    def post(self, args):
        """
        为用户添加押金卡
        ---
        tags: [押金卡]
        summary: 为用户添加押金卡
        description: 为用户添加押金卡

        parameters:
          - in: body
            schema:
                SendDepositCardDeserializer
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

        response = yield mb_async(DepositCardService().send_deposit_card)(args)

        self.success(response)


class ModifyUserDepositCardHandle(MBHandler):
    """
    编辑用户押金卡
    """

    @coroutine
    @use_args_query(ModifyDepositCardDeserializer)
    def post(self, args):
        """
        修改用户押金卡
        ---
        tags: [押金卡]
        summary: 修改用户押金卡
        description: 修改用户押金卡

        parameters:
          - in: body
            schema:
                ModifyDepositCardDeserializer
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

        response = yield mb_async(DepositCardService().modify_deposit_card_time)(args)

        self.success(response)


class BusModifyUserDepositCardHandle(MBHandler):
    """
    编辑用户押金卡
    """

    @coroutine
    @use_args_query(BusModifyDepositCardDeserializer)
    def post(self, args):
        """
        修改用户押金卡
        ---
        tags: [B端-押金卡]
        summary: 修改用户押金卡
        description: 修改用户押金卡

        parameters:
          - in: body
            schema:
                BusModifyDepositCardDeserializer
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
        response = yield mb_async(DepositCardService().modify_deposit_card_time)(args)

        self.success(response)

