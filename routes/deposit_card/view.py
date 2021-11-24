from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.deposit_card.serializers import (
    UserDepositCardSerializer,
    GetDepositWithServiceIdDeserializer,
    SendDepositCardDeserializer,
    ModifyDepositCardDeserializer,
    GetDepositDeserializer,
)
from service.deposit_card_service import DepositCardService


class GetUserDepositCardHandle(MBHandler):
    """
    用户押金卡
    """

    @coroutine
    @use_args_query(GetDepositDeserializer)
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
                GetDepositDeserializer
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
    @use_args_query(GetDepositDeserializer)
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
                GetDepositDeserializer
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

        data = yield mb_async(DepositCardService().query_one_day)(args)

        response = UserDepositCardSerializer().dump(data)

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

        response = yield mb_async(DepositCardService().modify_time)(args)

        self.success(response)
