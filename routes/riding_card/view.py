from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.riding_card.serializers import (
    GetRidingCardDeserializer,
    RidingCardSerializer,
    EditRidingCardDeserializer,
    SendRidingCardDeserializer,
    CurrentDuringTimeDeserializer,
    AddCountHandlerDeserializer,
    CurrentDuringTimeSerializer,
)
from service.riding_card_service import RidingCardService


class GetRidingCardHandle(MBHandler):
    """
    骑行卡
    """

    @coroutine
    @use_args_query(GetRidingCardDeserializer)
    def post(self, args: dict):
        """
        获取用户骑行卡
        ---
        tags: [骑行卡]
        summary: 获取用户骑行卡
        description: 获取用户骑行卡

        parameters:
          - in: body
            schema:
                UpdateWalletDeserializer
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
                            RidingCardSerializer
        """

        pin = args['pin']
        valid_data = (pin, args)
        data = yield mb_async(RidingCardService().user_card_info)(valid_data)
        data = RidingCardSerializer().dump(data)

        self.success(data)


class EditRidingCardHandle(MBHandler):
    """
    编辑用户骑行卡
    """

    @coroutine
    @use_args_query(EditRidingCardDeserializer)
    def post(self, args: dict):
        """
        平台修改用户骑行卡信息
        ---
        tags: [骑行卡]
        summary: 平台修改用户骑行卡信息
        description: 平台修改用户骑行卡信息

        parameters:
          - in: body
            schema:
                EditRidingCardDeserializer
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
        data = yield mb_async(RidingCardService().modify_time(args))

        self.success(data)


class SendRidingCardHandle(MBHandler):
    """
    发放骑行卡
    """

    @coroutine
    @use_args_query(SendRidingCardDeserializer)
    def post(self, args):
        """
        给用户发送骑行卡
                ---
        tags: [骑行卡]
        summary: 给用户发送骑行卡
        description: 给用户发送骑行卡

        parameters:
          - in: body
            schema:
                SendRidingCardDeserializer
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

        data = yield mb_async(RidingCardService.send_riding_card)(args)

        self.success(data=data)


class CurrentDuringTimeHandler(MBHandler):
    """
    查询当前骑行卡的持续时间
    """

    @coroutine
    @use_args_query(CurrentDuringTimeDeserializer)
    def post(self, args: dict):
        """
        查询当前骑行卡的持续时间
        ---
        tags: [骑行卡]
        summary: 查询当前骑行卡的持续时间
        description: 查询当前骑行卡的持续时间

        parameters:
          - in: body
            schema:
                CurrentDuringTimeDeserializer
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
                            CurrentDuringTimeSerializer
        """
        data = yield mb_async(RidingCardService.current_during_time)(args)
        data = CurrentDuringTimeSerializer().dump(data)

        self.success(data)


class AddCountHandler(MBHandler):
    """
    内部用, 骑行卡使用次数+1
    """

    @coroutine
    @use_args_query(AddCountHandlerDeserializer)
    def post(self, args: dict):
        """
        骑行卡使用次数加一
        ---
        tags: [骑行卡]
        summary: 骑行卡使用次数加一
        description: 骑行卡使用次数加一

        parameters:
          - in: body
            schema:
                AddCountHandlerDeserializer
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
                            type: str
        """
        data = yield mb_async(RidingCardService.add_count)(args)

        self.success(data)
