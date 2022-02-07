from datetime import datetime

from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.favorable_card.serializers import (
    GetFavorableDeserializer,
    UserFavorableCardSerializer,
    SendFavorableCardDeserializer,
    ModifyFavorableCardDeserializer,
    UserFavorableCardDaysSerializer,
    BusModifyFavorableCardDeserializer, FavorableCardToKafkaSerializer,
)
from service.favorable_card_service import FavorableCardUserService


class GetUserFavorableCardHandle(MBHandler):
    """
    用户优惠卡
    """

    @coroutine
    @use_args_query(GetFavorableDeserializer)
    def post(self, args):
        """
        获取用户优惠卡信息
        ---
        tags: [优惠卡]
        summary: 获取用户优惠卡信息
        description: 获取用户优惠卡信息

        parameters:
          - in: body
            schema:
                GetFavorableDeserializer
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
                            UserFavorableCardSerializer
        """

        data = yield mb_async(FavorableCardUserService().query_one)(args)

        data = None if data.end_time <= datetime.now() else data

        response = UserFavorableCardSerializer().dump(data)

        self.success(response)


class GetUserFavorableDaysHandle(MBHandler):
    """
    优惠卡天数
    """

    @coroutine
    @use_args_query(GetFavorableDeserializer)
    def post(self, args):
        """
        获取用户优惠卡剩余天数
        ---
        tags: [优惠卡]
        summary: 获取用户优惠卡剩余天数
        description: 获取用户优惠卡剩余天数

        parameters:
          - in: body
            schema:
                GetFavorableDeserializer
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
                            UserFavorableCardDaysSerializer
        """

        data = yield mb_async(FavorableCardUserService().query_one_day)(args)

        response = UserFavorableCardDaysSerializer().dump(data)

        self.success(response)


class SendUserFavorableCardHandle(MBHandler):
    """
    为用户添加优惠卡
    """

    @coroutine
    @use_args_query(SendFavorableCardDeserializer)
    def post(self, args):
        """
        为用户添加优惠卡
        ---
        tags: [优惠卡]
        summary: 为用户添加优惠卡
        description: 为用户添加优惠卡

        parameters:
          - in: body
            schema:
                SendFavorableCardDeserializer
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

        response = yield mb_async(FavorableCardUserService().send_user_favorable_card)(args)

        self.success(response)


class ModifyUserFavorableCardHandle(MBHandler):
    """
    编辑用户优惠卡
    """

    @coroutine
    @use_args_query(ModifyFavorableCardDeserializer)
    def post(self, args):
        """
        修改用户优惠卡
        ---
        tags: [优惠卡]
        summary: 修改用户优惠卡
        description: 修改用户优惠卡

        parameters:
          - in: body
            schema:
                ModifyFavorableCardDeserializer
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

        response = yield mb_async(FavorableCardUserService().modify_time)(args)

        self.success(response)


class BusModifyUserFavorableCardHandle(MBHandler):
    """
    编辑用户优惠卡
    """

    @coroutine
    @use_args_query(BusModifyFavorableCardDeserializer)
    def post(self, args):
        """
        修改用户优惠卡
        ---
        tags: [B端-优惠卡]
        summary: 修改用户优惠卡
        description: 修改用户优惠卡

        parameters:
          - in: body
            schema:
                BusModifyFavorableCardDeserializer
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
        # args['commandContext']["tenant_id"] = args['commandContext']['tenantId']
        response = yield mb_async(FavorableCardUserService().modify_time)(args)

        self.success(response)
