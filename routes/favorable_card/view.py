from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.favorable_card.serializers import (
    GetFavorableDeserializer,
    UserFavorableCardSerializer,
    GetFavorableWithServiceIdDeserializer,
    SendFavorableCardDeserializer,
    ModifyFavorableCardDeserializer,
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

        if args.get("service_id"):
            data = yield mb_async(FavorableCardUserService().query_one)(args)
        else:
            data = yield mb_async(FavorableCardUserService().query_all)(args)

        response = UserFavorableCardSerializer().dump(data)

        self.success(response)


class GetUserFavorableDaysHandle(MBHandler):
    """
    优惠卡天数
    """

    @coroutine
    @use_args_query(GetFavorableWithServiceIdDeserializer)
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
                            type: boolean
        """

        data = yield mb_async(FavorableCardUserService().query_one_day)(args)

        response = UserFavorableCardSerializer().dump(data)

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
