from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.favorable_card.serializers import (
    GetFavorableDeserializer,
    UserFavorableCardSerializer,
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
        tags: [钱包]
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
                            # todo
        """

        if args.get("service_id"):
            data = yield mb_async(FavorableCardUserService().query_one)(args)
        else:
            data = yield mb_async(FavorableCardUserService().query_all)(args)

        response = UserFavorableCardSerializer().dump(data)

        self.success(response)
