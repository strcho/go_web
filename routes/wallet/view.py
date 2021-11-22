from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import (
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.wallet.serializers import (
    GetWalletDeserializer,
    UpdateWalletDeserializer,
    UserWalletSerializer,
    GetWalletListDeserializer,
)
from service.wallet_service import WalletService


class EditWalletHandle(MBHandler):
    """
    编辑用户钱包
    """

    @coroutine
    @use_args_query(UpdateWalletDeserializer)
    def post(self, args: dict):
        """
        更新用户钱包信息
        ---
        tags: [钱包]
        summary: 更新用户钱包信息
        description: 更新用户钱包信息

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
                            type: boolean
      """
        pin = args.get('pin')
        valid_data = (pin, args)
        data = yield mb_async(WalletService().set_user_wallet)(*valid_data)

        # 测试内部调用
        # yield mb_async(print)("response: ", apiTest4({"name": "zhangsan", "timeout": 1000}))

        self.success(data)


class GetWalletHandle(MBHandler):
    """
    用户钱包
    """

    @coroutine
    @use_args_query(GetWalletDeserializer)
    def post(self, args: dict):
        """
        获取用户钱包信息
        ---
        tags: [钱包]
        summary: 获取用户钱包信息
        description: 获取用户钱包信息

        parameters:
          - in: body
            schema:
                GetWalletDeserializer
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
                            UserWalletSerializer
        """

        pin = args.get('pin')
        valid_data = (pin, args)
        data = yield mb_async(WalletService().get_user_wallet)(*valid_data)
        data = UserWalletSerializer().dump(data)

        self.success(data)


class GetWalletListHandle(MBHandler):
    """
    用户钱包
    """

    @coroutine
    @use_args_query(GetWalletListDeserializer)
    def post(self, args: dict):
        """
        获取用户钱包信息列表
        ---
        tags: [钱包]
        summary: 获取用户钱包信息列表
        description: 获取用户钱包信息列表

        parameters:
          - in: body
            schema:
                GetWalletListDeserializer
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
                            UserWalletSerializer
        """

        pins = args.get('pin')
        valid_data = (pins, args["commandContext"],)
        data = yield mb_async(WalletService().query_list)(valid_data)
        data = UserWalletSerializer(many=True).dump(data)

        self.success(data)