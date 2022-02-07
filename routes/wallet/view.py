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
    BusGetWalletDeserializer,
    DeductionBalanceDeserializer,
    WalletToKafkaSerializer,
    BusUpdateWalletDeserializer,
    CliGetWalletDeserializer,
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
        pin = args['pin']
        valid_data = (pin, args)
        response = yield mb_async(WalletService().set_user_wallet)(*valid_data)

        self.success(response)


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

        pin = args['pin']
        valid_data = (pin, args)
        data = yield mb_async(WalletService().get_user_wallet)(*valid_data)
        response = UserWalletSerializer().dump(data)

        self.success(response)


class BusGetWalletHandle(MBHandler):
    """
    用户钱包
    """

    @coroutine
    @use_args_query(BusGetWalletDeserializer)
    def post(self, args: dict):
        """
        获取用户钱包信息
        ---
        tags: [B端-钱包]
        summary: 获取用户钱包信息
        description: 获取用户钱包信息

        parameters:
          - in: body
            schema:
                BusGetWalletDeserializer
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

        args["commandContext"] = self.get_context()
        pin = args['pin']
        valid_data = (pin, args)
        data = yield mb_async(WalletService().get_user_wallet)(*valid_data)
        response = UserWalletSerializer().dump(data)

        self.success(response)


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

        pin_list = args.get('pin_list')
        valid_data = (pin_list, args["commandContext"],)
        data = yield mb_async(WalletService().query_list)(valid_data)
        response = UserWalletSerializer(many=True).dump(data)

        self.success(response)


class DeductionBalanceHandle(MBHandler):
    """
    余额
    """
    @coroutine
    @use_args_query(DeductionBalanceDeserializer)
    def post(self, args: dict):
        """
        扣减用户余额
        ---
        tags: [钱包]
        summary: 扣减用户余额
        description: 扣减用户余额

        parameters:
          - in: body
            schema:
                DeductionBalanceDeserializer
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

        valid_data = args['pin'], args
        response = yield mb_async(WalletService().deduction_balance)(*valid_data)
        mb_async(WalletService().wallet_to_kafka)(args["commandContext"], args)
        self.success(response)


class BusSetWalletHandle(MBHandler):
    """
    B端编辑用户钱包
    """

    @coroutine
    @use_args_query(BusUpdateWalletDeserializer)
    def post(self, args: dict):
        """
        B端编辑用户钱包
        ---
        tags: [B端-钱包]
        summary: 编辑用户钱包信息
        description: 编辑用户钱包信息

        parameters:
          - in: body
            schema:
                BusUpdateWalletDeserializer
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
        valid_data = (args['pin'], args)
        response = yield mb_async(WalletService().set_user_wallet)(*valid_data)

        self.success(response)


class ClientWalletHandle(MBHandler):
    """
    用户端获取钱包信息
    """

    @coroutine
    @use_args_query(CliGetWalletDeserializer)
    def post(self, args):
        """
        获取用户钱包信息
        ---
        tags: [C端-钱包]
        summary: 获取用户钱包信息
        description: 获取用户钱包信息

        parameters:
          - in: body
            schema:
                CliGetWalletDeserializer
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
                            CliUserWalletSerializer
        """

        args["commandContext"] = self.get_context()

        pin = args['pin']
        valid_data = (pin, args)
        wallet_data = yield mb_async(WalletService().get_user_wallet)(*valid_data)
        wallet_data["can_refund_amount"] = wallet_data.get("recharge")  # todo

        self.success(wallet_data)
