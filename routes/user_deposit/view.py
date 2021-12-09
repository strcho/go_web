from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import (
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.user_deposit.serializers import (
    GetDepositDeserializer,
    UpdateDepositDeserializer,
    UserDepositSerializer,
    BusUpdateDepositDeserializer,
)
from service.user_deposit_service import UserDepositService


class EditDepositHandle(MBHandler):
    """
    编辑用户押金
    """

    @coroutine
    @use_args_query(UpdateDepositDeserializer)
    def post(self, args: dict):
        """
        更新用户押金信息
        ---
        tags: [押金]
        summary: 更新用户押金信息
        description: 更新用户押金信息

        parameters:
          - in: body
            schema:
                UpdateDepositDeserializer
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
        response = yield mb_async(UserDepositService().set_user_deposit)(*valid_data)

        self.success(response)


class GetDepositHandle(MBHandler):
    """
    用户押金
    """

    @coroutine
    @use_args_query(GetDepositDeserializer)
    def post(self, args: dict):
        """
        获取用户押金信息
        ---
        tags: [押金]
        summary: 获取用户押金信息
        description: 获取用户押金信息

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
                            UserDepositSerializer
        """

        pin = args['pin']
        valid_data = (pin, args)
        data = yield mb_async(UserDepositService().get_user_deposit)(*valid_data)
        response = UserDepositSerializer().dump(data)

        self.success(response)


class BusEditDepositHandle(MBHandler):
    """
    编辑用户押金
    """

    @coroutine
    @use_args_query(BusUpdateDepositDeserializer)
    def post(self, args: dict):
        """
        更新用户押金信息
        ---
        tags: [B端-押金]
        summary: 更新用户押金信息
        description: 更新用户押金信息

        parameters:
          - in: body
            schema:
                BusUpdateDepositDeserializer
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
        args['commandContext']["tenant_id"] = args['commandContext']['tenantId']
        pin = args['pin']
        valid_data = (pin, args)
        response = yield mb_async(UserDepositService().set_user_deposit)(*valid_data)

        self.success(response)
