from tornado.gen import coroutine

from internal.user_apis import (
    apiTest3,
    apiTest4,
)
from mbutils import mb_async
from mbutils.autodoc import (
    use_args,
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.wallet.serializers import (
    GetWalletDeserializer,
    UpdateWalletDeserializer,
    UserWalletSerializer,
)
from service.wallet_service import WalletService


class WalletHandle(MBHandler):
    """
    用户钱包
    """

    @coroutine
    @use_args_query(GetWalletDeserializer)
    def get(self, args: dict):
        pin_id = args.get('pin_id')
        commandContext = args.get('commandContext')
        print(commandContext.get('tenantId'))
        valid_data = (pin_id, commandContext)
        data = yield mb_async(WalletService().get_user_wallet)(*valid_data)
        data = UserWalletSerializer().dump(data)

        self.success(data)

    @coroutine
    @use_args_query(UpdateWalletDeserializer)
    def post(self, args: dict):
        pin_id = args.get('pin_id')
        valid_data = (pin_id, "")
        data = yield mb_async(WalletService().query_one)(valid_data)

        # 测试内部调用
        yield mb_async(print)("response: ", apiTest4({"name": "zhangsan", "timeout": 1000}))

        self.success(data)
