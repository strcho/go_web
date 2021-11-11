from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.user_account.serializers import UserAccountDeserializer
from service.wallet_service import WalletService


class UserAccount(MBHandler):
    """
    用户资产
    """

    @coroutine
    @use_args_query(UserAccountDeserializer)
    def get(self, args):
        pin_id = args.get('pin_id')

        user_wallet = yield mb_async(WalletService().query_one)(pin_id)
        user_riding_card = yield mb_async(WalletService().query_one)(pin_id)
        user_deposit_card = yield mb_async(WalletService().query_one)(pin_id)
        user_free_order = yield mb_async(WalletService().query_one)(pin_id)
        user_discount = yield mb_async(WalletService().query_one)(pin_id)

        self.success()
