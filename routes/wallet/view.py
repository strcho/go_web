from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import (
    use_args,
    use_args_query,
)
from mbutils.mb_handler import MBHandler
from routes.wallet.serializers import GetWalletDeserializer
from service.favorable_card import FavorableCardService
from service.wallet_service import WalletService


class WalletHandle(MBHandler):
    """
    用户钱包
    """

    @coroutine
    @use_args_query(GetWalletDeserializer)
    def get(self, args: dict):
        pin_id = args.get('pin_id')
        valid_data = (pin_id, "")
        data = yield mb_async(WalletService().query_one)(valid_data)

        self.success(data)
