from mbutils.autodoc import use_args
from mbutils.mb_handler import MBHandler
from routes.wallet.serializers import GetWalletDeserializer


class WalletHandle(MBHandler):
    """
    用户钱包
    """

    @use_args(GetWalletDeserializer)
    def get(self):
        pass