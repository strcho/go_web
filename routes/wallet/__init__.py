from routes.wallet.view import (
        WalletHandle,
        GetWalletHandle,
)

# /account/user
urls = [
        (r'/wallet', WalletHandle),  # 用户钱包
        (r'/get_wallet_info', GetWalletHandle),  # 用户钱包

]
