from routes.wallet.view import (
        EditWalletHandle,
        GetWalletHandle,
        GetWalletListHandle,
)

# /account/user
urls = [
        (r'/edit_wallet', EditWalletHandle),  # 用户钱包
        (r'/get_wallet_info', GetWalletHandle),  # 用户钱包
        (r'/get_wallet_info_list', GetWalletListHandle),  # 用户钱包

]
