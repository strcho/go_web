from routes.wallet.view import (
        EditWalletHandle,
        GetWalletHandle,
        GetWalletListHandle,
)

# /account/user
urls = [
        (r'/edit_wallet', EditWalletHandle),
        (r'/get_wallet_info', GetWalletHandle),
        (r'/get_wallet_info_list', GetWalletListHandle),

]
