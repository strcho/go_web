from routes.wallet.view import (
        EditWalletHandle,
        GetWalletHandle,
        GetWalletListHandle,
        BusGetWalletHandle,
        DeductionBalanceHandle,
        BusSetWalletHandle,
        ClientWalletHandle,
)

# /account/wallet
urls = [
        (r'/internal/edit_wallet', EditWalletHandle),
        (r'/internal/deduction_balance', DeductionBalanceHandle),
        (r'/internal/get_wallet_info', GetWalletHandle),
        (r'/internal/get_wallet_info_list', GetWalletListHandle),

        (r'/business/get_wallet_info', BusGetWalletHandle),
        (r'/business/set_wallet_info', BusSetWalletHandle),

        (r'/client/get_wallet_info', ClientWalletHandle),
]
