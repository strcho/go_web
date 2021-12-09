from routes.wallet.view import (
        EditWalletHandle,
        GetWalletHandle,
        GetWalletListHandle,
        BusGetWalletHandle,
)

# /account/user
urls = [
        (r'/edit_wallet', EditWalletHandle),
        (r'/deduction_balance', DeductionBalanceHandle),
        (r'/get_wallet_info', GetWalletHandle),
        (r'/get_wallet_info_list', GetWalletListHandle),

        (r'/business/get_wallet_info', BusGetWalletHandle),

]
