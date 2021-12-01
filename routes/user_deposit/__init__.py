from routes.user_deposit.view import (
        EditDepositHandle,
        GetDepositHandle,
)

# /account/user
urls = [
        (r'/edit_deposit', EditDepositHandle),
        (r'/get_deposit_info', GetDepositHandle),

        # B端
        (r'/business/edit_deposit', BusEditDepositHandle),

]
