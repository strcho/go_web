from routes.user_deposit.view import (
        EditDepositHandle,
        GetDepositHandle,
        BusEditDepositHandle,
        DepositToKafkaHandle,
)

# /ebike_account/user
urls = [
        (r'/internal/edit_deposit', EditDepositHandle),
        (r'/internal/get_deposit_info', GetDepositHandle),
        (r'/internal/deposit_to_kafka', DepositToKafkaHandle),


        # B端
        (r'/business/edit_deposit', BusEditDepositHandle),

]
