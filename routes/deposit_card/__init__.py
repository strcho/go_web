from routes.deposit_card.view import (
    GetUserDepositCardHandle,
    GetUserDepositDaysHandle,
    SendUserDepositCardHandle,
    ModifyUserDepositCardHandle,
    BusModifyUserDepositCardHandle,
    ClientGetUserDepositCardHandle,
    RefundDepositCardHandle,
)

# /ebike_account/deposit_card
urls = [
    ("/internal/get_user_deposit_card", GetUserDepositCardHandle),
    ("/internal/get_user_deposit_days", GetUserDepositDaysHandle),
    ("/internal/send_user_deposit_card", SendUserDepositCardHandle),
    ("/internal/modify_user_deposit_card", ModifyUserDepositCardHandle),
    ("/internal/refund_deposit_card", RefundDepositCardHandle),


    # B端网关
    ("/business/modify_user_deposit_card", BusModifyUserDepositCardHandle),

    # C端网关
    ("/client/get_user_deposit_card", ClientGetUserDepositCardHandle),
]