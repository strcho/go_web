# /account/user
from routes.deposit_card.view import (
    GetUserDepositCardHandle,
    GetUserDepositDaysHandle,
    SendUserDepositCardHandle,
    ModifyUserDepositCardHandle,
)

urls = [
    ("/get_user_deposit_card", GetUserDepositCardHandle),
    ("/get_user_deposit_days", GetUserDepositDaysHandle),
    ("/send_user_deposit_card", SendUserDepositCardHandle),
    ("/modify_user_deposit_card", ModifyUserDepositCardHandle)
]