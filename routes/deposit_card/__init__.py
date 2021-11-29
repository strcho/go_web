# /account/user
from routes.deposit_card.view import (
    GetUserDepositCardHandle,
    GetUserDepositDaysHandle,
    SendUserDepositCardHandle,
    ModifyUserDepositCardHandle,
    BusModifyUserDepositCardHandle,
)

urls = [
    ("/get_user_deposit_card", GetUserDepositCardHandle),
    ("/get_user_deposit_days", GetUserDepositDaysHandle),
    ("/send_user_deposit_card", SendUserDepositCardHandle),
    ("/modify_user_deposit_card", ModifyUserDepositCardHandle),

    # B端网关
    ("/business/modify_user_deposit_card", BusModifyUserDepositCardHandle),

]