# /account/user
from routes.deposit_card.view import (
    GetUserDepositCardHandle,
    GetUserDepositDaysHandle,
    SendUserDepositCardHandle,
    ModifyUserDepositCardHandle,
    BusModifyUserDepositCardHandle, DepositCardToKafkaHandle,
)

urls = [
    ("/internal/get_user_deposit_card", GetUserDepositCardHandle),
    ("/internal/get_user_deposit_days", GetUserDepositDaysHandle),
    ("/internal/send_user_deposit_card", SendUserDepositCardHandle),
    ("/internal/modify_user_deposit_card", ModifyUserDepositCardHandle),
    ("/internal/deposit_card_to_kafka", DepositCardToKafkaHandle),


    # B端网关
    ("/business/modify_user_deposit_card", BusModifyUserDepositCardHandle),

]