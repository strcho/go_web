from routes.favorable_card.view import (
    GetUserFavorableCardHandle,
    GetUserFavorableDaysHandle,
    SendUserFavorableCardHandle,
    ModifyUserFavorableCardHandle,
    BusModifyUserFavorableCardHandle,
    ClientGetUserFavorableCardHandle,
    RefundFavorableCardHandle,
)

# /ebike_account/favorable_card
urls = [
    ("/internal/get_user_favorable_card", GetUserFavorableCardHandle),
    ("/internal/get_user_favorable_days", GetUserFavorableDaysHandle),
    ("/internal/send_user_favorable_card", SendUserFavorableCardHandle),
    ("/internal/modify_user_favorable_card", ModifyUserFavorableCardHandle),
    ("/internal/refund_favorable_card", RefundFavorableCardHandle),

    # B端网关
    ("/business/modify_user_favorable_card", BusModifyUserFavorableCardHandle),

    # C端网关
    ("/client/get_user_favorable_card", ClientGetUserFavorableCardHandle),
]
