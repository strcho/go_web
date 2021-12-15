from routes.favorable_card.view import (
    GetUserFavorableCardHandle,
    GetUserFavorableDaysHandle,
    SendUserFavorableCardHandle,
    ModifyUserFavorableCardHandle,
    BusModifyUserFavorableCardHandle,
)

# /account/user
urls = [
    ("/internal/get_user_favorable_card", GetUserFavorableCardHandle),
    ("/internal/get_user_favorable_days", GetUserFavorableDaysHandle),
    ("/internal/send_user_favorable_card", SendUserFavorableCardHandle),
    ("/internal/modify_user_favorable_card", ModifyUserFavorableCardHandle),

    # B端网关
    ("/business/modify_user_favorable_card", BusModifyUserFavorableCardHandle),
]
