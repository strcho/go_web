from routes.favorable_card.view import (
    GetUserFavorableCardHandle,
    GetUserFavorableDaysHandle,
    SendUserFavorableCardHandle,
    ModifyUserFavorableCardHandle,
    BusModifyUserFavorableCardHandle,
)

# /account/user
urls = [
    ("/get_user_favorable_card", GetUserFavorableCardHandle),
    ("/get_user_favorable_days", GetUserFavorableDaysHandle),
    ("/send_user_favorable_card", SendUserFavorableCardHandle),
    ("/modify_user_favorable_card", ModifyUserFavorableCardHandle),

    # B端网关
    ("/business/modify_user_favorable_card", BusModifyUserFavorableCardHandle),
]
