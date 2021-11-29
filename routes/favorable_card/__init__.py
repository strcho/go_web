from routes.favorable_card.view import (
    GetUserFavorableCardHandle,
    GetUserFavorableDaysHandle,
    SendUserFavorableCardHandle,
    ModifyUserFavorableCardHandle,
)

# /account/user
urls = [
    ("/get_user_favorable_card", GetUserFavorableCardHandle),
    ("/get_user_favorable_days", GetUserFavorableDaysHandle),
    ("/send_user_favorable_card", SendUserFavorableCardHandle),
    ("/modify_user_favorable_card", ModifyUserFavorableCardHandle)
]
