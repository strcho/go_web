from routes.favorable_card.view import (
    GetUserFavorableCardHandle,
    GetUserFavorableDaysHandle,
    SendUserFavorableCardHandle,
    ModifyUserFavorableCardHandle,
    BusModifyUserFavorableCardHandle, FavorableCardToKafkaHandle,
)

# /account/user
urls = [
    ("/internal/get_user_favorable_card", GetUserFavorableCardHandle),
    ("/internal/get_user_favorable_days", GetUserFavorableDaysHandle),
    ("/internal/send_user_favorable_card", SendUserFavorableCardHandle),
    ("/internal/modify_user_favorable_card", ModifyUserFavorableCardHandle),
    ("/internal/favorable_card_to_kafka", FavorableCardToKafkaHandle),

    # B端网关
    ("/business/modify_user_favorable_card", BusModifyUserFavorableCardHandle),
]
