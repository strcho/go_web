from routes.free_order.view import (
    GetUserFreeOrderHandler,
    GetUserAllFreeOrderHandler,
    UpdateUserFreeOrderHandler,
    BusUpdateUserFreeOrderHandler,
)


# /account/user
urls = [
    ("/internal/get_user_free_order", GetUserFreeOrderHandler),
    ("/internal/get_user_all_free_order", GetUserAllFreeOrderHandler),
    ("/internal/update_user_free_order", UpdateUserFreeOrderHandler),

    # B端
    ("/business/update_user_free_order", BusUpdateUserFreeOrderHandler),
]
