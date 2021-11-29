from routes.free_order.view import (
    GetUserFreeOrderHandler,
    GetUserAllFreeOrderHandler,
    UpdateUserFreeOrderHandler,
    BusUpdateUserFreeOrderHandler,
)


# /account/user
urls = [
    ("/get_user_free_order", GetUserFreeOrderHandler),
    ("/get_user_all_free_order", GetUserAllFreeOrderHandler),
    ("/update_user_free_order", UpdateUserFreeOrderHandler),

    # B端
    ("/business/update_user_free_order", BusUpdateUserFreeOrderHandler),
]
