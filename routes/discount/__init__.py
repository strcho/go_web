from routes.discount.view import (
    GetUserDiscountHandler,
    GetUserAllDiscountHandler,
    UpdateUserDiscountHandler,
    BusUpdateUserDiscountHandler,
)


# /account/user
urls = [
    ("/internal/get_user_discount", GetUserDiscountHandler),
    ("/internal/get_user_all_discount", GetUserAllDiscountHandler),
    ("/internal/update_user_discount", UpdateUserDiscountHandler),

    # B端网关
    ("/business/update_user_discount", BusUpdateUserDiscountHandler),
]
