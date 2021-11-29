from routes.discount.view import (
    GetUserDiscountHandler,
    GetUserAllDiscountHandler,
    UpdateUserDiscountHandler,
    BusUpdateUserDiscountHandler,
)


# /account/user
urls = [
    ("/get_user_discount", GetUserDiscountHandler),
    ("/get_user_all_discount", GetUserAllDiscountHandler),
    ("/update_user_discount", UpdateUserDiscountHandler),

    # B端网关
    ("/business/update_user_discount", BusUpdateUserDiscountHandler),
]
