from routes.discount.view import (
    GetUserDiscountHandler,
    GetUserAllDiscountHandler,
    UpdateUserDiscountHandler,
)


# /account/user
urls = [
    ("/get_user_discount", GetUserDiscountHandler),
    ("/get_user_all_discount", GetUserAllDiscountHandler),
    ("/update_user_discount", UpdateUserDiscountHandler),
]
