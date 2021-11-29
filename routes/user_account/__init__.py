from routes.user_account.view import (
        UserAccount,
        BusUserAccount,
)

# /account/user
urls = [
        (r'/user_account', UserAccount),  # 用户资产

        # B端网关
        (r'/business/user_account', BusUserAccount),  # 用户资产
]
