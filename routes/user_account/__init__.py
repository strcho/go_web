from routes.user_account.view import (
        UserAccount,
        BusUserAccount,
        ClientUserAccount,
)

# /account/user
urls = [
        (r'/internal/user_account', UserAccount),  # 用户资产

        # B端网关
        (r'/business/user_account', BusUserAccount),  # 用户资产

        # C端网关
        (r'/client/user_account', ClientUserAccount),  # 用户资产
]
