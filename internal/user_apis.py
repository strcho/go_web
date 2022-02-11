import json

from mbutils import (
    nacos,
    MbException,
)

# 用户服务
nacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-user",
    timeout=3,
)


@nacosClient.customRequestClient(method="POST", url="/user/detail",)
def internal_get_userinfo_by_id(jsonData):
    pass


@nacosClient.customRequestClient(method="POST", url="/userState/depositedStateChange",)
def internal_deposited_state_change(jsonData):
    pass


@nacosClient.customRequestClient(method="POST", url="/userState/depositCardStateChange",)
def internal_deposited_card_state_change(jsonData):
    pass


class UserApi:
    """
    用户服务 api
    """

    @staticmethod
    def get_user_info(pin: str, command_context: dict):

        param = {"pin": pin, 'commandContext': command_context}
        user_res = internal_get_userinfo_by_id(param)
        user_res_data = json.loads(user_res)
        if not user_res_data.get("success"):
            raise MbException("用户服务调用失败")

        return user_res_data.get('data')
