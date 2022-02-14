import json

from mbutils import (
    nacos,
    MbException,
)

# 营销服务
marketingNacosClient = nacos.nacosBalanceClient(
    serviceName="ebike-marketing",
    timeout=3)


@marketingNacosClient.customRequestClient(method="POST", url="/ebike_marketing/riding_config/platform/riding_card", )
def riding_card_info(jsonData):
    pass


@marketingNacosClient.customRequestClient(method="POST", url="/ebike_marketing/favorable_config/platform/favorable_card", )
def favorable_card_info(jsonData):
    pass


@marketingNacosClient.customRequestClient(method="POST", url="/ebike_marketing/deposit_config/platform/deposit_card", )
def deposit_card_info(jsonData):
    pass


class MarketingApi:
    """
    营销活动 api
    """

    @staticmethod
    def get_riding_card_info(config_id: int, command_context: dict):
        param = {"riding_card_config_id": config_id, 'commandContext': command_context}
        card_res = riding_card_info(param)
        card_res_data = json.loads(card_res)
        if not card_res_data.get("success"):
            raise MbException("营销活动服务调用失败")

        return card_res_data.get("data")

    @staticmethod
    def get_favorable_card_info(config_id: int, command_context: dict):
        param = {"card_id": config_id, 'commandContext': command_context}
        card_res = favorable_card_info(param)
        card_res_data = json.loads(card_res)
        if not card_res_data.get("success"):
            raise MbException("营销活动服务调用失败")

        return card_res_data.get("data")

    @staticmethod
    def get_deposit_card_info(config_id: int, command_context: dict):
        param = {"deposit_card_id": str(config_id), 'commandContext': command_context}
        card_res = deposit_card_info(param)
        card_res_data = json.loads(card_res)
        if not card_res_data.get("success"):
            raise MbException("营销活动服务调用失败")

        return card_res_data.get("data")
