from mbutils.constant import MbEnum
from mbutils import AGENT_NAME
# AGENT_NAME = "qiyiqi"

class KafkaRetry(Exception):
    """ kafka重传异常类 """
    pass


class PayKey(MbEnum):
    """ 支付相关的key集合 """
    DEPOSIT = "deposit"
    DEPOSIT_CARD = "deposit_card"
    FAVORABLE_CARD = "favorable_card"
    RIDING_CARD = "riding_card"
    WALLET = "wallet"


class KafkaTopic(MbEnum):
    """ python端收发的topic """
    PAYMENT = f"{AGENT_NAME}_PAY"


# 每个端的消费者组
CONSUMER_GROUP_ID = f"{AGENT_NAME}_python"
