import json
import ssl
import time
import traceback
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from service.kafka import KafkaTopic
from service.kafka import PayKey, KafkaRetry, CONSUMER_GROUP_ID
from service.kafka.producer import kafka_client
from service.payment.deposit import DepositService
from service.payment.deposit_card import DepositCardService
from service.payment.favorable_card import FavorableCardService
from service.payment.riding_card import RidingCardService
from service.payment.wallet import WalletService
from mbutils import cfg, compute_name
from mbutils import logger

kafka_config = cfg.get("kafkaConfig")
context = ssl.create_default_context()
context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
context.verify_mode = ssl.CERT_REQUIRED
context.check_hostname = False
context.load_verify_locations(cadata=kafka_config.get("certificate"))


def kafka_deal_data():
    logger.info("kafka_deal_data consumer start")
    consumer = KafkaConsumer(*KafkaTopic.to_tuple(),
                             bootstrap_servers=kafka_config.get("bootstrap_servers").split(","),
                             sasl_mechanism="PLAIN",
                             ssl_context=context,
                             api_version=(2, 2),
                             max_poll_interval_ms=60000,
                             max_poll_records = 10,
                             auto_offset_reset="earliest",
                             security_protocol='SASL_SSL',
                             sasl_plain_username="alikafka_post-cn-tl32a2goi003",
                             sasl_plain_password="tVlwcQHiTEQq8HGIK0iyleU74dfROC8w",
                             enable_auto_commit=False,
                             client_id=f"{CONSUMER_GROUP_ID}_{compute_name}",
                             group_id=CONSUMER_GROUP_ID,
                             key_deserializer=bytes.decode,
                             value_deserializer=bytes.decode)

    for msg in consumer:
        """
        ConsumerRecord(topic='PAYqiyiqi', partition=0, offset=38, timestamp=1627292591303, 
        timestamp_type=0, key=b'wallet', value=b'{"object_id": "60fa5522f69486000112ef90", "amount": 1, 
        "trade_no": "2021072622001476181453919966", "gmt_create": "2021-07-26+17:43:10", "account_channel": 1}', 
        headers=[], checksum=None, serialized_key_size=6, serialized_value_size=157, serialized_header_size=-1)
        
        报错提交失败:
        kafka的Consumer 端还有一个参数，用于控制 Consumer 实际消费能力对 Rebalance 的影响，即 max.poll.interval.ms 参数。
        它限定了 Consumer 端应用程序两次调用 poll 方法的最大时间间隔。它的默认值是 5 分钟，
        表示你的 Consumer 程序如果在 5 分钟之内无法消费完 poll 方法返回的消息，那么 Consumer 会主动发起 “离开组” 的请求，
        此时就不会提交偏移量了，Coordinator 也会开启新一轮 Rebalance。
        """
        logger.info("kafka消息:", msg)
        # 消息格式 msg.topic, msg.partition, msg.offset, msg.key, msg.value
        try:
            data_dict = json.loads(msg.value)
            if not isinstance(data_dict, dict):
                break
            if msg.topic == KafkaTopic.PAYMENT.value:
                try:
                    if msg.key == PayKey.DEPOSIT.value:
                        DepositService.handle_deposit(data_dict)
                    elif msg.key == PayKey.DEPOSIT_CARD.value:
                        DepositCardService.handle_deposit_card(data_dict)
                    elif msg.key == PayKey.FAVORABLE_CARD.value:
                        FavorableCardService.handle_favorable_card(data_dict)
                    elif msg.key == PayKey.RIDING_CARD.value:
                        RidingCardService.handle_riding_card(data_dict)
                    elif msg.key == PayKey.WALLET.value:
                        WalletService.handle_wallet(data_dict)
                    else:
                        break
                    logger.info("正常消费消息:",msg.offset)
                except KafkaRetry:
                    # 当数据出现异常后，将次消息从新插入队列之中
                    logger.error("KafkaRetry:", msg.value, msg.key)
                    time.sleep(0.5)
                    kafka_client.pay_send(msg.value, key=msg.key)
                except Exception as ex:
                    logger.info("异常消费消息", ex)
        except Exception as ex:
            logger.info("参数格式不正确：", msg.value, ex)
        finally:
            # before a return, break or continue statement
            consumer.commit(
            offsets={TopicPartition(msg.topic, msg.partition): OffsetAndMetadata(msg.offset + 1, None)})




if __name__ == '__main__':
    kafka_deal_data()
