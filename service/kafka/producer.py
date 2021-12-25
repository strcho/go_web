# import json
# import ssl
# from kafka import KafkaProducer
#
# from service.kafka import KafkaTopic
# from mbutils import logger, cfg,single_instance
#
# kafka_config = cfg.get("kafkaConfig")
#
# context = ssl.create_default_context()
# context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
# context.verify_mode = ssl.CERT_REQUIRED
# context.check_hostname = False
# context.load_verify_locations(cadata=kafka_config.get("certificate"))
#
#
# @single_instance
# class KafkaClient():
#     def __init__(self):
#         # 参数见: https://shimo.im/docs/m5kv9Jn91WFQ0EqX
#         self.producer = KafkaProducer(
#             bootstrap_servers=kafka_config.get("bootstrap_servers").split(","),
#             sasl_mechanism="PLAIN",
#             ssl_context=context,
#             api_version=(2, 2),
#             security_protocol='SASL_SSL',
#             sasl_plain_username=kafka_config.get('sasl_plain_username'),
#             sasl_plain_password=kafka_config.get('sasl_plain_password'),
#             retries=3,
#             retry_backoff_ms=1000,
#             max_block_ms=2000,
#             key_serializer=str.encode,
#             value_serializer=str.encode)
#
#     @staticmethod
#     def on_send_success(msg):
#         logger.info("msg is send, msg:", msg)
#
#     @staticmethod
#     def on_send_error(excp):
#         print('I am an errback: {}'.format(excp))
#
#     def pay_send(self, msg: dict, key: str):
#         """
#         1. kafka producer自己完成重试
#         2.TODO 发送完记录到数据库
#         :param msg: 发送的消息dict
#         :param key:指定发送的key
#         :return:
#         """
#         try:
#             if isinstance(msg, dict):
#                 msg = json.dumps(msg)
#             self.producer.send(KafkaTopic.PAYMENT.value, msg, key).add_callback(self.on_send_success).add_errback(
#                 self.on_send_error)
#             self.producer.flush()
#             return True
#         except Exception as ex:
#             logger.error("kafka发送支付消息失败", ex)
#             return False
#
#
# kafka_client = KafkaClient()
#
# if __name__ == '__main__':
#     kafka_client.pay_send("test", "test")


import json
import ssl
from datetime import datetime

from kafka import KafkaProducer


class KafkaClient():
    def __init__(self):
        # 参数见: https://shimo.im/docs/m5kv9Jn91WFQ0EqX
        self.producer = KafkaProducer(
            bootstrap_servers=['120.78.168.217:9092'],
            api_version=(2, 5, 1),
            # sasl_mechanism="PLAIN",
            # security_protocol='SASL_PLAINTEXT',
            # sasl_plain_username='producer',
            # sasl_plain_password='1GyMeXs4X4',
            # client_id='visual',
            client_id='visual',
            retries=3,
            retry_backoff_ms=1000,
            max_block_ms=3000,
            # key_serializer=str.encode,
            value_serializer=str.encode
        )

    @staticmethod
    def on_send_success(msg):
        print("msg is send, msg:", msg)

    @staticmethod
    def on_send_error(excp):
        print('I am an errback: {}'.format(excp))

    def pay_send(self, msg: dict, key: str):
        """
        1. kafka producer自己完成重试
        2.TODO 发送完记录到数据库
        :param msg: 发送的消息dict
        :param key:指定发送的key
        :return:
        """
        try:
            if isinstance(msg, dict):
                msg = json.dumps(msg)
            print(self.producer.config)
            self.producer.send('test_visual', value=msg, key=key.encode("utf-8")).add_callback(
                self.on_send_success).add_errback(
                self.on_send_error)
            self.producer.flush()
            return True
        except Exception as ex:
            print("kafka发送支付消息失败", ex)
            return False


kafka_client = KafkaClient()

if __name__ == '__main__':
    test = {"bike": "test"}
    kafka_client.pay_send(test, "wallet")






