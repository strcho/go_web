import json

from kafka import KafkaProducer

from mbutils import (
    cfg,
    logger,
    KAFKA_NAME_PREFIX,
    single_instance,
)


@single_instance
class KafkaClient():
    def __init__(self):
        # 参数见: https://shimo.im/docs/m5kv9Jn91WFQ0EqX
        # print(cfg.get("kafka_config"), cfg)
        self.producer = KafkaProducer(
            bootstrap_servers=cfg.get("kafka_config", {})["bootstrap_servers"],
            api_version=(2, 5, 1),
            client_id='visual',
            retries=3,
            retry_backoff_ms=1000,
            max_block_ms=3000,
            value_serializer=str.encode
        )

    @staticmethod
    def on_send_success(msg):
        logger.info("msg is send, msg:", msg)

    @staticmethod
    def on_send_error(excp):
        logger.info('I am an errback: {}'.format(excp))

    def visual_send(self, msg: dict, key: str):
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
            self.producer.send(f'{cfg["kafka_config"]["name_prefix"]}_visual', value=msg, key=key.encode("utf-8")).add_callback(
                self.on_send_success).add_errback(
                self.on_send_error)
            self.producer.flush()
            logger.json({
                "topic": f'{KAFKA_NAME_PREFIX}_visual',
                "value": str(msg),
                "key": key,
            })
            return True
        except Exception as ex:
            logger.info("kafka发送支付消息失败", ex)
            return False
