import random
import time
from paho.mqtt import client

broker = 'dispatchmqtt.xiaoantech.com'
port = 1883
client_id = f'meiba-api-{random.randint(0, 1000)}-{time.time()}'


class MqttPub:
    def __init__(self):
        """
        web接口中的mqtt
        """
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connnect, return code %d\n", rc)

        self.c = client.Client(client_id, clean_session=False)
        self.c.on_connect = on_connect
        try:
            self.c.connect(broker, port, keepalive=15)
            self.c.loop_start()
        except Exception:
            print("mqtt network error")

    def publish(self, topic, msg):
        result = self.c.publish(topic, msg, qos=1)
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


mqtt_client = MqttPub()

