import requests
from mbutils import cfg, logger
from mbutils import MbException
import jwt, json
import datetime

NODE_URL = cfg["apiurl"]  # https://chuduapi.xiaoantech.com
api_key = cfg['apiKey']


def node_auto_lock(object_id):
    # 调用node的autoLock
    url = f'{NODE_URL}/ebike/v2/device/lock/autoLock'.replace("8600", "8083")  # 如果存在8600则改为8083
    token = jwt.encode({"userId": "", "phone": "", "agentId": 2, "timestamp": datetime.datetime.now().timestamp()},
                       api_key).decode()
    headers = {
        'Content-type': 'application/json',
        "authorization": f"Bearer {token}"
    }
    try:
        response = requests.post(url=url, json={"objectId": object_id}, headers=headers)
        return bool(response and response.text and json.loads(response.text).get("suc"))
    except Exception as ex:
        logger.error("autoLock调用失败:", url, ex)
        # raise MbException("调用失败")
