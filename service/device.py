from datetime import datetime
import requests
from mbutils import dao_session
import json
import time
from functools import partial
from mbutils import logger


def acc_fn(imei, acc, is_ignore_fence=False):
    """
    挪车操作的时候,is_ignore_fence开启
    :param imei:
    :param acc:电门
    :param is_ignore_fence: 该次启动不受围栏影响，重新启动或设防或熄火或撤防后恢复
    :return:
    """
    # 跟node的acc_fn是相反的
    # isIgnoreFence:
    return {
        "imei": imei,
        "cmd":
            {
                "c": 33,
                "param": {
                    "acc": 1 if acc else 0,
                    "isIgnoreFence": 1 if is_ignore_fence else 0,
                }
            }
    }


def lock_fn(imei, is_defend):
    # 包含 node:defendFn
    return {
        "imei": imei,
        "cmd":
            {
                "c": 4,
                "param":
                    {"defend": 1 if is_defend else 0}
            }
    }


def car_searching(imei):
    return {
        "imei": imei,
        "cmd": {
            "c": 14,
            "param": {"idx": 9}
        }
    }


def open_batbox_cmd(imei):
    # 电池仓就是后座锁
    return {
        "imei": imei,
        "cmd": {
            "c": 40,
            "tm": int(datetime.now().timestamp()),
            "dt": 10,
            "param": {"sw": 0}
        }
    }


def lookfor_battery_and_back_seat(imei):
    return {
        "imei": imei,
        "cmd": {"c": 34}
    }


def send_cmd_v2(cmd: dict, max_retry: int = 3, start_interval: float = 0.1) -> dict:
    """
    :param cmd:发送命令
    :param max_retry:最大重试次数
    :param start_interval:初始重试间隔
    :return:
    {
        "code": 0,
        "result": {
            "restoreVoltage": 45000,
            "isOverSpeedOn": 0,
            "isMoveAlarmOn": 0,
        }
    }
    """
    headers = {
        "Content-Type": "application/json"
    }
    DEVICE_HOST = "xc_dev_host_{imei}"
    host = dao_session.redis_session.r.get(DEVICE_HOST.format(imei=cmd["imei"]))
    if not host:
        return
    url = f"http://{host}/v1/device"
    un_retry_codes = [0, 109]  # 0表示成功, 109表示地址错误?
    count = 0
    logger.info("url is:", url)
    while count < max_retry:
        r = requests.post(url=url, data=json.dumps(cmd), headers=headers)
        # logger.info("cmd:", cmd, r.text)
        res = json.loads(r.text)
        if res["code"] in un_retry_codes:
            break
        else:
            # 重试
            time.sleep(start_interval * (2 ** count))
            count += 1
    # 返回的是最后一次结果
    return res


send_cmd = partial(send_cmd_v2, max_retry=3, start_interval=0.1)  # 耗时0.1, 0.2, 0.4
