import json
import math
from datetime import timedelta

from model.all_model import *
from service.config import ConfigService
from mbutils import MbException
from mbutils import dao_session
from mbutils import logger
from utils.constant.account import SERIAL_TYPE, RIDING_CHANNEL_TYPE, PAY_TYPE, DEPOSIT_CONFIG_TYPE, DEPOSIT_CHANNEL_TYPE
from utils.constant.config import ConfigName
from utils.constant.redis_key import *
from utils.constant.user import UserState
from . import MBService


class VoiceService(MBService):
    def query_list(self, valid_data: tuple):
        status, imei, startTime, endTime, page, size = valid_data
        select_params = {
            "status_where": '1=1',
            "imei_where": '1=1',
            "updatedAt_where": "1=1",
            "limit_factor": "limit {}, {}".format(page * size, size),
        }
        if self.exists_param(status):
            select_params["status_where"] = "status={}".format(status)
        if self.exists_param(imei):
            select_params["imei_where"] = "imei={}".format(imei)
        if self.exists_param(startTime) and self.exists_param(endTime):
            select_params["updatedAt_where"] = "updatedAt between '{}' and '{}'".format(self.num2datetime(startTime),
                                                                                        self.num2datetime(endTime))

        res = dao_session.session().execute("""
        select count(*)
        from  xc_voice_download_records
        where {status_where} and {imei_where} and {updatedAt_where}""".format(**select_params)).first()
        if not res:
            return {"list": [], "count": 0}
        count = res[0]
        infos = dao_session.session().execute("""
        select imei, agentId, status, createdAt, updatedAt
        from  xc_voice_download_records
        where {status_where} and {imei_where} and {updatedAt_where}
        {limit_factor}""".format(**select_params))

        return {"list": [{"imei": info["imei"],
                          "agentId": info["agentId"],
                          "status": info["status"],
                          "createdAt": self.datetime2num(info["createdAt"]),
                          "updatedAt": self.datetime2num(info["updatedAt"])
                          } for info in infos], "count": count}
