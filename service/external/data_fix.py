import subprocess

from service import MBService


class GetLogService(MBService):
    def get_log(self,valid_data):
        _type, row_num = valid_data
        if _type == "info":
            res = subprocess.getoutput("tail -n {} /logs/ebike/*.log".format(row_num))
        else:
            res = subprocess.getoutput("tail -n {} /logs/ebike/*.log|grep -v get_log".format(row_num))
        return res.split("\n")[::-1]