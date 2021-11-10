import subprocess

from service import MBService


class GetLogService(MBService):
    def get_log(self, valid_data):
        _type, is_node, row_num = valid_data
        if is_node:
            if _type == "info":
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/eBikeServer-*.log".format(row_num))
            elif _type == "error":
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/error.log.*.log".format(row_num))
            else:
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/biz-*.log".format(row_num))
        else:
            if _type == "info":
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/mbServer_*.log".format(row_num))
            elif _type == "error":
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/error_*.log".format(row_num))
            else:
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/biz_*.log|grep -v get_log".format(row_num))
        return res.split("\n")[::-1]