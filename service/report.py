import time

from sqlalchemy import func

from model.all_model import *
from mbutils import cfg, DefaultMaker
from mbutils import dao_session, logger
from mbutils.aliyun_func import AliyunFunc
from utils.constant.redis_key import EXPORT_MERCHANT_REPORT_LOCK, EXPORT_REVENUE_REPORT_LOCK, \
    EXPORT_SETTLEMENT_REPORT_LOCK
from . import MBService


class ReportService(MBService):

    def export_bill_record(self, file_name: str, file_type: int, user_id):
        """
        向xc_ebike_bill_record插入一条记录
        :param user_id:
        :param file_name: 文件名
        :param file_type: 文件类型:商户对账：10001,营收对账：10002,结算对账：10003
        :return:
        """
        logger.info("export_bill_record start: {}".format(file_type))
        if file_type == 10001:
            result = dao_session.redis_session.r.set(EXPORT_MERCHANT_REPORT_LOCK, int(time.time()), nx=True,
                                                     px=10 * 60 * 1000)
            if not result:
                return EXPORT_MERCHANT_REPORT_LOCK
        elif file_type == 10002:
            result = dao_session.redis_session.r.set(EXPORT_REVENUE_REPORT_LOCK, int(time.time()), nx=True,
                                                     px=30 * 60 * 1000)
            if not result:
                return EXPORT_REVENUE_REPORT_LOCK
        elif file_type == 10003:
            result = dao_session.redis_session.r.set(EXPORT_SETTLEMENT_REPORT_LOCK, int(time.time()), nx=True,
                                                     px=60 * 60 * 1000)
            if not result:
                return EXPORT_SETTLEMENT_REPORT_LOCK

        params = {
            "name": file_name,
            "fileType": file_type,
            "opManId": user_id,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }

        logger.info("export_bill_record add start {}".format(params))
        record = XcEbikeBillRecord(**params)
        dao_session.session().add(record)
        dao_session.session().commit()

    def update_bill_record(self, file_name: str, status: int, file_type: int):
        """
        更新文件记录
        :param file_type:
        :param status:
        :param file_name: 文件名
        :return:
        """
        bill_record = {
            "status": status,
            "complateDate": datetime.now(),
            "updatedAt": datetime.now()
        }
        dao_session.session().query(XcEbikeBillRecord). \
            filter(XcEbikeBillRecord.name == file_name, XcEbikeBillRecord.fileType == file_type).update(bill_record)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def merchant_export_func_request(self, start_time: int, end_time: int, file_name: str):
        """
        调用函数计算生成merchant报表
        :param start_time: 毫秒时间戳
        :param end_time: 毫秒时间戳
        :param file_name: 文件名
        :return:
        """
        try:
            aliyun_info = cfg.get("aliyun")
            ops_config = cfg.get("OpsConfig")
            # 函数计算region
            fc_region = ops_config.get("FCregion")
            fc_server_name = ops_config.get("FcServerName", None)
            service_name = "{}-{}".format(fc_server_name, "script4py") if fc_server_name else "script4py"
            func_client = AliyunFunc(
                account_id=aliyun_info.get("accountId"),
                region=fc_region,
                access_key_id=aliyun_info.get("accessKeyId"),
                access_key_secret=aliyun_info.get("secretAccessKey"),
                service_name=service_name,
                function_name="merchantReport",
                body={"start_time": start_time, "end_time": end_time, "file_name": file_name}
            )
            func_client.do_http_request()
        except Exception as ex:
            # 更新文件状态为失败
            self.update_bill_record(file_name=file_name, status=3, file_type=10001)
            logger.exception(ex)
        finally:
            logger.info("merchant_export_func_request finally: {}".format(time.time()))
            dao_session.redis_session.r.delete(EXPORT_MERCHANT_REPORT_LOCK)

    def revenue_export_func_request(self, start_time: int, end_time: int, file_name: str, service_ids: list):
        """
        调用函数计算生成revenue报表
        :param start_time: 毫秒时间戳
        :param end_time: 毫秒时间戳
        :param file_name: 文件名
        :param service_ids: 服务区id
        :return:
        """
        try:
            aliyun_info = cfg.get("aliyun")
            ops_config = cfg.get("OpsConfig")
            # 函数计算region
            fc_region = ops_config.get("FCregion")
            fc_server_name = ops_config.get("FcServerName", None)
            service_name = "{}-{}".format(fc_server_name, "script4py") if fc_server_name else "script4py"
            func_client = AliyunFunc(
                account_id=aliyun_info.get("accountId"),
                region=fc_region,
                access_key_id=aliyun_info.get("accessKeyId"),
                access_key_secret=aliyun_info.get("secretAccessKey"),
                service_name=service_name,
                function_name="revenueReport",
                body={"start_time": start_time, "end_time": end_time, "file_name": file_name,
                      "service_ids": service_ids}
            )
            func_client.do_http_request()
        except Exception as ex:
            # 更新文件状态为失败
            self.update_bill_record(file_name=file_name, status=3, file_type=10002)
            logger.exception(ex)
        finally:
            logger.info("revenue_export_func_request finally: {}".format(time.time()))
            dao_session.redis_session.r.delete(EXPORT_REVENUE_REPORT_LOCK)

    def settlement_export_func_request(self, start_time: int, end_time: int, file_name: str, service_ids: list, task_id,
                                       agent_id):
        """
        调用函数计算生成settlement报表
        :param start_time: 毫秒时间戳
        :param end_time: 毫秒时间戳
        :param file_name: 文件名
        :param service_ids: 服务区id
        :param task_id: 任务id
        :param agent_id: 公司id
        :return:
        """
        try:
            aliyun_info = cfg.get("aliyun")
            ops_config = cfg.get("OpsConfig")

            if isinstance(task_id, DefaultMaker) or isinstance(agent_id, DefaultMaker):
                fc_body = {"start_time": start_time, "end_time": end_time, "file_name": file_name,
                           "service_ids": service_ids}
            else:
                fc_body = {"start_time": start_time, "end_time": end_time, "file_name": file_name,
                           "service_ids": service_ids, "task_id": task_id, "agent_id": agent_id}
            # 函数计算region
            fc_region = ops_config.get("FCregion")
            fc_server_name = ops_config.get("FcServerName", None)
            service_name = "{}-{}".format(fc_server_name, "script4py") if fc_server_name else "script4py"
            func_client = AliyunFunc(
                account_id=aliyun_info.get("accountId"),
                region=fc_region,
                access_key_id=aliyun_info.get("accessKeyId"),
                access_key_secret=aliyun_info.get("secretAccessKey"),
                service_name=service_name,
                function_name="dashboardReport",
                body=fc_body
            )
            func_client.do_http_request()
        except Exception as ex:
            # 更新文件状态为失败
            self.update_bill_record(file_name=file_name, status=3, file_type=10003)
            logger.exception(ex)
        finally:
            logger.info("revenue_export_func_request finally: {}".format(time.time()))
            dao_session.redis_session.r.delete(EXPORT_SETTLEMENT_REPORT_LOCK)

    def query_reconciliation_list(self, user_id, page, size):
        """
        查询当前用户的下载历史
        :param size:
        :param page:
        :param user_id: 后台用户登录手机号
        :return:
        """

        count = dao_session.session().query(func.count(XcEbikeBillRecord.id)).filter(
            XcEbikeBillRecord.opManId == user_id, XcEbikeBillRecord.fileType.in_([10001, 10002, 10003])).scalar()
        reconciliation_list = dao_session.session().query(
            XcEbikeBillRecord.name.label("file_name"),
            XcEbikeBillRecord.createdAt,
            XcOpman.name,
            XcEbikeBillRecord.status,
            XcEbikeBillRecord.complateDate,
        ).join(XcEbikeBillRecord, XcEbikeBillRecord.opManId == XcOpman.opManId).filter(
            XcEbikeBillRecord.opManId == user_id, XcEbikeBillRecord.fileType.in_([10001, 10002, 10003])).order_by(
            XcEbikeBillRecord.createdAt.desc()).limit(size).offset(
            page * size).all()

        rows = []
        for r in reconciliation_list:
            info = {
                "file_name": r[0],
                "createdAt": r[1].strftime("%Y-%m-%d %H:%M:%S"),
                "name": r[2],
                "status": r[3],
                "complateDate": r[4].strftime("%Y-%m-%d %H:%M:%S") if r[4] else "",
            }
            rows.append(info)

        data = {
            "count": count,
            "rows": rows
        }
        return data

    def get_car_line(self, valid_data):
        """
        获取车辆数量
        :param valid_data:
        :return:
        """
        service_ids, begin_time, end_time = valid_data
        begin_date = datetime.fromtimestamp(begin_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)

        filters = set()
        filters.add(XcMieba2HourCarAnalysis.created_at.between(begin_date, end_date))
        filters.add(XcMieba2HourCarAnalysis.service_id.in_(service_ids))
        car_analysis_info = dao_session.session().query(
            XcMieba2HourCarAnalysis.created_at.label("date"),
            XcMieba2HourCarAnalysis.service_id.label("service_id"),
            XcMieba2HourCarAnalysis.total.label("car_num")
        ).filter(*filters).all()
        car_line = []
        for r in car_analysis_info:
            date_dict = {"date": r[0].strftime("%Y-%m-%d %H:%M:%S"), "service_id": r[1], "car_num": r[2]}
            car_line.append(date_dict)
        return car_line
