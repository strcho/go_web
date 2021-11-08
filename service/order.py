import gc
import sys
import time
import zipfile

import pandas as pd
from io import BytesIO
import concurrent.futures

from mbutils import dao_session, logger, DefaultMaker
from utils.aliyun_func import AliyunFunc
from utils.aliyun_oss import AliyunOSS
from utils.constant.redis_key import EXPORT_EXCEL_LOCK
from . import MBService
from model.all_model import *
from mbutils import cfg


class OrderService(MBService):
    def __init__(self):
        self.limit = 40000
        self.order_header = ["订单编号", "行程编号", "车辆编号", "姓名", "手机号", "支付状态", "订单费用(元)", "充值金额结算(元)",
                             "赠送金额结算(元)", "折扣", "骑行费用(元)", "调度费(元)", "优惠方式", "开始时间", "结束时间", "骑行时长", "骑行距离(公里)"]

    def export_bill_record(self, file_name: str):
        """
        向xc_ebike_bill_record插入一条记录
        :param file_name: 文件名
        :return:
        """
        result = dao_session.redis_session.r.set(EXPORT_EXCEL_LOCK, int(time.time()), nx=True, px=10 * 60 * 1000)
        if not result:
            return EXPORT_EXCEL_LOCK
        params = {
            "name": file_name,
            "fileType": 2,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
        record = XcEbikeBillRecord(**params)
        dao_session.session().add(record)
        dao_session.session().commit()

    def update_bill_record(self, file_name: str):
        """
        更新文件记录
        :param file_name: 文件名
        :return:
        """
        bill_record = {
            "status": 2,
            "complateDate": datetime.now(),
            "updatedAt": datetime.now()
        }
        dao_session.session().query(XcEbikeBillRecord). \
            filter(XcEbikeBillRecord.name == file_name, XcEbikeBillRecord.fileType == 2).update(bill_record)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def df_excel_one(self, df):
        """
        写入一个sheet
        :param df:
        :return:
        """
        df[1].to_excel(df[0], sheet_name=df[2], index=False)

    def df_excel_many(self, order_df_list: list):
        """
        future处理并发
        :param order_df_list:
        :return:
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.df_excel_one, order_df_list)

    def get_order_df(self, valid_data: tuple):
        """
        生成dataframe
        :param valid_data:
        :return:
        """
        a = time.time()
        logger.info("order_export start: {}".format(a))
        start_time, end_time, service_id, order_id, itin_id, car_id, person_info, phone, is_complainted, \
        is_paid, discount_type, itinerary, equal = valid_data
        start_date = datetime.fromtimestamp(start_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)

        params = {"start_date": start_date, "end_date": end_date, "service_id": service_id,
                  "order_id_where": "1=1", "itin_id_where": "1=1", "car_id_where": "1=1", "person_info_where": "1=1",
                  "phone_where": "1=1", "is_complainted_where": "1=1", "is_paid_where": "1=1",
                  "discount_type_where": "1=1",
                  "itinerary_where": "1=1"}

        if not isinstance(order_id, DefaultMaker):
            params["orders_where"] = "o.orderId = '{}'".format(order_id)

        if not isinstance(itin_id, DefaultMaker):
            params["itin_id_where"] = "o.deviceItineraryId = '{}'".format(itin_id)

        if not isinstance(car_id, DefaultMaker):
            params["car_id_where"] = "i.carId = '{}'".format(car_id)

        if not isinstance(person_info, DefaultMaker):
            params["person_info_where"] = "u.personInfo like '%{}%'".format(person_info)

        if not isinstance(phone, DefaultMaker):
            params["phone_where"] = "u.phone = '{}'".format(phone)

        if not isinstance(is_complainted, DefaultMaker):
            params["is_complainted_where"] = "o.isComplainted = '{}'".format(is_complainted)

        if not isinstance(is_paid, DefaultMaker):
            params["is_paid_where"] = "o.isPaid = '{}'".format(is_paid)

        if not isinstance(discount_type, DefaultMaker):
            if discount_type == 2:
                params["discount_type_where"] = "o.isUseRidingCard = 1"
            elif discount_type == 3:
                params["discount_type_where"] = "o.isFreeOrder =1"
            elif discount_type == 4:
                params["discount_type_where"] = "(o.discount < 1 and o.isFreeOrder = 0 and o.isUseRidingCard = 0)"
            elif discount_type == 1:
                params["discount_type_where"] = "(o.discount >= 1 and o.isFreeOrder = 0 and o.isUseRidingCard = 0)"

        if not isinstance(itinerary, DefaultMaker):
            if equal == 1:
                params["itinerary_where"] = "i.itinerary > '{}'".format(itinerary)
            elif equal == 2:
                params["itinerary_where"] = "i.itinerary < '{}'".format(itinerary)

        order_sql = '''
                    SELECT
                        o.orderId,
                        o.deviceItineraryId,
                        i.carId,
                        REPLACE(JSON_EXTRACT(u.personInfo, '$.name'),'"','') AS name,
                        u.phone,
                        IF(o.isPaid=1, "已支付","未支付") AS isPaid,
                        o.cost/100 AS cost,
                        o.rechargeCost/100 AS rechargeCost,
                        o.presentCost/100 AS presentCost,
                        o.discount,
                        o.originCost/100 AS originCost,
                        ifnull(o.penalty/100,0) AS penalty,
                        CASE
                       WHEN o.isFreeOrder =1 THEN "新用户免单"
                       WHEN o.isUseRidingCard = 1 THEN "折扣支付"
                         WHEN (o.discount < 1 and o.isFreeOrder = 0 and o.isUseRidingCard = 0) THEN "骑行卡"
                         ELSE "无优惠"
                        END discountType,
                        i.startTime,
                        i.endTime,
                        date_format(date_sub(from_unixtime( TIMESTAMPDIFF(SECOND, i.startTime,i.endTime)), INTERVAL 8 HOUR), '%H:%i:%s') AS ridingTime,
                        ifnull(i.itinerary/1000,0) AS itinerary
                    FROM
                        xc_ebike_user_orders AS o
                        LEFT JOIN xc_ebike_device_itinerary AS i ON o.deviceItineraryId = i.itinId
                        LEFT JOIN xc_ebike_usrs_2 AS u ON o.userId = u.id 
                    WHERE
                        o.serviceId =:service_id
                        AND i.startTime BETWEEN :start_date AND :end_date
                        AND :order_id_where AND :itin_id_where AND :car_id_where AND :person_info_where AND :phone_where
                        AND :is_complainted_where AND :is_paid_where AND :discount_type_where AND :itinerary_where
                    ORDER BY i.startTime DESC;
                '''

        orders = dao_session.session().execute(order_sql, params).fetchall()
        logger.info("order_export diff_1: {}".format(time.time() - a))
        # order_df = pd.DataFrame(orders, columns=self.order_header)
        # order_df['订单费用(元)'] = order_df['订单费用(元)'].map(lambda x: ("%.2f") % x)
        # order_df['充值金额结算(元)'] = order_df['充值金额结算(元)'].map(lambda x: ("%.2f") % x)
        # order_df['赠送金额结算(元)'] = order_df['赠送金额结算(元)'].map(lambda x: ("%.2f") % x)
        # order_df['骑行费用(元)'] = order_df['骑行费用(元)'].map(lambda x: ("%.2f") % x)
        # order_df['调度费(元)'] = order_df['调度费(元)'].map(lambda x: ("%.2f") % x)
        # order_df['骑行距离(公里)'] = order_df['骑行距离(公里)'].map(lambda x: ("%.3f") % x)
        b = time.time()
        order_info = {}
        for r in orders:
            a = dict(r)
            order_info.setdefault(self.order_header[0], []).append(a.get("orderId"))
            order_info.setdefault(self.order_header[1], []).append(a.get("deviceItineraryId"))
            order_info.setdefault(self.order_header[2], []).append(a.get("carId"))
            order_info.setdefault(self.order_header[3], []).append(a.get("name"))
            order_info.setdefault(self.order_header[4], []).append(a.get("phone"))
            order_info.setdefault(self.order_header[5], []).append(a.get("isPaid"))
            order_info.setdefault(self.order_header[6], []).append(round(float(a.get("cost")), 2))
            order_info.setdefault(self.order_header[7], []).append(round(float(a.get("rechargeCost")), 2))
            order_info.setdefault(self.order_header[8], []).append(round(float(a.get("presentCost")), 2))
            order_info.setdefault(self.order_header[9], []).append(a.get("discount"))
            order_info.setdefault(self.order_header[10], []).append(round(float(a.get("originCost")), 2))
            order_info.setdefault(self.order_header[11], []).append(round(float(a.get("penalty")), 2))
            order_info.setdefault(self.order_header[12], []).append(a.get("discountType"))
            order_info.setdefault(self.order_header[13], []).append(a.get("startTime"))
            order_info.setdefault(self.order_header[14], []).append(a.get("endTime"))
            order_info.setdefault(self.order_header[15], []).append(a.get("ridingTime"))
            order_info.setdefault(self.order_header[16], []).append(round(float(a.get("itinerary")), 3))
        order_df = pd.DataFrame(order_info)
        logger.info("order_export order_df size(kb): {}".format(int(order_df.memory_usage(deep=True).sum() / 1024)))
        logger.info("order_export diff_2: {}".format(time.time() - b))
        return order_df

    def chunk_order_bytes(self, order_df, valid_data: tuple, file_name: str):
        """
        拆分几个bytes
        :param order_df:
        :param valid_data:
        :return:
        """
        start_time, end_time, service_id, order_id, itin_id, car_id, person_info, phone, is_complainted, \
        is_paid, discount_type, itinerary, equal = valid_data
        start_date = datetime.fromtimestamp(start_time / 1000)
        end_date = datetime.fromtimestamp(end_time / 1000)

        order_df_rows = order_df.shape[0]
        index = order_df_rows // self.limit + 1
        file_name_list = []
        object_list = []
        try:
            for r in range(index):
                in_memory_excel = BytesIO()
                sheet_name = "{}-{} 用户订单".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
                r_df = order_df[self.limit * r:self.limit * (r + 1)]
                r_df.to_excel(in_memory_excel, sheet_name=sheet_name, index=False)
                del r_df
                in_memory_excel.seek(0)
                a = in_memory_excel.read()
                del in_memory_excel
                object_list.append(a)
                file_name_list.append("{}-{}".format(file_name, r))
                gc.collect()
        except Exception as ex:
            logger.exception(ex)
        return object_list, file_name_list

    def zip_bytes(self, object_list: list, file_name_list: list):
        """
        压缩字节流文件
        :return:
        """
        try:
            in_memory_zip = BytesIO()
            zf = zipfile.ZipFile(in_memory_zip, 'a', zipfile.ZIP_DEFLATED, False)

            for k, v in enumerate(object_list):
                zf.writestr(file_name_list[k] + ".xlsx", v)
                gc.collect()

            in_memory_zip.seek(0)
            zip_bytes = in_memory_zip.read()
            del in_memory_zip
        except Exception as ex:
            logger.exception(ex)
        logger.info("zip_bytes size: {}".format(sys.getsizeof(zip_bytes)))
        return zip_bytes

    def order_export(self, valid_data: tuple, file_name: str):
        """
        本地生成order报表压缩成zip上传到oss
        :param valid_data: valid_data
        :param file_name: 文件名
        :return:
        """

        order_df = self.get_order_df(valid_data)

        gc.collect()

        object_list, file_name_list = self.chunk_order_bytes(order_df, valid_data, file_name)

        gc.collect()

        zip_bytes = self.zip_bytes(object_list, file_name_list)

        gc.collect()

        # 上传到oss
        oss_config = cfg.get("OSSConfig")
        oss_client = AliyunOSS(oss_config)
        oss_client.put_object_bytes(file_name + ".zip", zip_bytes)

        gc.collect()

        # writer = pd.ExcelWriter('order.xlsx', engine="openpyxl")
        #
        # # order_df_list = []
        # for r in range(index):
        #     c = time.time()
        #     sheet_name = "{}-{}-{} 用户订单".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), r)
        #     r_df = order_df[self.limit * r:self.limit * (r + 1)]
        #     r_df.to_excel(writer, sheet_name=sheet_name, index=False)
        #     writer.save()
        #     writer.close()
        #     logger.info("order_export diff_3 [{}]: {}".format(r, (time.time() - c)))
        # data = [writer, r_df, sheet_name]
        # order_df_list.append(data)

        # c = time.time()
        # self.df_excel_many(order_df_list)
        # logger.info("order_export diff_3: {}".format(time.time() - c))

        # d = time.time()
        # writer.save()
        # logger.info("order_export diff_4: {}".format(time.time() - d))

        # c = time.time()
        # in_memory_order = BytesIO()
        # sheet_name = "{}-{} 用户订单".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        # logger.info(order_df.to_excel(in_memory_order, sheet_name=sheet_name, index=False))
        # in_memory_order.seek(0)
        # order_bytes = in_memory_order.read()
        # logger.info("order_export order_bytes size: {}".format(order_bytes.__sizeof__()))
        # logger.info("order_export diff_3: {}".format(time.time() - c))

        # e = time.time()
        # oss_config = cfg.get("OSSConfig")
        # oss_client = AliyunOSS(oss_config)
        # oss_client.put_object(file_name + ".xlsx", "order.xlsx")
        # logger.info("order_export diff_4: {}".format(time.time() - e))

        # 更新文件状态
        self.update_bill_record(file_name=file_name)

        # 删锁
        dao_session.redis_session.r.delete(EXPORT_EXCEL_LOCK)

    def order_export_func_request(self, valid_list: list, file_name: str):
        """
        调用函数计算生成order报表
        :param valid_list: 传递的参数
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
                function_name="dashboardReport",
                body={"valid_list": valid_list, "file_name": file_name, "type": 1}
            )
            func_client.do_http_request()
        except Exception as ex:
            logger.exception(ex)
        finally:
            logger.info("order_export_func_request finally: {}".format(time.time()))
            dao_session.redis_session.r.delete(EXPORT_EXCEL_LOCK)
