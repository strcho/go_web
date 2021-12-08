import json

import jpush
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from jpush import common

from model.all_model import *
from utils.constant.config import ConfigName, MsgType, MsgMethod
from . import MBService


class MsgService(MBService):

    def get_template_msg(self, name: str, service_id: int):
        return None

    def translate_template_msg(self, content, params):
        """
        模板和数据组装起来
        :param content: "尊敬的用户${username}，由于您使用车辆编号为${carId}过程中有${reportType}违规操作，现对您处以冻结押金处罚，如有问题可致电客服电话：${telephone}。"
        :param params: {"username":username, "carId":111, "reportType":"11', 'telephone':15827}
        :return: str
        """
        return content.replace("$", "").format(**params)

    # def send_msg(self, user_id, tp: MsgType, params, remind):
    #     """
    #     发送通用信息的接口
    #     :param user_id:用户id
    #     :param tp: MsgType枚举值
    #     :param params: app,sms,system 三个模板里面的参数集合
    #     :param remind: 提醒渠道,0为系统通知 1为APP推送 2为短信， []拼接的字符串
    #     :return:
    #     """
    #     tp = str(tp.value)
    #     remind_list = json.loads(remind)
    #     if str(MsgMethod.SYS.value) in remind_list and str(MsgMethod.APP.value) not in remind_list:
    #         template_msg = self.get_template_msg(ConfigName.SYSTEMMSG.value, 2)
    #         if template_msg:
    #             translate_template_msg = self.translate_template_msg(template_msg[tp]['content'], params)
    #             # 插入消息表
    #             obj_params = {
    #                 "userId": user_id,
    #                 "title": template_msg[tp]['title'],
    #                 "content": translate_template_msg,
    #                 "messageType": MsgMethod.SYS.value,
    #                 "isRead": 0,
    #                 "createAt": datetime.now()
    #             }
    #             msg_obj = XcEbike2Message(**obj_params)
    #             dao_session.session().add(msg_obj)
    #             dao_session.session().commit()
    #     if str(MsgMethod.SMS.value) in remind_list:
    #         template_msg = self.get_template_msg(ConfigName.SMSMSG.value, 2)
    #         if "phone" not in params:
    #             raise MbException("send {} sms with no telephone params".format(user_id))
    #         if template_msg:
    #             sms_params = {
    #                 "PhoneNumbers": params["phone"],
    #                 "SignName": template_msg[tp]['title'],
    #                 "TemplateCode": template_msg[tp]['code'],
    #                 "TemplateParam": json.dumps(params)
    #             }
    #             self.send_sms(sms_params)
    #     if str(MsgMethod.APP.value) in remind_list:
    #         template_msg = self.get_template_msg(ConfigName.APPMSG.value, 2)
    #         if template_msg:
    #             translate_template_msg = self.translate_template_msg(template_msg[tp]['content'], params)
    #             # 插入表，返回id
    #             obj_params = {
    #                 "userId": user_id,
    #                 "title": template_msg[tp]['title'],
    #                 "content": translate_template_msg,
    #                 "messageType": MsgMethod.SYS.value,
    #                 "isRead": 0,
    #                 "createAt": datetime.now()
    #             }
    #             msg_obj = XcEbike2Message(**obj_params)
    #             dao_session.session().add(msg_obj)
    #             dao_session.session().commit()
    #             self.send_jiguang(user_id, translate_template_msg, {"sneakId": msg_obj.id})

    # @staticmethod
    # def send_sms(params):
    #     """
    #     阿里云短信接口
    #     :param params:短信参数
    #     {
    #             "PhoneNumbers": "",
    #             "SignName": "",
    #             "TemplateCode": "配置的模板编号",
    #             "TemplateParam": "模板参数"
    #         }
    #     :return:
    #     """
    #     client = AcsClient(cfg["aliyunSMS"]["accessKeyId"], cfg["aliyunSMS"]["accessKeySecret"],
    #                        cfg["aliyunSMS"]["regionId"])
    #
    #     request = CommonRequest()
    #     request.set_accept_format('json')
    #     request.set_domain('dysmsapi.aliyuncs.com')
    #     request.set_method('POST')
    #     request.set_protocol_type('https')  # https | http
    #     request.set_version(cfg["aliyunSMS"]["apiVersion"])
    #     request.set_action_name(cfg["aliyunSMS"]["action"])
    #
    #     request.add_query_param('RegionId', cfg["aliyunSMS"]["regionId"])
    #     request.add_query_param('PhoneNumbers', params["PhoneNumbers"])
    #     request.add_query_param('SignName', params["SignName"])
    #     request.add_query_param('TemplateCode', params["TemplateCode"])
    #     request.add_query_param('TemplateParam', params["TemplateParam"])
    #     try:
    #         response = client.do_action(request)
    #     except Exception:
    #         logger.error("sms send failure", params)

    # @staticmethod
    # def send_jiguang(user_id, msg, extra, app_type=0):
    #     """
    #     极光推送
    #     :param user_id:用户id
    #     :param msg: 消息内容
    #     :param extra: 额外携带参数，供app调转
    #     :param app_type: 0用户端，1商家端
    #     :return:
    #     """
    #     if app_type == 0:
    #         app_key = cfg["jpushMsg"]["appkey"]
    #         master_secret = cfg["jpushMsg"]["masterSecret"]
    #         platform_name = cfg["jpushMsg"]["platformName"]
    #     else:
    #         app_key = cfg["jpushBusinessMsg"]["appkey"]
    #         master_secret = cfg["jpushBusinessMsg"]["masterSecret"]
    #         platform_name = cfg["jpushBusinessMsg"]["platformName"]
    #
    #     _jpush = jpush.JPush(app_key, master_secret)
    #     client = _jpush.create_push()
    #     # if you set the logging level to "DEBUG",it will show the debug logging.
    #     _jpush.set_logging("ERROR")
    #     client.audience = jpush.alias(user_id)
    #     client.notification = jpush.notification(
    #         ios=jpush.ios(alert=msg, sound='sound', badge=1, content_available=True, extras=extra),
    #         android=jpush.android(msg, platform_name, 1, extra))
    #     client.platform = jpush.all_
    #
    #     try:
    #         client.send()
    #     except common.Unauthorized:
    #         raise common.Unauthorized("Unauthorized")
    #     except common.APIConnectionException:
    #         raise common.APIConnectionException("conn error")
    #     except common.JPushFailure:
    #         logger.error("jiguang push failure", msg)
    #     except:
    #         logger.error("jiguang push exception", msg)
