# coding:utf-8
import os.path
import sys

# 将ebike-mb-tools目录加入环境变量
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../ebike-mb-tools/")))

from mbutils.app_start_init import AppInit
import tornado.ioloop
import tornado.web

from utils.url_mapping import handlers

from mbutils import (
    logger,
    cfg,
    settings,
    dao_session,
)

from mbutils.middle_ware import middle_ware_list
from scripts import register_scheduler


class Application(tornado.web.Application):
    def __init__(self):
        super(Application, self).__init__(handlers=handlers, **settings)
        self.middle_ware_list = middle_ware_list


if __name__ == "__main__":
    loop = tornado.ioloop.IOLoop.current()
    app = Application()
    AppInit(loop, service_name='ebike-account', dataId=['ebike_account.json'])

    logger.initialize(server_name=cfg["name"], debug=cfg['debug'])
    application = tornado.httpserver.HTTPServer(app, xheaders=True)

    YearType = ["2020", "2021", "2022"]
    MonthType = ["2021_11", "2021_12", "2022_01"]
    TenantType = ["dianlv", "qiyue", "qiyiqi", "chudu", "1", "2", "xiaoantech_dev"]
    from model.all_model import *

    split_info = {
        "tenant_models": [TRidingCard, TDepositCard, TFavorableCard, TDiscountsUser, TFreeOrderUser, TUserWallet],
        "year_models": [],
        "month_models": [],
        "unsplit_models": [],
        "month_type": MonthType,
        "year_type": YearType,
        "tenant_type": TenantType
    }
    dao_session.initialize(app, split_info)

    app.listen(cfg['port'])
    logger.debug('listen to {} port,env: {}'.format(cfg['port'], cfg['is_test_env']))
    cfg['app'] = app

    if cfg["doc"]:
        from mbutils.autodoc import generate_swagger_file, swagger_api_doc

        generate_swagger_file(handlers=handlers, serviceName='ebike-account')
        swagger_api_doc(app)

    loop = tornado.ioloop.IOLoop.current()
    register_scheduler(loop)
    loop.start()
