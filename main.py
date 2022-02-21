# coding:utf-8
import os.path
import sys

# 将ebike-mb-tools目录加入环境变量
import threading
import time

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

    logger.initialize(debug=cfg['debug'])
    AppInit(loop, service_name='ebike-account', dataId=['ebike_account.json', 'ebike_mb.json'])
    application = tornado.httpserver.HTTPServer(app, xheaders=True)

    YearType = ["2020", "2021", "2022"]
    MonthType = ["2021_11", "2021_12", "2022_01"]
    TenantType = ["1"]
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

    def check_tenant_type(cfg, TenantType, app, split_info):

        while True:
            if len(cfg.get("TenantType"), []) > len(TenantType):
                TenantType = cfg.get("TenantType", [])
                split_info["tenant_type"] = TenantType
                dao_session.initialize(app, split_info)
            time.sleep(5)

    testset = threading.Thread(target=check_tenant_type, args=(cfg, TenantType, app, split_info))
    testset.start()

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
