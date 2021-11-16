# coding:utf-8
import concurrent.futures
import os.path
import signal
import sys

# 将ebike-mb-tools目录加入环境变量
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../ebike-mb-tools/")))

import time

from mbutils.app_start_init import AppInit
import swagger_ui
import tornado.ioloop
import tornado.web

from mbutils.autodoc import (
    generate_swagger_file,
    SWAGGER_API_OUTPUT_FILE,
)

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
    TenantType = ["dianlv", "qiyue", "qiyiqi", "chudu"]
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

    if cfg['debug']:
        generate_swagger_file(handlers=handlers, file_location=SWAGGER_API_OUTPUT_FILE)
        print(handlers)
        # Start the Swagger UI. Automatically generated swagger.json can also
        # be served using a separate Swagger-service.
        swagger_ui.tornado_api_doc(
            app,
            config_path=SWAGGER_API_OUTPUT_FILE,
            url_prefix="/swagger/spec.html",
            title="EbikePay API",
        )

    loop = tornado.ioloop.IOLoop.current()
    register_scheduler(loop)
    loop.start()
