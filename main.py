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

    from model.all_model import *

    YearType = []
    MonthType = []
    TenantType = []

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

    def check_segmentation_type(app, split_info):

        while True:
            try:
                is_changed = False
                new_year_type = cfg.get("YearType", [])
                new_month_type = cfg.get("MonthType", [])
                new_tenant_type = cfg.get("TenantType", [])

                print(new_year_type, new_month_type, new_tenant_type)

                year_add = set(new_year_type) - set(split_info.get("year_type"))
                if year_add:
                    is_changed = True
                    split_info.get("year_type").extend(year_add)

                month_add = set(new_month_type) - set(split_info.get("month_type"))
                if month_add:
                    is_changed = True
                    split_info.get("month_type").extend(month_add)

                tenant_add = set(new_tenant_type) - set(split_info.get("tenant_type"))
                if tenant_add:
                    is_changed = True
                    split_info.get("tenant_type").extend(tenant_add)

                if is_changed:
                    dao_session.initialize(app, split_info)
            except Exception as e:
                print(e)
                pass
            finally:
                is_changed = False
            time.sleep(5)

    t_check_segmentation = threading.Thread(target=check_segmentation_type, args=(app, split_info))
    t_check_segmentation.start()

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
