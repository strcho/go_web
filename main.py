# coding:utf-8
import concurrent.futures
import os.path
import signal
import sys
import time

import swagger_ui
import tornado.ioloop
import tornado.web

# 将ebike-mb-tools目录加入环境变量
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../ebike-mb-tools/")))

from mbutils.nacos import Nacos
from setting import ConfigNacos
from utils.arguments import (
    SWAGGER_API_OUTPUT_FILE,
    generate_swagger_file,
)
from utils.constant.config import THREAD_NUM, REDIS_MAX_COLLECITONS
from utils.url_mapping import handlers
from tornado.options import options

from mbutils import logger, cfg, settings, dao_session
from mbutils.db_manager import DBManager, SubDBManager
from mbutils.middle_ware import middle_ware_list
from mbutils.redis_manager import RedisManager
from model.all_model import create_table
from scripts import register_scheduler


class Application(tornado.web.Application):
    def __init__(self):
        super(Application, self).__init__(handlers=handlers, **settings)
        redis_cfg: dict = cfg['redis_cli']
        redis_cfg.update({"redis_max_num": REDIS_MAX_COLLECITONS, "is_test_env": cfg["is_test_env"]})
        self.redis_session = RedisManager(redis_cfg)
        self.db_session = DBManager(cfg['mysql']).get_db()
        if cfg.get("sub_mysql", None):
            self.sub_db_session = SubDBManager(cfg['sub_mysql']).get_db()
        else:
            self.sub_db_session = self.db_session
        self.thread_executor = concurrent.futures.ThreadPoolExecutor(THREAD_NUM, 'xcmbServer')
        self.async_do = self.thread_executor.submit
        self.middle_ware_list = middle_ware_list


def parse_command_line():
    # 顺序:命令行 > config.py > 默认配置
    options.logging = None
    options.define("port", help="run server on a specific port", type=int)
    options.define("debug", help="level of logging", type=bool)
    options.define("env", help="env", type=str, default='dev')
    options.define("name", help="server name", type=str, default='mbServer')
    # 命令行上添加的参数项
    options.parse_command_line()
    if options.debug:
        settings.update({'debug': True, 'autoreload': True})
    else:
        settings.update({'debug': False, 'autoreload': False})
    cfg.update({k: v for k, v in options.as_dict().items() if v})
    cfg["is_test_env"] = 1 if options.env == "test" else 0
    cfg["name"] = options.name

#
# def sig_handler(sig, frame):
#     """信号处理函数
#     """
#     print("\nReceived interrupt signal: %s" % sig)
#     tornado.ioloop.IOLoop.instance().add_callback(shutdown)
#
#
# def shutdown():
#     """进程关闭处理
#     """
#     print("Stopping http server, please wait...")
#
#     # nacos 注销此实例
#     nacosServer.deletedInstance()
#
#     # 停止接受Client连接
#     io_loop = tornado.ioloop.IOLoop.instance()
#     # 设置最长等待强制结束时间
#     deadline = time.time() + 3
#
#     def stop_loop():
#         now = time.time()
#         if now < deadline:
#             io_loop.add_timeout(now + 1, stop_loop)
#         else:
#             io_loop.stop()
#
#     stop_loop()


if __name__ == "__main__":

    # signal.signal(signal.SIGTERM, sig_handler)
    # signal.signal(signal.SIGINT, sig_handler)

    parse_command_line()

    test_config = {}
    # ==========================
    # nacos 接入
    nacosServer = Nacos(ip=ConfigNacos.nacosIp, port=ConfigNacos.nacosPort)
    # 将本地配置注入到nacos对象中即可获取远程配置，并监听配置变化实时变更
    # 获取配置1：
    nacosServer.config(dataId="demo-python.json", group="dev", tenant=ConfigNacos.namespaceId,
                       myConfig=cfg)

    # 获取配置2：
    nacosServer.config(dataId="python_common", group="account", tenant=ConfigNacos.namespaceId,
                       myConfig=test_config)
    # 配置服务注册的参数
    nacosServer.registerService(serviceIp=ConfigNacos.ip, servicePort=ConfigNacos.port, serviceName="ebike-assets",
                                namespaceId=ConfigNacos.namespaceId, groupName="dev", metadata={"test": 1024})
    # 开启监听配置的线程和服务注册心跳进程的健康检查进程
    nacosServer.healthyCheck()
    # ========================

    logger.initialize(server_name=cfg["name"], debug=cfg['debug'])
    app = Application()
    dao_session.initialize(app)
    create_table()
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
