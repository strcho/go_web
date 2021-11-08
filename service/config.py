import json
import time
from model.all_model import *
from mbutils import dao_session
from utils.constant.config import ConfigName
from utils.constant.redis_key import CONFIG_ROUTER_KEY, CONFIG_ROUTER_SERVICE_KEY
from . import MBService
from mbutils import logger


class ConfigService(MBService):
    def get_router_content(self, router: str, service_id):
        """agentId没有用，serviceId有用"""
        content = {}
        find_config = dao_session.redis_session.r.hgetall(CONFIG_ROUTER_SERVICE_KEY.
                                                          format(router=router, serviceId=service_id)) or \
                      dao_session.redis_session.r.hgetall(CONFIG_ROUTER_KEY.format(router=router))
        if find_config:
            try:
                content = json.loads(find_config["content"])
            except Exception:
                content = find_config["content"]
        else:
            db_config = dao_session.session().query(XcEbike2Config).filter_by(serviceId=service_id,
                                                                              rootRouter=router).first()
            if db_config:
                content = json.loads(db_config.content)
                dao_session.redis_session.r.hset(CONFIG_ROUTER_SERVICE_KEY.format(router=router, serviceId=service_id),
                                                 mapping={"content": db_config.content,
                                                          "version": datetime.now().timestamp()})
            else:
                db_config = dao_session.session().query(XcEbike2Config).filter_by(serviceId=None,
                                                                                  rootRouter=router).first()
                if db_config:
                    content = json.loads(db_config.content)
                    dao_session.redis_session.r.hset(CONFIG_ROUTER_SERVICE_KEY.format(
                        router=router, serviceId=service_id),
                        mapping={"content": db_config.content, "version": datetime.now().timestamp()})
        return content

    def set_router_content(self, router: str, router_config: str, service_id):
        dao_session.redis_session.r.hset(CONFIG_ROUTER_SERVICE_KEY.format(router=router, serviceId=service_id),
                                         mapping={"content": router_config, "version": datetime.now().timestamp()})


        params = {
            "rootRouter": router,
            "content": router_config,
            "version": time.time(),
            "serviceId": service_id,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
        rowcount = dao_session.session().query(XcEbike2Config).filter_by(rootRouter=router, serviceId=service_id).update(params)
        dao_session.session().commit()
        if not rowcount:
            # 没有更新到
            config = XcEbike2Config(**params)
            dao_session.session().add(config)
            dao_session.session().commit()
