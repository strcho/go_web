import datetime

from apscheduler.schedulers.tornado import TornadoScheduler

from mbutils import AGENT_NAME, compute_name, MbException, dao_session, cfg, logger
from mb3party.ding_talk import ding_talk
from service import MBService
from utils.constant.redis_key import SCHEDULE_JOB

JOBS_KEY = compute_name + ('testapscheduler.jobs' if cfg["is_test_env"] == 1 else 'apscheduler.jobs')
RUN_TIMES_KEY = compute_name + ('testapscheduler.run_times' if cfg["is_test_env"] == 1 else 'apscheduler.run_times')


class TimeTask(object):

    def __init__(self, loop):
        try:
            self.scheduler = TornadoScheduler(io_loop=loop, logger=logger)
            # 它会创建默认大小为10的线程池，apscheduler.executors.pool的ThreadPoolExecutor
            self.scheduler.add_jobstore("redis", host=cfg['redis_cli']['host'], port=cfg['redis_cli']['port'],
                                        password=cfg['redis_cli']['pwd'],
                                        db=cfg['redis_cli'].get('db', 0) + cfg["is_test_env"],
                                        jobs_key=JOBS_KEY,
                                        run_times_key=RUN_TIMES_KEY)
        except Exception as e:
            logger.info("this job is error:{}".format(e))

        # 在 2019-08-14 01:00:00 以后, 每隔一天执行一次 job_func 方法
        # scheduler.add_job(func, 'interval', days=1, start_date='2019-08-14 01:00:00', jitter=120)

    def start_tasks(self):
        self.scheduler.start()


def prevent_concurrency(index_name: str, timeout=3 * 60):
    """防止脚本在多个机器上并发"""
    MONITOR_INDEX_NAME = ["BigScreenMerchants.merchant_script"]
    DING_TALK_URL = 'https://oapi.dingtalk.com/robot/send?access_token=b0f555ed55bfeb29b641845f8861d67dc8551482f3700256d23ad6066635f9b7'
    DEVELOPS = ["15172474706"]

    def mid(f):
        def wrapper(*args, **kwargs):
            res = ''
            try:
                logger.info("enter:{}".format(index_name))
                MBService.nx_lock(SCHEDULE_JOB, index_name, timeout=timeout)
                res = 'enter'
                res = f(*args, **kwargs)
                dao_session.session().commit()  # 有事物忘记提交,先兜底
                logger.info("exit:{}".format(index_name))
                return res
            except Exception as ex:
                dao_session.session().rollback()
                dao_session.session().close()
                dao_session.sub_session().close()
                if not isinstance(ex, MbException):
                    logger.info("index_name:", index_name, ex)
                else:
                    pass
            finally:
                #  当脚本执行失败时，发送消息到群里（后期如果还有需要，可以考虑将方法抽出）
                if index_name in MONITOR_INDEX_NAME and res == 'enter':
                    logger.info("index_name", index_name, "is failed")
                    index_name_data = ("""### **脚本执行失败** *{index_name}* \n"""
                                       + """> 客户的名称是: ***{agent_name}*** \n"""
                                       + """> ###### {time_str} \n""").format(
                        index_name=index_name, agent_name=AGENT_NAME,
                        time_str=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    title = "{}执行失败".format(index_name)
                    ding_talk(DING_TALK_URL, title, index_name_data, DEVELOPS)

        return wrapper

    return mid
