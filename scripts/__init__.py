from datetime import datetime as dt, timedelta
from mbutils import AGENT_NAME, dao_session
from utils.schedule import TimeTask, JOBS_KEY, RUN_TIMES_KEY
from .payment_query import Payment

"""
    由于pickle的原因,不能用中间匿名的函数,或者参数里面有函数等无法序列化的场景
"""
RANDOM_MINUTE = hash(AGENT_NAME) % 60

AGENT_NAME_LIST_SCRIPT = []


def query_payment_order():
    Payment().query_payment_order()


def kafka_sub_worker_job():
    pass


def register_scheduler(loop):
    """
        由于pickle的原因,不能用中间匿名的函数,或者参数里面有函数等无法序列化的场景
    """

    def add_time(t: str):
        """解决小安数据库所有客户的计算任务同时进行导致数据库查询压力太大"""
        return (dt.strptime(t, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=RANDOM_MINUTE)).strftime(
            "%Y-%m-%d %H:%M:%S")

    # FBI warnning: 正式服和测试服环境没有区分数据库,而处理脚本是一样的,有些场景可能有问题
    dao_session.redis_session.r.delete(JOBS_KEY)  # 防止发版的时候，任务乱掉导致任务不触发
    dao_session.redis_session.r.delete(RUN_TIMES_KEY)
    tt = TimeTask(loop)

    # 只运行一次的长脚本
    tt.scheduler.add_job(kafka_sub_worker_job, 'date', replace_existing=True,
                         next_run_time=dt.now() + timedelta(seconds=10), max_instances=1, misfire_grace_time=15)

    # -------------------------------- 支付主动查询 ------------------------------------------------
    tt.scheduler.add_job(query_payment_order, 'interval', replace_existing=True, minutes=10,
                         start_date=add_time('2021-01-01 00:00:00'))  # 支付订单主动查询

    tt.start_tasks()
