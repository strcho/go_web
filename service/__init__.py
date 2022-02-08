import datetime
import json
import random
import string

from mbutils import DefaultMaker
from mbutils import dao_session, MbException, ARG_DEFAULT
from mbutils.snowflake import ID_Worker


class MBService:
    @staticmethod
    def create_trade_number():
        """创建流水号"""
        return datetime.datetime.now().strftime("%Y%m%d") + ''.join(random.choices(string.digits, k=24))

    @staticmethod
    def exists_param(p):
        """参数是否存在"""
        return not isinstance(p, DefaultMaker)

    @staticmethod
    def get_or_default(d: dict, key: str):
        return d[key] if key in d else ARG_DEFAULT

    @staticmethod
    def remove_empty_param(params: dict):
        """
        移除字典中的value为默认参数类型的参数，只保留用户传入过参数的数据
        :param params:
        """
        return {k: v for k, v in params.items() if not isinstance(v, DefaultMaker)}

    @staticmethod
    def num2datetime(num):
        """时间戳转成datetime"""
        return datetime.datetime.fromtimestamp(int(num))

    @staticmethod
    def millisecond2datetime(num):
        """时间戳转成datetime"""
        return datetime.datetime.fromtimestamp(int(num) / 1000)

    @staticmethod
    def datetime2num(dt: datetime.datetime):
        """datetime转化成时间戳"""
        return dt and int(dt.timestamp())

    @staticmethod
    def nx_lock(key, *args, timeout=1, promt=""):
        """
        互斥锁,兼容key.format(*args)在函数内外两种方式
        :param key:
        :param timeout: 过期时间
        :return:
        """
        if len(args) >= 0:
            key = key.format(*args)
        try:
            res = dao_session.redis_session.r.set(key, 1, ex=timeout, nx=True) # 返回None或者True
        except Exception:
            res = None
        if not res:
            raise MbException(promt=(promt or "Concurrent operation {}".format(key)))

    @staticmethod
    def del_lock(key):
        dao_session.redis_session.r.delete(key)

    @staticmethod
    def get_user_name(person_info: str, default=''):
        """
        根据user表中的person_info获取用户名称
        """
        try:
            return json.loads(person_info)["name"]
        except Exception:
            return default

    @staticmethod
    def double_storage_4_str(redis_key, sql_query, expire=60) -> str:
        """
            redis-mysql两级存储
            如果redis存在redis取,
            如果redis不存在则在数据库存储,数据库不存在则报错,
            如果数据库存在则返回并存入redis里面后面不用查询
        :param redis_key:
        :param query: 查询数量的语句
        :param expire: redis过期时间, 默认60s
        :return:
        """

        res = dao_session.redis_session.r.get(redis_key)
        if not res:
            res = sql_query.scalar() or 0
            dao_session.redis_session.r.set(redis_key, res, ex=expire)
        return res

    @staticmethod
    def double_storage_4_hash(redis_key, query, serialize_func, expire=60) -> dict:
        """
        同double_storage_4_str
        """
        res = dao_session.redis_session.r.hgetall(redis_key)
        if not res:
            res = query.first()
            if not res:
                raise MbException("数据库查询不到数据")
            else:
                res = serialize_func(res)
            dao_session.redis_session.r.hset(redis_key, mapping=res)
            dao_session.redis_session.r.expire(redis_key, expire)
        return res

    @staticmethod
    def get_cfg_redis_key(agent_id, router, service_id):
        if service_id:
            return "{}_config_{}_{}".format(agent_id, router, service_id)
        else:
            return "{}_config_{}".format(agent_id, router)

    @staticmethod
    def get_today_date():
        now = datetime.datetime.now()
        zero_today = now - datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second,
                                              microseconds=now.microsecond)
        last_today = zero_today + datetime.timedelta(hours=23, minutes=59, seconds=59)
        return zero_today, last_today

    @staticmethod
    def get_number_price(number):
        return '%.2f' % (number / 100 if number else 0)

    @staticmethod
    def millisecond2datetime(num):
        """时间戳转成datetime"""
        return datetime.datetime.fromtimestamp(int(num) / 1000)

    @staticmethod
    def get_model_common_field(commandContext: dict = None):
        """
        获取模型公共字段字典
        """
        fields_dict = dict(
            id=ID_Worker(),
            tenant_id=None,
            created_at=datetime.datetime.now(),
            created_pin=None,
            # updated_at=
            updated_pin=None,
            # version=
            # iz_del=
        )

        if commandContext:
            fields_dict["tenant_id"] = commandContext.get('tenantId')
            fields_dict["created_pin"] = commandContext.get('pin')
            fields_dict["updated_pin"] = commandContext.get('pin')

        return fields_dict
