from mbutils import doo_redis


def lock(
        key: str,
        value: bytes = b'1',
        timeout: int = 5,
) -> bool:
    """
    尝试为某 Redis key 加锁

    :param key: 待加锁的目标 key
    :param value: 待加锁的目标 value
    :param timeout: 锁释放超时，默认 5 秒
    :return: 是否加锁成功
    """

    key = f'lock_{key}'

    locked = doo_redis.r.setnx(key, value)
    if locked:
        doo_redis.r.expire(key, timeout)

    return bool(locked)


def release_lock(
        key: str,
) -> bool:
    """
    释放为某 Redis key 加的锁

    :param key: 待释放锁的目标 key
    :return: 是否释放成功
    """

    key = f'lock_{key}'

    deleted = doo_redis.r.delete(key)
    return bool(deleted)
