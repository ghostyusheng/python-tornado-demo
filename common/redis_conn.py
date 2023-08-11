# -*- coding:utf-8 -*-
from aredis import StrictRedisCluster, StrictRedis

from core.const import const
from function.function import config


class RedisConn:
    # redis cluster 连接对象
    __redis_client = None

    @staticmethod
    def _redisConn():
        if RedisConn.__redis_client is None:
            if (const.ENV == 'product') or (const.ENV == 'hkproduct'):
                startup_nodes = [
                    {"host": config("redis", "redis_host"), "port": config("redis", "redis_port")}]
                RedisConn.__redis_client = StrictRedisCluster(startup_nodes=startup_nodes, max_connections=1024,
                                                              password=config("redis", "redis_password"))
            else:
                RedisConn.__redis_client = StrictRedis(host=config("redis", "redis_host"),
                                                       port=config("redis", "redis_port"))
        return RedisConn.__redis_client
