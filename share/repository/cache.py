# -*- coding:utf-8 -*-
from typing import Union

from aredis import StrictRedis, StrictRedisCluster

from share.repository.base import BaseRepository
from json import loads,dumps


class CacheRepository(BaseRepository):
    _instance = None
    _redis_conn = None
    _cache_ttl = None


    @staticmethod
    def _redisConn(host, port, password, _cluster=False):
        if CacheRepository._redis_conn is None:
            if _cluster:
                CacheRepository._redis_conn = StrictRedisCluster(host=host,port=port, max_connections=128,skip_full_coverage_check=True)
            else:
                CacheRepository._redis_conn = StrictRedis(host=host, port=port, max_connections=50, password=password, decode_responses=True)
        return CacheRepository._redis_conn


    async def _get(self, key) -> Union[str, None]:
        result = await self._redis_conn.get(key)
        return result

    async def _set(self, key, value, ttl=None):
        expire_time = self._cache_ttl
        if ttl:
            expire_time = ttl
        await self._redis_conn.set(key, value, ex=expire_time)

    async def get(self, key) -> Union[str, None]:
        result = await self._get(key)
        if result:
            result = loads(result)
        return result

    async def getString(self, key) -> Union[str, None]:
        result = await self._get(key)
        return result

    async def set(self, key, val, ttl=None):
        if type(val) != str:
            val = dumps(val, ensure_ascii=False)
        await self._set(key, val, ttl)

    async def hget(self, hash_key, key) -> Union[str, None]:
        result = await self._redis_conn.hget(hash_key, key)
        return result

    async def hgetall(self, hash_key) -> dict:
        result = await self._redis_conn.hgetall(hash_key)
        return result

    async def hset(self, hash_key, key, val):
        await self._redis_conn.hset(hash_key, key, val)

    async def hmget(self, hash_key, keys):
        result = await self._redis_conn.hmget(hash_key, keys)
        return result

    async def hmgetall(self, key):
        result = await self._redis_conn.hgetall(key)
        return result

    async def hmset(self, hash_key, val):
        await self._redis_conn.hmset(hash_key, val)

    async def sadd(self, key, val):
        await self._redis_conn.sadd(key, val)

    async def sIsMember(self, key, val):
        result = await self._redis_conn.sismember(key, val)
        return result

    async def exists(self, key):
        result = await self._redis_conn.exists(key)
        return result

    async def expire(self, key, ttl):
        await self._redis_conn.expire(key, ttl)

    async def delete(self, key):
        await self._redis_conn.delete(key)

    async def smembers(self, key):
        return await self._redis_conn.smembers(key)

    async def hdel(self, hash_key, key):
        return await self._redis_conn.hdel(hash_key, key)

    def getPubSub(self):
        return self._redis_conn.pubsub()

    async def persist(self, key):
        await self._redis_conn.persist(key)

    async def incr(self, key):
        return await self._redis_conn.incr(key)

    async def srem(self, key, val):
        await self._redis_conn.srem(key, val)

    async def publish(self, channel, msg):
        await self._redis_conn.publish(channel, msg)

    async def scan(self, cursor, pattern, count):
        return await self._redis_conn.scan(cursor=cursor, match=pattern, count=count)


