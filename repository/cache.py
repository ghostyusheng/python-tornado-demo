# -*- coding:utf-8 -*-
from json import dumps, loads
from typing import Union

from common.redis_conn import RedisConn
from function.function import config
from repository.base import BaseRepository
from service.config import ConfigService
from utils.utils import log_exception


class CacheRepository(BaseRepository):

    def __init__(self):
        self._redis_conn = RedisConn._redisConn()
        self._cache_ttl = int(config("redis", "cache_ttl"))

    def setTTl(self, msg):
        print("收到设置ttl的消息")
        ttl = self._cache_ttl
        if msg and msg.get('data'):
            ttl = int(msg.get('data'))
        else:
            print("收到设置ttl的消息,但是ttl不合法")
        self._cache_ttl = ttl
        ConfigService.instance().setConfig("redis","cache_ttl",str(ttl))
        return self._cache_ttl

    async def subscribeTTl(self):
        pubsub = self.getPubSub()
        await pubsub.subscribe(**{'update-ttl-channel': self.setTTl})
        thread = pubsub.run_in_thread(daemon=True)

    async def _get(self, key) -> Union[str, None]:
        try:
            result = await self._redis_conn.get(key)
        except Exception as e:
            log_exception(e)
            return None
        else:
            return result

    async def _set(self, key, value, ttl=None):
        expire_time = self._cache_ttl
        if ttl:
            expire_time = ttl
        try:
            await self._redis_conn.set(key, value, ex=expire_time)
        except Exception as e:
            log_exception(e)

    async def get(self, key) -> Union[str, None]:
        try:
            result = await self._get(key)
            if result:
                result = loads(result)
        except Exception as e:
            log_exception(e)
            return None
        else:
            return result

    async def set(self, key, val, ttl=None):
        if type(val) != str:
            val = dumps(val, ensure_ascii=False)
        try:
            await self._set(key, val, ttl)
        except Exception as e:
            log_exception(e)

    async def sadd(self, key, val):
        try:
            await self._redis_conn.sadd(key, val)
        except Exception as e:
            log_exception(e)

    async def sIsMember(self, key, val):
        try:
            result = await self._redis_conn.sismember(key, val)
        except Exception as e:
            log_exception(e)
            return None
        return result

    async def exists(self, key):
        try:
            result = await self._redis_conn.exists(key)
        except Exception as e:
            log_exception(e)
            return None
        return result

    def getPubSub(self):
        return self._redis_conn.pubsub()

    async def delete(self, key):
        try:
            pattern = "*" + key + "*"
            result = []
            cursor, batch = await self._redis_conn.scan(cursor=0, match=pattern, count=500)
            result.extend(batch)
            while cursor:
                cursor, batch = await self._redis_conn.scan(cursor=cursor, match=pattern, count=500)
                result.extend(batch)
            for key in result:
                await self._redis_conn.delete(key)
        except Exception as e:
            log_exception(e)

    async def persist(self, key):
        try:
            await self._redis_conn.persist(key)
        except Exception as e:
            log_exception(e)

    async def expire(self, key, ttl):
        try:
            await self._redis_conn.expire(key, ttl)
        except Exception as e:
            log_exception(e)

    async def incr(self, key):
        try:
            return await self._redis_conn.incr(key)
        except Exception as e:
            log_exception(e)

    async def srem(self, key, val):
        try:
            await self._redis_conn.srem(key, val)
        except Exception as e:
            log_exception(e)

    async def publish(self, channel, msg):
        try:
            await self._redis_conn.publish(channel, msg)
        except Exception as e:
            log_exception(e)
