# -*- coding:utf8 -*-
from functools import wraps
from function.context import context_var
from service.cache import CacheService
from function.function import DEBUG


class Cache:
    special_keys = {}

    def __init__(self, cache_keys, cache_time):
        self.special_cache_keys = cache_keys
        self.cache_time = cache_time

    @classmethod
    def set(cls, cache_keys=[], cache_time=None):
        return cls(cache_keys, cache_time)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            _obj = args[0]
            redis_key_args = '|'.join([str(i) for i in args[1:]])
            redis_key_kwargs = '|'.join(
                ["{}:{}".format(str(i), str(j)) for i, j in kwargs.items()])
            redis_special_key = '|'.join(["{}:{}".format(str(key), str(
                getattr(_obj, key))) for key in self.special_cache_keys])
            ab = context_var.get().BUCKET
            redis_key = "{}#{}@{}@{}@ab={}".format(
                func.__name__, redis_special_key, redis_key_args, redis_key_kwargs,ab)

            data = await CacheService.instance().get(redis_key)
            if (not DEBUG()) and (data is not None):
                res = data
            else:
                res = await func(*args, **kwargs)
                await CacheService.instance().set(redis_key, res, self.cache_time)
            return res
        return wrap
