# -*- coding:utf8 -*-

from functools import wraps
from core.const import const
from time import time


class Debug:
    def __init__(self, where):
        self.where = where

    @classmethod
    def set(cls, where):
        return cls(where)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            print('方法名:', func.__name__)
            print('入参:', args, kwargs)
            res = await func(*args, **kwargs)
            print('结果:', res)
            return res
        return wrap
