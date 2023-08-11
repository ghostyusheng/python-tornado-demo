# -*- coding:utf8 -*-

from functools import wraps

from core.const import const
from function.context import context_var


class Apm:
    def __init__(self, where):
        self.where = where

    @classmethod
    def set(cls, where):
        return cls(where)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            if const.APM:
                const.APM.begin_transaction(self.where)
                http_message = context_var.get().HTTP_MESSAGE
                res = await func(*args, **kwargs)
                const.APM.end_transaction(self.where, http_message)
            else:
                http_message = context_var.get().HTTP_MESSAGE
                res = await func(*args, **kwargs)
            return res
        return wrap
