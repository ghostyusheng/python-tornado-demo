# -*- coding:utf8 -*-

from functools import wraps

from core.const import const
from function.context import context_var


class ColumnCopy:
    def __init__(self, new_field,from_field):
        self.new_field = new_field
        self.from_field = from_field

    @classmethod
    def copy(cls, new_field,from_field):
        return cls(new_field,from_field)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            res = await func(*args, **kwargs)
            if res is None:
                res = []
            nres = []
            for i in res:
                tmp = i
                tmp[self.new_field] = i[self.from_field]
                nres.append(i)
            return nres
        return wrap

