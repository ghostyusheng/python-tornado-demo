# -*- coding:utf8 -*-

from functools import wraps
from core.const import const
from time import time


class Timekeeping:
    def __init__(self):
        pass

    def __call__(self, func):
        @wraps(func)
        def wrap(*args, **kwargs):
            print('方法名:', func.__name__)
            st = time()
            res = func(*args, **kwargs)
            time_slot = int(time() - st)
            print('耗时: %d s' % time_slot)
            return res
        return wrap
