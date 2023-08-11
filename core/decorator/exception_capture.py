# -*- coding:utf8 -*-
from functools import wraps
from function.context import context_var
from service.cache import CacheService
from tornado.web import MissingArgumentError
from utils.utils import log_exception, syslog
from core.httpcode import HTTPCode


class ExceptionCapture:

    def __init__(self, ):
        pass

    @classmethod
    def set(cls, cache_keys=[]):
        return cls(cache_keys)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            try: 
                res = await func(*args, **kwargs)
                return res
            except MissingArgumentError as e:
                syslog(e.log_message)
                _handler = args[0]
                _handler.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
            except Exception as e:
                _handler = args[0]
                log_exception(e.args[0])
                _handler.out(msg=e.args[0])
        return wrap
