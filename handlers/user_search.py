# -*- coding: utf-8 -*-
import json
import uuid

from tornado.web import MissingArgumentError

from core.const import const
from core.entity.request import RequestEntity
from core.httpcode import HTTPCode
from function.function import DEBUG, SDEBUG, GET
from function.function import R
from handlers.base import BaseHandler
from middleware.request import RequestMiddleware
from service.cache import CacheService
from utils.utils import log_exception, syslog


class UserSearchHandler(BaseHandler):

    async def get(self):
        try:
            RequestMiddleware.verifyParams([
                ['email', ''],
            ])
            request: RequestEntity = R()

            print('___>', request.email)

            self.out()
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])
