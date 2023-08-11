# -*- coding: utf-8 -*-
import json
import uuid

from tornado.web import MissingArgumentError

from core.const import const
from core.decorator.log import Apm
from core.entity.request import RequestEntity
from core.httpcode import HTTPCode
from function.function import DEBUG, SDEBUG, GET
from function.function import R
from handlers.base import BaseHandler
from middleware.request import RequestMiddleware
from middleware.user import UserMiddleware
from service.app import AppService
from service.cache import CacheService
from utils.redis_key_tool import build_redis_key
from utils.utils import log_exception, syslog


class AppSearchHandler(BaseHandler):

    @Apm.set('app-search')
    async def get(self):
        try:
            await UserMiddleware.checkUserId(self)
            RequestMiddleware.verifyParams([
                ['email'],
            ])
            request: RequestEntity = R()
            booster = request.getBooster()

            cache_params = {
                'keyword': request.getKeyword(),
                'from': request.getFrom(),
                'limit': request.getLimit(),
                'store': request.getStore(),
                'platform': request.getPlatform(),
                'sort': request.getSort(),
                'booster': request.getBooster()
            }

            data = await CacheService.instance().getAppSearchResult(cache_params)

            if data is None:
                session_id = GET('session_id')
                if session_id == '':
                    session_id = const.HOSTNAME + str(uuid.uuid1())
                    request.setSessionId(session_id)
                kw = request.getKeyword()[:32]
                request.setKeyword(kw)
                if booster == '1':
                    result, total = await AppService().instance().searchAppWithBooster(request)
                else:
                    result, total = await AppService().instance().searchApp(request)
                data = {
                    'list': result,
                    'total': total,
                    'sessionId': session_id
                }
                await CacheService.instance().setAppSearchResult(cache_params, data)
                if DEBUG():
                    data['sdebug'] = SDEBUG()

            self.out(data)
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])
