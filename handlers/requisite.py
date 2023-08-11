# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from utils.keygen import Keygen
from core.const import const
from middleware.request import RequestMiddleware
from function.function import R
from time import time

class RequisiteHandler(BaseHandler):

    async def get(self):
        RequestMiddleware.verifyParams([
            ['ts', 0, 'int'],
        ])
        request: RequestEntity = R()
        ts = request.getTs()
        if int(abs(ts - time())) >= 3 or (not ts):
            self.out(code=500,msg="FAIL")
            return
        M, key = Keygen.generateCert()
        await const.cache_service.setRequisiteKey(key, key)
        self.out(M)
