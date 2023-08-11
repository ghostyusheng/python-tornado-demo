# -*- coding: utf-8 -*-
from handlers.base import BaseHandler

class _HealthHandler(BaseHandler):

    async def get(self):
        self.out('OK')
