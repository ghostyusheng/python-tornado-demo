# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from repository.db import DBRepository


class DemoRdsHandler(BaseHandler):

    async def get(self):
        results = await DBRepository.queryAll("select * from test_user")
        print(results)
        self.out('OK')
