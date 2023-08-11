# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from common.es_conn import EsConn
from share.repository.els import ElsRepository
from elasticsearch.client import IndicesClient


class DemoEsHandler(BaseHandler):

    async def get(self):
        es_client = EsConn()
        index_client = IndicesClient(es_client.conn)
        result = await index_client.exists('opensearch_dashboards_sample_data_flights')
        print(result)
        result = await index_client.exists('null')
        print(result)
        self.out('OK')
