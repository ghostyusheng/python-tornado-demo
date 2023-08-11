# -*- coding:utf-8 -*-
from function.function import config
from elasticsearch import ConflictError, NotFoundError
from elasticsearch_async import AsyncElasticsearch
from utils.utils import syslog, index_exception


class EsConn:
    _instance = None

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.conn = AsyncElasticsearch(
            config("elasticsearch", "es_conn").split(','))

    async def delete(self, index, _id):
        try:
            await self.conn.delete(index, _id)
        except NotFoundError as e:
            syslog("+++++++++++++++++++++> {} DELETE FAILED!".format(_id))

    async def update(self, index, _id, typ, data):
        try:
            await self.conn.update(index, _id, typ, data, retry_on_conflict=3)
        except ConflictError as e:
            syslog("+++++++++++++++++++++> {} DOC VERSION CONFLICT!".format(_id))
        except Exception as e:
            index_exception(e)

    async def search(self, *args, **kwargs):
        preference = kwargs.get("preference")
        if preference == "":
            kwargs.pop('preference')
        return await self.conn.search(*args, **kwargs)

    async def get(self, *args, **kwargs):
        try:
            return await self.conn.get(*args, **kwargs)
        except NotFoundError as e:
            index_exception(e)

    async def bulk(self, body, type, index):
        await self.conn.bulk(body, type, index)
