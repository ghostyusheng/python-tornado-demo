# -*- coding:utf-8 -*-

from common.mongo_conn import MongoConn
from repository.base import BaseRepository


class MongoRepository(BaseRepository):

    def __init__(self):
        self._buildConn()


    def _buildConn(self):
        self.conn = MongoConn()._mongoconn()


    async def queryOne(self, db: str, collection: str, msql: dict, sort=None):
        if sort:
            doc = await self.conn[db][collection].find_one(msql, sort=sort)
        else:
            doc = await self.conn[db][collection].find_one(msql)
        if doc:
            doc['_id'] = str(doc['_id'])
        return doc


    async def queryAll(self, db: str, collection: str, msql: dict, sort=None):
        if sort:
            cursor = self.conn[db][collection].find(msql, sort=sort)
        else:
            cursor = self.conn[db][collection].find(msql)
        lst = []
        async for i in cursor:
            i['_id'] = str(i['_id'])
            lst.append(i)
        return lst

    async def queryAllDoc(self, db: str, collection: str, msql: dict, sort=None):
        if sort:
            cursor = self.conn[db][collection].find(msql, sort=sort)
        else:
            cursor = self.conn[db][collection].find(msql)
        lst = []
        async for i in cursor:
            lst.append(i)
        return lst

