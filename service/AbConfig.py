#!/usr/bin/env python
# -*- coding:utf-8 -*-
from elasticsearch_async import AsyncElasticsearch
from common.abt_cache import AbtCache
from common.mongo_conn import MongoConn
from function.function import config
from service.DevelopingBetaAppService import DevelopingBetaAppService
from service.base import BaseService
from service.cache import CacheService


class AbConfigService(BaseService):

    def __init__(self):
        self._buildCol()

    def _buildCol(self):
        self.conn = MongoConn._mongoconn()
        self.db = self.conn.engine
        self.abt = self.db.abt
        self.es_client = AsyncElasticsearch(
            config("elasticsearch", "es_conn").split(','))

    async def reloadConfig(self):
        AbtCache.experCache['cache'] = await self.loadConfig()
        return AbtCache.experCache['cache']

    def resetCache(self,msg):
        print("收到重置abtest配置的消息",msg)
        if AbtCache.experCache.get('cache'):
            AbtCache.experCache.pop('cache')

    async def subscribeAbConfig(self):
        pubsub = CacheService.instance().getPubSub()
        await pubsub.subscribe(**{'update-abconfig-channel': self.resetCache})
        thread = pubsub.run_in_thread(daemon=True)

    async def searchAll(self):
        if 'cache' not in AbtCache.experCache:
            AbtCache.experCache['cache'] = await self.loadConfig()
        return AbtCache.experCache['cache']

    async def loadConfig(self):
        doc_list = []
        async for document in self.abt.find():
            if document.get('buckets'):
                document['buckets'] = [
                    int(bucket) for bucket in document.get("buckets").split(",")]
            if document.get('gray_uids'):
                document['gray_uids'] = [
                    int(bucket) for bucket in document.get("gray_uids").split(",")]
            doc_list.append(document)
        return doc_list
