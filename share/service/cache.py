# -*- coding:utf-8 -*-
from typing import List

from share.repository.cache import CacheRepository
from share.service.base import BaseService
from json import loads


class CacheService(BaseService):
    cache_repository = None

    def __init__(self, host, port, password, _cluster=False):
        self.cache_repository = CacheRepository.instance()
        self.cache_repository._redisConn(host, port, password, _cluster)


    @staticmethod
    def buildCacheKey(entity_src, action, entity_dest='', params={}, perment=False, strategy=''):
        cache_key = entity_src + ":" + action + ":" + entity_dest
        if params and (type(params) == dict):
            cache_key = cache_key + ":" + "|".join(str(k) + "@" + str(v) for k, v in params.items())
        elif params:
            cache_key = cache_key + ":" + str(params)
        if strategy:
            cache_key = strategy + ':' + cache_key
        if perment:
            cache_key = "P:" + cache_key
        else:
            cache_key = "C:" + cache_key
        return cache_key

    async def getHotNftRec(self, params=""):
        key = self.buildCacheKey(
            entity_src='nft',
            action='hot',
            entity_dest='nft',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)


    async def setHotNftRec(self, value, params=""):
        key = self.buildCacheKey(
            entity_src='nft',
            action='hot',
            entity_dest='nft',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 1)


    async def setRequisiteKey(self, value, params=""):
        key = self.buildCacheKey(
            entity_src='requisite',
            action='temp',
            entity_dest='key',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 5)

    async def getRequisiteKey(self, params=""):
        key = self.buildCacheKey(
            entity_src='requisite',
            action='temp',
            entity_dest='key',
            params=params,
            perment=False
        )
        return await self.cache_repository.getString(key)
