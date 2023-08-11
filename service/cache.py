# -*- coding:utf-8 -*-
from typing import List

from function.function import config
from service.base import BaseService
from service.config import ConfigService
from share.repository.cache import CacheRepository
from utils.redis_key_tool import buildBucketParams
from core.const import const


class CacheService(BaseService):
    cache_repository = None

    def __init__(self, host=None, port=None, password=None):
        if not host:
            host = config("redis", "redis_host")
        if not port:
            port = config("redis", "redis_port")
        if not password:
            password = config("redis", "redis_password")
        self.cache_repository = CacheRepository.instance()
        if const.ENV == 'product':
            self.cache_repository._redisConn(host, port, password, True)
        else:
            self.cache_repository._redisConn(host, port, password)
        self._cache_ttl = int(config("redis", "cache_ttl"))


    @staticmethod
    def buildCacheKey(entity_src, action, entity_dest='', params={}, perment=False, strategy=''):
        cache_key = entity_src + ":" + action + ":" + entity_dest
        if params and (type(params) == dict):
            params = buildBucketParams(params)
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

    async def getSquenceRelatedApps(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='squence_related',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setSquenceRelatedApps(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='squence_related',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getFmRecallApps(self, params):
        key = self.buildCacheKey(
                entity_src='app',
                action='fm_vec',
                entity_dest='apps',
                params=params,
                perment=False
        )
        return await self.cache_repository.get(key)

    async def setFmRecallApps(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='fm_vec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600)

    async def getRankData(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='rank_list',
            entity_dest='',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setRankData(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='rank_list',
            entity_dest='',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 10)

    async def getRelatedTopics(self, params):
        key = self.buildCacheKey(
            entity_src='topic',
            action='related',
            entity_dest='topics',
            params=params,
            perment=False
        )
        return self.cache_repository.get(key)

    async def setRelatedTopics(self, params, value):
        key = self.buildCacheKey(
            entity_src='topic',
            action='related',
            entity_dest='topics',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getUserTopicRelated(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='related',
            entity_dest='topics',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserTopicRelated(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='related',
            entity_dest='topics',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600)

    async def getVideoRelation(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='related',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setVideoRelation(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='related',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600)

    async def getAppEditorRec(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='editor_rec',
            entity_dest='',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setAppEditorRec(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='editor_rec',
            entity_dest='',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getUserAppRec(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='rec',
            entity_dest='app',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserAppRec(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='rec',
            entity_dest='app',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getAppHumanRec(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='human_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setAppHumanRec(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='human_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 5)

    async def getCloudAppRec(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='cloud_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setCloudAppRec(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='cloud_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 10)

    async def getHotMoment(self, params):
        key = self.buildCacheKey(
            entity_src='moment',
            action='hot',
            entity_dest='',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHotMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='moment',
            action='hot',
            entity_dest='',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getUserAppMoment(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='app_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserAppMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='app_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getUserDeveloperMoment(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='developer_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserDeveloperMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='developer_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getUserTagMoment(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='tag_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserTagMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='tag_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getUserUserMoment(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='user_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserUserMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='user_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getSquenceMoment(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='squence_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setSquenceMoment(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='squence_rec',
            entity_dest='moment',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 1800)

    async def getUserAppRate(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='app_rate',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserAppRate(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='app_rate',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getAilabHotVideos(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='ailab_hot',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setAilabHotVideos(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='ailab_hot',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getAilabVideos(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='ailab_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setAilabVideos(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='ailab_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getHotVideos(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='search_hot',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHotVideos(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='search_hot',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getAppHotVideos(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='app_hot_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setAppHotVideos(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='app_hot_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getHotVideosByTag(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='tag_hot_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHotVideosByTag(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='tag_hot_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getNewVideos(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='new_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setNewVideos(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='new_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    def setTTl(self, msg):
        print("收到设置ttl的消息")
        ttl = self._cache_ttl
        if msg and msg.get('data'):
            ttl = int(msg.get('data'))
        else:
            print("收到设置ttl的消息,但是ttl不合法")
        self._cache_ttl = ttl
        ConfigService.instance().setConfig("redis","cache_ttl",str(ttl))
        return self._cache_ttl

    async def subscribeTTl(self):
        pubsub = self.getPubSub()
        await pubsub.subscribe(**{'update-ttl-channel': self.setTTl})
        pubsub.run_in_thread(daemon=True)

    def getPubSub(self):
        return self.cache_repository.getPubSub()

    async def get(self, key):
        return await self.cache_repository.get(key)

    async def set(self, key, value, ttl=None):
        if ttl is None:
            ttl = self._cache_ttl
        return await self.cache_repository.set(key, value, ttl)

    async def getVideoWatchedCount(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='watched_count',
            entity_dest='',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setVideoWatchedCount(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='watched_count',
            entity_dest='',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, self._cache_ttl)

    async def getVideoRelationRec(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='xd_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setVideoRelationRec(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='xd_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getVideoRecByEs(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='search_es_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setVideoRecByEs(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='search_es_rec',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600)

    async def getRelatedVideo(self, params):
        key = self.buildCacheKey(
            entity_src='video',
            action='related_with_es',
            entity_dest='videos',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setRelatedVideo(self, params, value):
        key = self.buildCacheKey(
            entity_src='video',
            action='related_with_es',
            entity_dest='videos',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getHumanAppTopRecs(self, params):
        key = self.buildCacheKey(
            entity_src='app',
            action='human_top_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHumanAppTopRecs(self, params, value):
        key = self.buildCacheKey(
            entity_src='app',
            action='human_top_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 5)

    async def getUserTopReload(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='reload_top',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserTopReload(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='reload_top',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600*6)


    async def getUserAppRelated(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='search_model_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setUserAppRelated(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='search_model_rec',
            entity_dest='apps',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 300)

    async def getCoordinatorContentResult(self, params):
        key = self.buildCacheKey(
            entity_src='user',
            action='coordinator_content',
            entity_dest='ugc',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setCoordinatorContentResult(self, params, value):
        key = self.buildCacheKey(
            entity_src='user',
            action='coordinator_content',
            entity_dest='ugc',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 300)

    async def getHotAppRec(self, params=""):
        key = self.buildCacheKey(
            entity_src='app',
            action='hot',
            entity_dest='app',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHotAppRec(self, value, params=""):
        key = self.buildCacheKey(
            entity_src='app',
            action='hot',
            entity_dest='app',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 3600 * 6)

    async def getHotVideoRec(self, params=""):
        key = self.buildCacheKey(
            entity_src='video',
            action='hot',
            entity_dest='video',
            params=params,
            perment=False
        )
        return await self.cache_repository.get(key)

    async def setHotVideoRec(self, value, params=""):
        key = self.buildCacheKey(
            entity_src='video',
            action='hot',
            entity_dest='video',
            params=params,
            perment=False
        )
        await self.cache_repository.set(key, value, 60 * 10)




