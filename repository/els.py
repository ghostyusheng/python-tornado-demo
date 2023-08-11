# -*- coding:utf-8 -*-

import json

from elasticsearch import NotFoundError
from elasticsearch.client import IndicesClient
from elasticsearch_async import AsyncElasticsearch

from function.function import config
from repository.base import BaseRepository
from utils.utils import log_exception, syslog


class ElsRepository(BaseRepository):
    def __init__(self):
        self.esclient = AsyncElasticsearch(
            config("elasticsearch", "es_conn").split(','))
        self.index_client = IndicesClient(self.esclient)

    async def getById(self, index, _id):
        try:
            doc = await self.esclient.get(index=index, id=_id)
            return doc.get("_source", None)
        except NotFoundError as e:
            return None
        except Exception as e:
            log_exception(e)
            return None

    async def deleteById(self, index, _id):
        try:
            res = await self.esclient.delete(index=index, id=_id)
            return res
        except NotFoundError as e:
            return None
        except Exception as e:
            log_exception(e)
            return None

    async def count(self, index: str, dsl: dict):
        body = json.dumps(dsl)
        response = await self.esclient.count(
            index=index,
            body=body
        )
        return response['count']

    async def exists(self, index, _id):
        response = await self.esclient.exists(
            index=index,
            id=_id
        )
        return response

    async def termVectors(self, index: str, dsl: dict):
        body = json.dumps(dsl)
        response = await self.esclient.termvectors(
            index=index,
            body=body
        )
        return response

    async def queryDSL(self,  index: str, dsl: dict, preference=None):
        fro = dsl.get("from", 0)
        size = dsl.get("size", 0)
        if fro+size > 10000:
            return {
                'hits': {
                    'total': {
                        "value": 0,
                        "relation": "gte"
                    },
                    'hits': []
                }
            }
        body = json.dumps(dsl)
        if preference:
            response = await self.esclient.search(
                index=index,
                body=dsl,
                preference=preference
            )
        else:
            response = await self.esclient.search(
                index=index,
                body=body
            )

        return response

    async def createIndex(self, index, settings):
        await self.index_client.create(index, settings)

    async def deleteAlias(self, index, alias):
        await self.index_client.delete_alias(index, alias)

    async def reindex(self, body):
        await self.esclient.reindex(body=body)

    async def putMapping(self, mappings, type, index):
        await self.index_client.put_mapping(mappings, type, index)

    async def makeAlias(self, index, alias):
        await self.index_client.put_alias(index, alias)

    async def putAlias(self, index, alias):
        await self.index_client.put_alias(index, alias)

    async def deleteAlias(self, index, alias):
        await self.index_client.delete_alias(index, alias)

    async def getAlias(self, index, alias):
        indices = await self.index_client.get_alias(index, alias)
        return indices
