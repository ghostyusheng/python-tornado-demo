from share.repository.base import BaseRepository
from elasticsearch_async import AsyncElasticsearch
from elasticsearch import NotFoundError
from elasticsearch.exceptions import ConflictError
from elasticsearch.client import IndicesClient
from importlib import import_module
import json

class ElsRepository(BaseRepository):
    def __init__(self, es_conn: list, http_auth=None):
        if http_auth:
            self.esclient = AsyncElasticsearch(es_conn, http_auth=http_auth)
        else:
            self.esclient = AsyncElasticsearch(es_conn)
        self.index_client = IndicesClient(self.esclient)


    async def initIndex(self, index):
        _class = self.getClassNameByIndex(index)
        t = import_module("share.common." + index)
        obj = getattr(t, _class)
        index = obj.getIndex()
        try:
            await self.createIndex(index, obj.getBody())
            print(f"{index} index create finish!")
        except Exception as e:
            print(str(e))
        await self.putMapping(index, obj.getMappings())
        print(f"{index} index update finish!")


    def getClassNameByIndex(self,index):
        classCharList = []
        isInitials = True
        for c in index:
            if c == "_":
                isInitials = True
                continue
            else:
                if isInitials:
                    classCharList.append(str(c).upper())
                    isInitials = False
                else:
                    classCharList.append(c)
        return "".join(classCharList)

    async def getById(self, index, _id):
        try:
            doc = await self.esclient.get(index=index, id=_id)
            return doc.get("_source", None)
        except NotFoundError as e:
            return None
        except Exception as e:
            return None


    async def upsert(self, index, doc_id, data):

        await self.esclient.update(index, doc_id, '_doc', {
            'doc_as_upsert': True,
            'doc': data
        })



    async def createIndex(self, index, settings):
        await self.index_client.create(index, settings)


    async def putMapping(self, index, mappings, typ=None):
        await self.index_client.put_mapping(mappings, typ, index)

    async def queryByDSL(self,  index: str, dsl: dict, preference=None):
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
