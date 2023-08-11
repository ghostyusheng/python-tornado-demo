# -*- coding:utf-8 -*-
import json
from elasticsearch.client import IndicesClient
from pypinyin import lazy_pinyin, pinyin, Style

from common.es_conn import EsConn
from core.decorator.cache import Cache
from core.decorator.log import Apm
from core.entity.request import RequestEntity
from core.search.builder import SearchBuilder
from function.function import config, SDEBUG, DEBUG
from models.app import AppModel, AppSearchRequest
from service.base import BaseService
from service.cache import CacheService
from utils.utils import log_exception, index_exception, eslog
from share.repository.els import ElsRepository
from core.const import const
from repository.db import DBRepository


class NftService(BaseService):

    def __init__(self):
        self.es_client = EsConn.instance()
        self.index_client = IndicesClient(self.es_client.conn)

    #@Cache.set(cache_time=60*3)
    async def search(self, request: RequestEntity):
        kw = request.getKeyword()
        fro = request.getFrom()
        limit = request.getLimit()
        label_id_list = request.getLabelIdList()
        priceLow = request.getPriceLow()
        priceHigh = request.getPriceHigh()
        priceRange = [["gte", priceLow], ["lte", priceHigh]] if (priceLow!="" or priceHigh!="") else None
        _sort = request.getSort()
        sort_key = '';
        sort_value = '';
        if _sort:
            sort_key, sort_value = list(_sort[0].keys())[0], list(_sort[0].values())[0]

        sub_dsl = SearchBuilder().match('nft_name_token_id', kw, 100)
        sub_dsl = sub_dsl.term("status", 10)
        if request.getGameId():
            sub_dsl = sub_dsl.term('game_id', request.getGameId())
        if priceRange:
            sub_dsl = sub_dsl.range('unit_price', priceRange)
        if label_id_list:
            sub_dsl = sub_dsl.term("nft_label_ids", label_id_list.split(","))
        if request.getCollectionId():
            sub_dsl = sub_dsl.term("collection_id", request.getCollectionId())

        builder = (
            SearchBuilder()
           .boolQuery()
           .must(
               sub_dsl.dsl()
            )
        )

        if sort_key and sort_value:
            builder = builder.sort([{sort_key: sort_value}])  

        query_body = builder.limit(0, 0).dsl()
        query_body["aggs"] = {
            "unique_sorted_nft_with_price_asc": {
                "terms": {
                  "field": "nft_id",
                  "size": 1000
                },
                "aggs": {
                    "top1": {
                      "top_hits": {
                        "size": 1,
                        "sort": [
                          {
                            "unit_price": {
                              "order": "asc"
                            }
                          }
                        ]
                      }
                    },
                    "ranking_unit_price": {
                        "min": {
                            "field": "unit_price" if not sort_key else sort_key
                        }
                    },
                    "my_bucket_sort": {
                      "bucket_sort": {
                        "sort": [
                          {
                            "ranking_unit_price": {
                              "order": "asc" if not sort_value else sort_value
                            }
                          }
                        ],
                        "size": limit,
                        "from": fro
                      }
                    }
                }
        }
     }

        if DEBUG():
            SDEBUG(type='search', dsl="echo '\n' '" + json.dumps(query_body).replace('"',"#") + "'|sed 's/#/\"/g'")

        response = await const.es.queryByDSL(
            index="t_market_order",
            dsl=query_body,
            preference=request.getSessionId()
        )
        total = response['hits']['total']['value']
        hits = response['aggregations']['unique_sorted_nft_with_price_asc'].get('buckets', [])

        result = []
        for hit in hits:
            data = {}
            _hit = hit['top1']['hits']['hits'][0]['_source']
            data['id'] = _hit['id']
            data['nftId'] = _hit.get('nft_id')
            data['nftName'] = _hit.get('nft_name')
            data['tokenId'] = _hit.get('token_id')
            data['gameId'] = _hit.get('game_id')
            data['gameName'] = _hit.get('game_name')
            data['unitPrice'] = _hit.get('unit_price_str')
            data['basePrice'] = _hit.get('base_price_str')
            data['hash'] = _hit.get('hash')
            data['num'] = _hit.get('num')
            data['coin'] = _hit.get('coin')
            data['coinId'] = _hit.get('coin_id')
            data['nftImage'] = _hit.get('nft_image')
            data['listingTime'] = _hit['listing_time']
            data['expireTime'] = _hit['expire_time']
            result.append(data)

        return result, total


    #@Cache.set(cache_time=60*5)
    async def hot(self, request: RequestEntity):
        fro = request.getFrom()
        limit = request.getLimit()
        priceLow = request.getPriceLow()
        priceHigh = request.getPriceHigh()
        priceRange = [["gte", priceLow], ["lte", priceHigh]] if (priceLow!="" or priceHigh!="") else None
        _sort = request.getSort()
        sort_key = '';
        sort_value = '';
        if _sort:
            sort_key, sort_value = list(_sort[0].keys())[0], list(_sort[0].values())[0]

        builder = SearchBuilder()

        sub_dsl = builder.term("status", 10)

        if request.getGameId():
            sub_dsl = sub_dsl.term('game_id', request.getGameId())
        if priceRange:
            sub_dsl = sub_dsl.range('unit_price', priceRange)
        if request.getLabelIdList():
            sub_dsl = sub_dsl.term("nft_label_ids", request.getLabelIdList().split(","))
        if request.getCollectionId():
            sub_dsl = sub_dsl.term("collection_id", request.getCollectionId())

        builder = (
            SearchBuilder()
           .boolQuery()
           .must(
               sub_dsl.dsl()
            )
        )

        query_body = builder.limit(0, 0).dsl()
        query_body["aggs"] = {
            "unique_sorted_nft_with_price_asc": {
                "terms": {
                  "field": "nft_id",
                  "size": 1000
                },
                "aggs": {
                    "top1": {
                      "top_hits": {
                        "size": 1,
                        "sort": [
                          {
                            "unit_price": {
                              "order": "asc"
                            }
                          }
                        ]
                      }
                    },
                    "ranking_unit_price": {
                        "min": {
                            "field": "unit_price" if not sort_key else sort_key
                        }
                    },
                    "my_bucket_sort": {
                      "bucket_sort": {
                        "sort": [
                          {
                            "ranking_unit_price": {
                              "order": "asc" if not sort_value else sort_value
                            }
                          }
                        ],
                        "size": limit,
                        "from": fro
                      }
                    }
                }
        }
     }

        if DEBUG():
            SDEBUG(type='hot', dsl="echo '\n' '" + json.dumps(query_body).replace('"',"#") + "'|sed 's/#/\"/g'")

        response = await const.es.queryByDSL(
            index="t_market_order",
            dsl=query_body,
            preference=request.getSessionId()
        )
        total = response['hits']['total']['value']
        hits = response['aggregations']['unique_sorted_nft_with_price_asc'].get('buckets', [])

        result = []
        for hit in hits:
            data = {}
            _hit = hit['top1']['hits']['hits'][0]['_source']
            data['id'] = _hit['id']
            data['nftId'] = _hit.get('nft_id')
            data['nftName'] = _hit.get('nft_name')
            data['tokenId'] = _hit.get('token_id')
            data['gameId'] = _hit.get('game_id')
            data['gameName'] = _hit.get('game_name')
            data['unitPrice'] = _hit.get('unit_price_str')
            data['basePrice'] = _hit.get('base_price_str')
            data['hash'] = _hit.get('hash')
            data['num'] = _hit.get('num')
            data['coin'] = _hit.get('coin')
            data['coinId'] = _hit.get('coin_id')
            data['nftImage'] = _hit.get('nft_image')
            data['listingTime'] = _hit['listing_time']
            data['expireTime'] = _hit['expire_time']
            result.append(data)

        return result, total
 
    def getFavourDF(self, request: RequestEntity):
        res = DBRepository.instance().queryAll("""
            SELECT
                nft_id,
                count(1) as cnt 
            FROM
                "market11"."t_nft_favour" 
            group
                by nft_id
        """)
        return res
