# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from common.es_conn import EsConn
from share.repository.els import ElsRepository
from elasticsearch.client import IndicesClient
from middleware.request import RequestMiddleware
from core.entity.request import RequestEntity
from function.function import R
from service.nft import NftService
from core.const import const
from core.decorator.exception_capture import ExceptionCapture
from function.function import DEBUG, SDEBUG


class NftHotHandler(BaseHandler):

    @ExceptionCapture()
    async def get(self, request=None):
        if not request:
            RequestMiddleware.verifyParams([
                ['from', 0, 'int'],
                ['limit', 10, 'int'],
                ['labelIdList', '', 'str'],
                ['sort', '', 'str'],
                ['collectionIdList', '', 'str'],
                ['priceLow', '', 'str'],
                ['priceHigh', '', 'str'],
                ['gameId', '', 'str'],
            ])
            request: RequestEntity = R()
        result = []
        fro = request.getFrom()
        limit = request.getLimit()
        
        result, total = await NftService.instance().hot(request)
        if DEBUG():
            self.out(SDEBUG())
            return

        self.out({
            "total": total, 
            "list": result
        })

    async def post(self):
        limit = self.get_json_argument("limit",10)
        fro = self.get_json_argument("from",0)
        label_id_list = self.get_json_argument("labelIdList","")
        collection_id = self.get_json_argument("collectionIdList","")
        sort = self.get_json_argument("sort","")
        priceLow = self.get_json_argument("priceLow", '')
        priceHigh = self.get_json_argument("priceHigh", '')
        sort = self.get_json_argument("sort","")
        gameId = self.get_json_argument("gameId","")
        request: RequestEntity = R()
        request.setLimit(limit)
        request.setFrom(fro)
        request.setLabelIdList(label_id_list)
        request.setCollectionId(collection_id)
        request.setSort(sort)
        request.setPriceLow(priceLow)
        request.setPriceHigh(priceHigh)
        request.setGameId(gameId)
        return await self.get(request)
