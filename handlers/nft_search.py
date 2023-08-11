# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from common.es_conn import EsConn
from share.repository.els import ElsRepository
from elasticsearch.client import IndicesClient
from middleware.request import RequestMiddleware
from core.entity.request import RequestEntity
from function.function import R
from service.nft import NftService
from core.decorator.exception_capture import ExceptionCapture
from function.function import DEBUG, SDEBUG


class NftSearchHandler(BaseHandler):

    @ExceptionCapture()
    async def get(self, request=None):
        if not request:
            RequestMiddleware.verifyParams([
                ['from', 0, 'int'],
                ['limit', 10, 'int'],
                ['keyword', '', 'str'],
                ['labelIdList', '', 'str'],
                ['sort', '', 'str'],
                ['collectionIdList', '', 'str'],
                ['priceLow', '', 'str'],
                ['priceHigh', '', 'str'],
                ['gameId', '', 'str'],
            ])
            request: RequestEntity = R()
        result = []

        #df = NftService.instance().getFavourDF()
        #print(df)

        #print('cid: ', request.getCollectionId())
        #print('label_ids: ', request.getLabelIdList())

        #self.out()

        result, total = await NftService.instance().search(request)

        if DEBUG():
            self.out(SDEBUG())
            return

        self.out({
            "total": total, 
            "list": result
        })

    async def post(self):
        keyword = self.get_json_argument("keyword","")
        limit = self.get_json_argument("limit",10)
        fro = self.get_json_argument("from",0)
        label_id_list = self.get_json_argument("labelIdList","")
        collection_id = self.get_json_argument("collectionIdList","")
        sort = self.get_json_argument("sort","")
        priceLow = self.get_json_argument("priceLow", '')
        priceHigh = self.get_json_argument("priceHigh", '')
        gameId = self.get_json_argument("gameId","")
        request: RequestEntity = R()
        request.setKeyword(keyword)
        request.setLimit(limit)
        request.setFrom(fro)
        request.setLabelIdList(label_id_list)
        request.setCollectionId(collection_id)
        request.setSort(sort)
        request.setPriceLow(priceLow)
        request.setPriceHigh(priceHigh)
        request.setGameId(gameId)
        return await self.get(request)
