# -*- coding:utf-8 -*-
import json
import math
import random
import re
import time

import numpy as np
from elasticsearch.client import IndicesClient
from pypinyin import lazy_pinyin, pinyin, Style

from common.app import App
from common.es_conn import EsConn
from common.stratagem import Stratagem
from core.decorator.cache import Cache
from core.decorator.log import Apm
from core.entity.request import RequestEntity
from core.search.builder import SearchBuilder
from function.function import config, SDEBUG, DEBUG
from models.app import AppModel, AppSearchRequest
from repository.els import ElsRepository
from service.base import BaseService
from service.cache import CacheService
from service.developer import DeveloperService
from service.els import ElsService
from service.format import FormatService
from service.identify import IdentifyService
from service.index_filter import IndexFilterService
from service.reviews_keywords import ReviewsKeywordsService
from service.stratagem import StratagemService
from service.user_behavior import UserBehaviorService
from share.utils.utils import filterSpecialChar
from share.utils.utils import suggest_search_amend
from utils import utils, const
from utils.utils import log_exception, index_exception, eslog, belongSn, chooseMatchLanguageTitle, buildLogVia


class AppService(BaseService):

    def __init__(self):
        self.es_client = EsConn.instance()
        self.index_client = IndicesClient(self.es_client.conn)

    async def createIndex(self, index):
        settings = {}
        settings['settings'] = App.settings
        settings['settings']['number_of_replicas'] = 0
        await self.index_client.create(index, settings)
        await self.index_client.put_mapping(App.properties, None, index)

    async def getIndicesByAlias(self, alias):
        try:
            indices = await ElsService.instance().getAlias(None, alias)
            return [index for index in indices]
        except Exception as e:
            log_exception(e)
            return []

    async def indexExist(self, index):
        return await self.index_client.exists(index)

    async def addAlias(self, index, alias):
        await self.index_client.put_alias(index, alias)

    @Apm.set('app-replace')
    async def replaceIndex(self, data, doc_id):
        index = config("elasticsearch", "app_index")
        data['id'] = doc_id
        if 'title_all' in data:
            filter_titles = []
            for t in data['title_all']:
                filter_titles.append(utils.titleFilter(t))
            data['title_all'] = filter_titles
            data['title_standard'] = data['title_all']
            data['title_not_analyzed'] = data['title_all']
            data['title_twogram'] = data['title_all']
        if 'alias' in data and data['alias']:
            data['alias'] = list(map(lambda x: x.lower(), data['alias']))
        if 'title' in data:
            filter_title = utils.titleFilter(data['title'])
            data['title'] = filter_title
            py = ''.join(lazy_pinyin(data['title']))
            py_blank = ' '.join(lazy_pinyin(data['title']))
            data['full_pinyin'] = py
            data['pinyin'] = py_blank
            abbr = ''
            for fp in pinyin(data['title'], style=Style.FIRST_LETTER):
                abbr = ''.join((abbr, fp[0]))
            data['abbr'] = abbr
        if 'title_not_analyzed' in data:
            data['clear_title'] = []
            for t in data['title_not_analyzed']:
                clear_title, clear = utils.clearTitle(t)
                if type(clear_title) == list:
                    data['clear_title'].extend(clear_title)
                else:
                    data['clear_title'].append(clear_title)

        if ('whats_new' in data) and (data['whats_new'] == ''):
            data.pop('whats_new')
        if ('is_flag_close' in data) and (data['is_flag_close'] == ''):
            data.pop('is_flag_close')

        data['hits_weight'] = self.cacl_hits_weight(data)
        data['hits_weight_suggest'] = self.cacl_hits_weight_suggest(data)

        is_exists = await ElsRepository.instance().exists(index, doc_id)

        await self.es_client.index(index, doc_id, '_doc', data)
        await AppModel.instance().replaceOneByDict(data)

        if (not is_exists) and ('title' in data):
            key = data['title'][:2]
            await CacheService.instance().delete(key)
            if key != key.lower():
                await CacheService.instance().delete(key.lower())

    @Apm.set('app-update')
    async def updateIndex(self, data, doc_id):
        index = config("elasticsearch", "app_index")
        data['id'] = doc_id
        if 'title_all' in data:
            filter_titles = []
            for t in data['title_all']:
                filter_titles.append(utils.titleFilter(t))
            data['title_standard'] = data['title_all']
            data['title_not_analyzed'] = data['title_all']
            data['title_twogram'] = data['title_all']
        if 'alias' in data and data['alias']:
            data['alias'] = list(map(lambda x: x.lower(), data['alias']))
        if 'title' in data:
            filter_title = utils.titleFilter(data['title'])
            data['title'] = filter_title
            py = ''.join(lazy_pinyin(data['title']))
            py_blank = ' '.join(lazy_pinyin(data['title']))
            data['full_pinyin'] = py
            data['pinyin'] = py_blank
            abbr = ''
            for fp in pinyin(data['title'], style=Style.FIRST_LETTER):
                abbr = ''.join((abbr, fp[0]))
            data['abbr'] = abbr
        if 'title_not_analyzed' in data:
            data['clear_title'] = []
            for t in data['title_not_analyzed']:
                clear_title, clear = utils.clearTitle(t)
                if type(clear_title) == list:
                    data['clear_title'].extend(clear_title)
                else:
                    data['clear_title'].append(clear_title)

        response = await self.es_client.search(
            index=index,
            body={
                "query": {
                    "ids": {
                        "values": [doc_id]
                    }
                }
            }
        )
        hits = response['hits']
        total = hits['total']['value']
        if total > 0:
            hit = hits['hits'][0]
            hit['_source'].update(data)
            data = hit['_source']
            data['hits_weight'] = self.cacl_hits_weight(data)
            data['hits_weight_suggest'] = self.cacl_hits_weight_suggest(data)
            await self.es_client.update(index, doc_id, '_doc', {
                'doc_as_upsert': True,
                'doc': data
            })

            await AppModel.instance().updateOneByDict(data)

    @Apm.set('app-delete')
    async def deleteById(self, _id):
        try:
            doc = await ElsRepository.instance().getById(config("elasticsearch", "app_index"), _id)
            await self.es_client.delete(config("elasticsearch", "app_index"), _id)
            if doc and doc.get('title'):
                key = doc['title'][:2]
                await CacheService.instance().delete(key)
                await CacheService.instance().delete(key.lower())
            await AppModel.instance().deleteById(_id)
        except Exception as e:
            index_exception(e)
            return None

    def cacl_hits_weight_suggest(self, doc):
        hits_weight_suggest = 0
        hits = 0
        if 'hits' in doc:
            hits = int(doc['hits'])
            hits_weight_suggest = hits_weight_suggest + 0.6 * hits

        if 'reserve_count_new' in doc:
            reserve_stats = max(
                1, int(doc['reserve_count_new']) - int(doc['reserve_canceled_count']))
            hits_weight_suggest = hits_weight_suggest + 0.5 * reserve_stats
        if 'pv_search_one_day' in doc:
            page_view = int(doc['pv_search_one_day'])

        return hits_weight_suggest

    def cacl_hits_weight(self, doc):
        hits = 0
        if 'hits' in doc:
            hits = int(doc['hits'])
        play_hits = 0
        if 'play_hits' in doc:
            play_hits = int(doc['play_hits'])

        hits_weight = round(math.log(max(hits, 100), 100), 1)
        play_hits_weight = round(math.log(max(play_hits, 100), 100), 1)

        hits_weight = max(hits_weight, play_hits_weight)

        if hits_weight == 1:
            reserve_stats = max(
                1, int(doc['reserve_count_new']) - int(doc['reserve_canceled_count']))
            hits_weight = round(math.log(reserve_stats + 1, 10000), 2)
            hits_weight = max(hits_weight, 1)

        fans_stats = max(1, int(doc['fans_count']))
        fans_weight = round(math.log(fans_stats + 1, 1000000), 2)
        hits_weight += fans_weight

        page_view = int(doc['pv_search_one_day'])
        if page_view is not None:
            hits_weight += round(0.1 * math.log(page_view + 1, 1000), 2)

        return hits_weight

    async def searchAppWithBooster(self, request: RequestEntity):
        kw = request.getKeyword()
        fro = request.getFrom()
        limit = request.getLimit()
        store = request.getStore()
        if (not store) or (store=='other'):
            store = 'default'

        builder = (SearchBuilder()
                   .boolQuery()
                   .should(
            SearchBuilder()
            .term('title_not_analyzed', kw, 100)
            .prefix('title_not_analyzed', kw, 50)
            .match_phrase('title_all', kw, 35)
            .match_phrase('title_standard', kw, 30)
            .term('alias', kw, 10)
            .dsl()
        )
            .must_not(
                SearchBuilder()
            .term('identifier', '')
            .term(f'store_status.android.{store}', 3)
            .dsl()
        )
        )

        query_body = builder.limit(fro, limit).dsl()

        response = await ElsRepository.instance().queryDSL(
            index=config("elasticsearch", "app_index"),
            dsl=query_body,
            preference=request.getSessionId()
        )
        hits = response['hits']

        total = hits['total']['value']
        hits = hits['hits']
        result = []
        for hit in hits:
            data = {}
            hit_id = hit['_id']
            data['id'] = hit_id
            result.append(data)

        return result, total

    async def searchApp(
            self,
            request: RequestEntity,
            not_show_ids=None
    ):
        kw = request.getKeyword()
        fro = request.getFrom()
        limit = request.getLimit()
        uid = request.getUserId()
        store = request.getStore()
        platform = request.getPlatform()
        device_id = request.getDeviceId()
        highlight_fields = request.getHighlight()
        sort_fields = request.getSort()
        scene = request.getScene()
        session_id = request.getSessionId()
        filter_kw = utils.keywordFilter(kw)
        filters = request.getFilter()
        analysis_kws = []
        stratagem = await (StratagemService(kw, analysis_kws)
                           # .setting('advice')
                           .setting('google_fix')
                           .enable())
        sort_fields = FormatService().instance().getSortFields(sort_fields)
        highlight_fields = FormatService().instance().getHighlightFields(highlight_fields)

        identify_tag_info = await IdentifyService().tagIdentify(kw)
        identify_title_info = await IdentifyService().appIdentify(filter_kw)
        identify_top_info = await IdentifyService().topIdentify(filter_kw)
        identify_synonym_info = await IdentifyService().synonymIdentify(filter_kw)
        identify_developer_info = await IdentifyService.instance().developerIdentify(kw)
        keyword_feature = IdentifyService().keywordIdentify(kw)

        app_search_request = AppSearchRequest(request)
        app_search_request.filtered_kw = filter_kw
        app_search_request.keyword_feature = keyword_feature
        app_search_request.strategem = stratagem
        app_search_request.sort_fields = sort_fields
        app_search_request.highlight_fields = highlight_fields
        app_search_request.top_apps = identify_top_info
        app_search_request.synonyms = identify_synonym_info[0]['synonyms'] if identify_synonym_info else [
        ]
        app_search_request.not_show_ids = [] if not_show_ids is None else not_show_ids

        identify_developer_search = False
        if identify_developer_info:
            developer_id = identify_developer_info[0]['id']
            if int(developer_id) in [3505,1,1012]:
                identify_developer_search = True
            # developer_search = await DeveloperService.instance().isPopular(developer_id)
            # if developer_search:
            #     identify_developer_search = True

        if len(identify_tag_info) > 0:
            (hits, suggest_kws) = await self.searchByKeywordInTag(
                kw,
                fro,
                limit,
                store,
                platform,
                highlight_fields,
                sort_fields,
                identify_top_info,
                not_show_ids,
                session_id,
                filters
            )
        elif identify_developer_search:
            developer_id = identify_developer_info[0]['id']
            (hits, suggest_kws) = await self.searchByDeveloper(kw, developer_id, fro, limit, store, platform, identify_top_info, not_show_ids, session_id, filters)
        else:
            # 标题完全匹配，标题和标签加权策略
            if identify_title_info:
                (hits, suggest_kws) = await self.searchByKeywordInTitle(
                    kw,
                    filter_kw,
                    fro,
                    limit,
                    store,
                    platform,
                    highlight_fields,
                    sort_fields,
                    identify_title_info,
                    identify_top_info,
                    identify_synonym_info,
                    not_show_ids,
                    session_id,
                    filters
                )
            else:
                (hits, suggest_kws) = await self.searchByKeyword(app_search_request)

        if hits['total']['value'] == 0:
            google_kws = stratagem['result']['google_fix']
            if len(google_kws) > 0:
                kw = ' '.join(google_kws)
                (hits, suggest_kws) = await self.searchByKeyword(app_search_request)

        total = hits['total']['value']
        hits = hits['hits']
        result = []
        log_ids = []
        log_titles = []
        shown_apps = []
        for hit in hits:
            data = {}
            hit_id = hit['_id']
            title = hit['_source']['title']
            data['id'] = hit_id
            shown_apps.append(hit_id)
            log_ids.append(hit_id)
            log_titles.append(title)
            if 'highlight' in hit:
                data['highlight'] = hit['highlight']
            result.append(data)
        await UserBehaviorService.instance().putShownApp(kw, fro, shown_apps)
        buildLogVia(result, fro, 'APP')
        eslog({
            'hit': total,
            'result_titles': log_titles,
            'result_ids': log_ids,
            'suggest_result': suggest_kws,
            'search': kw,
            'store': store,
            'device_id': device_id,
            'from': fro,
            'platform': platform,
            'scene': scene,
            'date': utils.getDateStr(),
            'session_id': session_id
        }, 'app')
        return result, total

    async def searchByKeyword(self, request):
        body, suggest_kws = self.keywordDslBuilderV1(request)

        if DEBUG():
            SDEBUG(type='app', dsl=json.dumps(body))

        response = await ElsRepository.instance().queryDSL(
            index=config("elasticsearch", "app_index"),
            dsl=body,
            preference=request.session_id
        )
        # response = await self.es_client.search(
        #     index=config("elasticsearch", "app_index"),
        #     body=body,
        #     preference=session_id
        # )

        hits = response['hits']
        return (hits, suggest_kws)

    def keywordDslBuilder(self, kw, filter_useless_kw, fro, limit, stratagem, top_apps, synonyms, platform, store, highlight_fields, sort_fields, not_show_ids, filters, keyword_feature):
        orgin_kw = kw
        if filter_useless_kw != '':
            kw = filterSpecialChar(filter_useless_kw)
        else:
            kw = filterSpecialChar(kw)
        fuzz_re = re.compile(r'^[a-zA-Z ]+$')
        builder = self.keywordBaseBuilder(
            kw, orgin_kw, fuzz_re, keyword_feature)
        builder = IndexFilterService.instance().wrapSearchBuilder(builder, filters)

        if not_show_ids:
            builder.must_not(
                SearchBuilder()
                .term('id', not_show_ids)
                .dsl()
            )

        try:
            suggest_kws = []
            if len(suggest_kws) > 0:
                suggest_kw = suggest_kws[0]
                (builder
                    .should(
                        SearchBuilder()
                        .match('title_all', {
                            'query': filterSpecialChar(suggest_kw),
                            'minimum_should_match': '1<60% 20<80%'
                        }, 0.3).dsl()
                    )
                 )
            pass
        except Exception as e:
            log_exception(e)
            raise e

        (builder
            .should(
                SearchBuilder().match_phrase('illustrations', kw, 0.3).dsl()
            )
         )

        if top_apps:
            for top_app in top_apps:
                boost = 1
                if 'boost' in top_app:
                    boost = top_app['boost']
                builder.should(
                    SearchBuilder()
                    .term('id', top_app['app_id'], boost=boost * 100)
                    .dsl()
                )

        py = ''.join(lazy_pinyin(kw.strip('[0123456789]')))
        if '' != py:
            builder.should(
                SearchBuilder()
                .prefix('full_pinyin', py, 0.2)
                .wildcard('full_pinyin', '*' + py, boost=0.4)
                .wildcard('abbr', '*' + kw.lower() + '*', boost=0.1)
                .dsl()
            )

        if utils.str_is_identifier(kw):
            builder.should(
                SearchBuilder()
                .term('identifier', kw, 0.4)
                .dsl()
            )

        kws_builder = []
        if synonyms:
            for synonym in synonyms[0]['synonyms']:
                kws_builder.append(
                    {
                        "function_score": self.keywordBaseBuilder(synonym, synonym, fuzz_re, keyword_feature).function({
                            'field_value_factor': {
                                'field': 'hits_weight',
                                'factor': 2,
                                'missing': 0
                            }
                        }).dsl()
                    }
                )

        kws_builder.append(
            {
                "function_score": builder.function({
                    'field_value_factor': {
                        'field': 'hits_weight',
                        'factor': 1,
                        'missing': 0
                    }
                }).dsl()
            }
        )

        time_builder = []
        time_builder.append(
            {
                "function_score": {
                    "query": {
                        "match_all": {}
                    },
                    "functions": [
                        {
                            "script_score": {
                                "script": {
                                    "source": "1 /( cur_time - doc['updated_at'].value > 86400 ? log10(( cur_time - doc['updated_at'].value ) / 86400 + 10) : 1)",
                                    "params": {
                                        "cur_time": int(time.time())
                                    },
                                    "lang": "expression"
                                }
                            }
                        }
                    ]
                }
            }
        )

        result_builder = (SearchBuilder()
                          .boolQuery()
                          .must([SearchBuilder().boolQuery().should(kws_builder).dsl()['query']])
                          .should(time_builder)
                          )

        if store:
            if 'ios' == platform:
                result_builder.should(
                    SearchBuilder()
                    .term('area_ios', store, 0.5)
                    .dsl()
                )
            else:
                result_builder.should(
                    SearchBuilder()
                    .term('area', store, 0.5)
                    .dsl()
                )

        # 非简版游戏优先
        result_builder.should(
            SearchBuilder()
            .term('is_style_simple', 'false', 0.1)
            .dsl()
        ).limit(fro, limit)

        if store and platform:
            result_builder.must(
                [self.storeDslBuilder(platform, store).get('query')])

        query_body = result_builder.dsl()

        if len(highlight_fields) > 0:
            query_body['highlight'] = self.buildHighlightBody(highlight_fields)

        sort = self.buildSortBody(sort_fields)
        if len(sort) > 0:
            query_body['sort'] = sort

        query_body['track_total_hits'] = True
        return query_body, suggest_kws

    def keywordDslBuilderV1(self, request: AppSearchRequest):
        platform = request.platform
        store = request.store
        sort_fields = request.sort_fields
        area = 'area_ios' if 'ios' == platform else 'area'

        origin_kw = request.kw
        filtered_kw = request.filtered_kw
        kw = filterSpecialChar(
            origin_kw) if filtered_kw == '' else filterSpecialChar(filtered_kw)
        py = ''.join(lazy_pinyin(kw.strip('[0123456789]')))
        fuzz_re = re.compile(r'^[a-zA-Z ]+$')

        query = SearchBuilder().boolQuery()
        fs_query_builder = self.keywordBaseBuilder(kw, origin_kw, fuzz_re,
                                                   IdentifyService.instance().keywordIdentify(kw))
        fs_query_builder = IndexFilterService.instance(
        ).wrapSearchBuilder(fs_query_builder, request.filter)

        # function_score查询补充should
        fs_query_should = (SearchBuilder().match_phrase('illustrations', kw, boost=0.3)
                           .term('clear_title.keyword', kw, 2)
                           )
        if py:
            (fs_query_should
             .prefix('full_pinyin', py, boost=0.2)
             .wildcard('full_pinyin', f'*{py}', boost=0.4)
             .wildcard('abbr', f'*{kw.lower()}*', boost=0.1))
        if utils.str_is_identifier(kw):
            fs_query_should.term('identifier', kw, boost=0.4)
        for top_app in request.top_apps:
            fs_query_should.term(
                'id', top_app['app_id'], boost=top_app.get('boost', 1) * 100)

        fs_query_builder.should(fs_query_should.dsl())
        fs = [{
            "function_score": {
                "query": fs_query_builder.dsl().get('query'),
                "field_value_factor": {"field": "hits_weight"}
            }
        }]

        if request.synonyms:
            for synonym in request.synonyms:
                fs.append({
                    "function_score": {
                        "query": self.keywordBaseBuilder(synonym, synonym, fuzz_re,
                                                         IdentifyService.instance().keywordIdentify(kw)).dsl().get(
                            'query'),
                        "field_value_factor": {"field": "hits_weight", 'factor': 2}
                    }
                })
            # 取关键词和同义词的并集
            fs = SearchBuilder().boolQuery().should(fs).dsl().get('query')

        """1. query must: function score包含的查询"""
        query.must([fs])

        """2. 商店状态关闭，不显示的id"""
        query.must([self.storeDslBuilder(platform, store).get('query')])

        if request.not_show_ids:
            query_must_not = SearchBuilder().term('id', request.not_show_ids, filter=False)
            query.must_not(query_must_not.dsl())

        """3. query should: 简版游戏，地区，更新时间"""
        query_should = SearchBuilder().term('is_style_simple', 'false',
                                            0.1).term(area, store, 0.5).dsl()
        query_should.append(
            {
                "script_score": {
                    "query": {
                        "match_all": {}
                    },
                    "script": {
                        "source": "1 /( cur_time - doc['updated_at'].value > 86400 ? log10(( cur_time - doc['updated_at'].value ) / 86400 + 10) : 1)",
                        "params": {
                            "cur_time": int(time.time())
                        },
                        "lang": "expression"
                    }
                }
            }
        )
        query.should(query_should)
        query.highlight(request.highlight_fields).trackTotalHits().limit(
            request.fro, request.limit)
        if sort_fields:
            query.sort([sort_fields])
        return query.dsl(), []

    async def searchByDeveloper(self,
                                kw,
                                developer_id,
                                fro=0,
                                limit=10,
                                store='other',
                                platform='',
                                top_apps=None,
                                not_show_ids=[],
                                session_id='',
                                filters=''):
        query_body = self.developerDslBuilder(
            kw, developer_id, top_apps, platform, store, fro, limit, not_show_ids, filters)
        body = json.dumps(query_body)

        if DEBUG():
            SDEBUG(type='app', dsl=body)

        response = await self.es_client.search(
            index=config("elasticsearch", "app_index"),
            body=body,
            preference=session_id
        )

        hits = response['hits']
        return (hits, [])

    def developerDslBuilder(self, kw, developer_id, top_apps, platform, store, fro, limit, not_show_ids, filters):

        builder = (SearchBuilder()
                   .boolQuery()
                   .should(
            SearchBuilder()
            .term('developer_id', developer_id, boost=5)
            .match_phrase('author', {
                'query': kw
            }, 5)
            .dsl()
        )
        )

        builder = IndexFilterService.instance().wrapSearchBuilder(builder, filters)
        if not_show_ids:
            builder.must_not(
                SearchBuilder()
                .term('id', not_show_ids)
                .dsl()
            )

        if top_apps:
            for top_app in top_apps:
                boost = 1
                if 'boost' in top_app:
                    boost = top_app['boost']
                builder.should(
                    SearchBuilder()
                    .term('id', top_app['app_id'], boost=boost * 100)
                    .dsl()
                )

        result_builder = (SearchBuilder()
                          .boolQuery()
                          .must([
                              builder.dsl()['query']
                          ])).limit(fro, limit)

        if store and platform:
            result_builder.must(
                [self.storeDslBuilder(platform, store).get('query')])

        query_body = result_builder.dsl()
        query_body['sort'] = [
            {
                "hits_weight": {
                    "order": "desc"
                }
            }
        ]

        query_body['track_total_hits'] = True
        return query_body

    @Cache.set()
    async def searchByKeywordInTag(
            self,
            kw,
            fro=0,
            limit=10,
            store='other',
            platform='',
            highlight_fields=None,
            sort_fields=None,
            top_apps=None,
            not_show_ids=[],
            session_id='',
            filters=''
    ):

        query_body = self.tagDslBuilder(
            kw, top_apps, platform, store, fro, limit, highlight_fields, sort_fields, not_show_ids, filters)
        body = json.dumps(query_body)

        if DEBUG():
            SDEBUG(type='app', dsl=body)

        response = await self.es_client.search(
            index=config("elasticsearch", "app_index"),
            body=body,
            preference=session_id
        )

        hits = response['hits']
        return (hits, [])

    def tagDslBuilder(self, kw, top_apps, platform, store, fro, limit, highlight_fields, sort_fields, not_show_ids, filters):
        kw = kw.lower()

        builder = (SearchBuilder()
                   .boolQuery()
                   .should(
            SearchBuilder()
            .match_phrase('title_not_analyzed', kw, 10)
            .match_phrase('title_all', kw, 1.2)
            .match_phrase('alias', kw, 1)
            .match_phrase('tags', kw, 2)
            .dsl()
        )
        )

        builder = (SearchBuilder()
                   .boolQuery()
                   .must([
                       {
                           "function_score": builder.function({
                               'field_value_factor': {
                                   'field': 'hits_weight',
                                   'factor': 1,
                                   'missing': 0
                               }
                           }).dsl()
                       }
                   ]))
        builder = IndexFilterService.instance().wrapSearchBuilder(builder, filters)
        if not_show_ids:
            builder.must_not(
                SearchBuilder()
                .term('id', not_show_ids)
                .dsl()
            )

        if top_apps:
            for top_app in top_apps:
                boost = 1
                if 'boost' in top_app:
                    boost = top_app['boost']
                builder.should(
                    SearchBuilder()
                    .term('id', top_app['app_id'], boost=boost * 100)
                    .dsl()
                )

        # platform特殊处理
        if platform is not None and 'ios' == platform:
            builder.should(
                SearchBuilder()
                .term('area_ios', store, 0.5)
                .dsl()
            )
        else:
            builder.should(
                SearchBuilder()
                .term('area', store, 0.5)
                .dsl()
            )

        # 非简版游戏优先
        builder.should(
            SearchBuilder()
            .term('is_style_simple', 'false', 0.3)
            .dsl()
        )

        result_builder = (SearchBuilder()
                          .boolQuery()
                          .must([
                              builder.dsl()['query']
                          ])).limit(fro, limit)

        if store and platform:
            result_builder.must(
                [self.storeDslBuilder(platform, store).get('query')])

        query_body = result_builder.dsl()

        if highlight_fields:
            query_body['highlight'] = self.buildHighlightBody(highlight_fields)

        if sort_fields:
            sort = self.buildSortBody(sort_fields)
            query_body['sort'] = sort

        query_body['track_total_hits'] = True
        return query_body

    async def searchByKeywordInTitle(
            self,
            kw,
            filter_kw='',
            fro=0,
            limit=10,
            store='other',
            platform='',
            highlight_fields=None,
            sort_fields=None,
            relate_app=None,
            top_apps=None,
            synonyms=None,
            not_show_ids=[],
            session_id='',
            filters=''
    ):
        query_body = await self.titleDslBuilder(
            kw,
            filter_kw,
            relate_app,
            top_apps,
            fro,
            limit,
            synonyms,
            platform,
            store,
            highlight_fields,
            sort_fields,
            not_show_ids,
            filters
        )
        body = json.dumps(query_body)

        if DEBUG():
            SDEBUG(type='app', dsl=body)

        response = await self.es_client.search(
            index=config("elasticsearch", "app_index"),
            body=body,
            preference=session_id
        )

        hits = response['hits']
        return (hits, [])

    async def titleDslBuilder(
            self,
            kw,
            filter_kw,
            relate_app,
            top_apps,
            fro,
            limit,
            synonyms,
            platform,
            store,
            highlight_fields,
            sort_fields,
            not_show_ids,
            filters
    ):
        kw = kw.lower()
        if filter_kw == '':
            filter_kw = kw
        prog = re.compile("[（(].*[)）]")
        filter_kw = re.sub(prog, '', filter_kw)
        prog = re.compile("-.*$")
        search_kw = re.sub(prog, '', filter_kw)
        prog = re.compile(r'[0123456789零一二三四五六七八九I]$')
        search_kw = prog.sub('', search_kw)
        tags = []
        if relate_app:
            tags = relate_app[0]['tags'][:5]

        builder = self.titleBaseBuilder(kw, search_kw)
        builder = IndexFilterService.instance().wrapSearchBuilder(builder, filters)
        if top_apps:
            for top_app in top_apps:
                boost = 1
                if 'boost' in top_app:
                    boost = top_app['boost']
                builder.should(
                    SearchBuilder()
                    .term('id', top_app['app_id'], boost=boost * 100)
                    .dsl()
                )

        if not_show_ids:
            builder.must_not(
                SearchBuilder()
                .term('id', not_show_ids)
                .dsl()
            )

        if len(tags) > 0:
            for i in range(len(tags)):
                builder.should(
                    SearchBuilder()
                    .term('tags', tags[i], 2 - 0.4 * i)
                    .dsl()
                )
        use_user_profile = belongSn('click_feedback')
        if use_user_profile and int(fro) / int(limit) >= 1:
            recent_tags_limit = Stratagem._global['user_profile']['recent_tag']['length']
            recent_tags = await UserBehaviorService.instance().getRecentTags(recent_tags_limit)
            if recent_tags:
                shown_app = await UserBehaviorService.instance().getShownApp(kw)
                # 需要not in 的id 失效时了 就不能走用户画像逻辑了,
                if shown_app:
                    builder.must_not(
                        SearchBuilder()
                        .term('id', shown_app)
                        .dsl()
                    )
                    max_boost = Stratagem._global['user_profile']['recent_tag']['max_boost']
                    min_boost = Stratagem._global['user_profile']['recent_tag']['min_boost']
                    for i, boost in enumerate(np.linspace(max_boost, min_boost, len(recent_tags))):
                        builder.should(
                            SearchBuilder()
                            .term('tags', recent_tags[i], boost)
                            .dsl()
                        )
                    fro = 0

        synonyms_builder = []
        synonyms_builder.append(
            {
                "function_score": builder.function({
                    'field_value_factor': {
                        'field': 'hits_weight',
                        'factor': 1,
                        'missing': 0
                    }
                }).dsl()
            }
        )

        if synonyms:
            for synonym in synonyms[0]['synonyms']:
                synonyms_builder.append(
                    {
                        "function_score": self.titleBaseBuilder(synonym, synonym).function({
                            'field_value_factor': {
                                'field': 'hits_weight',
                                'factor': 2,
                                'missing': 0
                            }
                        }).dsl()
                    }
                )

        builder = (SearchBuilder()
                   .boolQuery()
                   .must([
                       synonyms_builder
                   ]))

        # platform特殊处理
        if store:
            if 'ios' == platform:
                builder.should(
                    SearchBuilder()
                    .term('area_ios', store, 20)
                    .dsl()
                )
            else:
                builder.should(
                    SearchBuilder()
                    .term('area', store, 20)
                    .dsl()
                )

        # 非简版游戏优先
        builder.should(
            SearchBuilder()
            .term('is_style_simple', 'false', 0.2)
            .dsl()
        ).limit(fro, limit)

        if store and platform:
            builder.must([self.storeDslBuilder(platform, store).get('query')])

        query_body = builder.dsl()

        if len(highlight_fields) > 0:
            query_body['highlight'] = self.buildHighlightBody(highlight_fields)

        sort = self.buildSortBody(sort_fields)
        if len(sort) > 0:
            query_body['sort'] = sort

        query_body['track_total_hits'] = True
        return query_body

    def storeDslBuilder(self, platform, store):
        return SearchBuilder().boolQuery().should(
            SearchBuilder()
            .term('store_status.{}.{}'.format(platform, 'default'), const.STORE_STATUS_OPEN)
            .dsl()
        ).should(
            SearchBuilder()
            .term('store_status.{}.{}'.format(platform, store), const.STORE_STATUS_OPEN)
            .dsl()
        ).must_not(
            SearchBuilder()
            .term('store_status.{}.{}'.format(platform, store), const.STORE_STATUS_CLOSED)
            .dsl()
        ).dsl()

    async def recommendByTag(self, limit=10, first_tags=[], second_tags=[], recommended_apps=[]):

        builder = SearchBuilder().boolQuery()

        if first_tags:
            for tag in first_tags:
                builder.should(
                    SearchBuilder()
                    .term('tags', tag, boost=round(random.uniform(0.5, 1), 2), filter=True)
                    .dsl()
                )

        if second_tags:
            count = len(second_tags)
            if count > 5:
                second_tags = random.choices(second_tags, k=5)
            for tag in set(second_tags):
                builder.should(
                    SearchBuilder()
                    .term('tags', tag, boost=round(random.uniform(0.1, 0.6), 2), filter=True)
                    .dsl()
                )

        if recommended_apps:
            builder.must_not(
                SearchBuilder()
                .term('id', list(recommended_apps))
                .dsl()
            )

        builder = (SearchBuilder()
                   .boolQuery()
                   .must([
                       {
                           "function_score": builder.function({
                               'field_value_factor': {
                                   'field': 'hits_weight',
                                   'factor': 1,
                                   'missing': 0
                               }
                           }).dsl()
                       }
                   ]))

        query_body = builder.limit(0, limit).dsl()

        if DEBUG():
            SDEBUG(type='app', dsl=json.dumps(query_body))

        response = await ElsRepository.instance().queryDSL(config("elasticsearch", "app_index"), query_body)

        hits = response['hits']
        return hits

    async def recommendByHeat(self, limit, recommended_apps):

        response = await ElsRepository.instance().queryDSL(config("elasticsearch", "app_index"), {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "terms": {
                                "id": recommended_apps
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "hits_weight": {
                        "order": "desc"
                    }
                }
            ],
            "size": limit
        })

        hits = response['hits']
        return hits

    def titleBaseBuilder(self, kw, search_kw):
        return (SearchBuilder()
                .boolQuery()
                .should(
            SearchBuilder()
                .term('title.keyword', kw, 60)
                .match_phrase('title_not_analyzed', kw, 50)
                .term('clear_title.keyword', search_kw, 25)
                .match_phrase('clear_title', search_kw, 15)
                .match_phrase('title_all',  search_kw, 15)
                .match_phrase('title_standard', search_kw, 10)
                .match('title_standard', {
                    'query': search_kw,
                    'minimum_should_match': '2<70% 20<80%'
                }, 1)
                .match_phrase('title_twogram', search_kw, 10)
                .match_phrase('alias', search_kw, 25)
                .dsl()
                )
                )

    def keywordBaseBuilder(self, kw, orgin_kw, fuzz_re, keyword_feature):
        isalpha = ('isalpha' in keyword_feature) and keyword_feature['isalpha']
        return (SearchBuilder()
                .boolQuery()
                .should(
            SearchBuilder()
                .match_phrase('title', kw)
                .match_phrase('title', {
                    'slop': '2',
                    'query': kw
                }, 0.8)
                .term('title_not_analyzed', orgin_kw, 2)
                .match('title_all', {
                    'query': kw,
                    'minimum_should_match': '1<70% 20<80%',
                    'fuzziness': 1 if fuzz_re.match(kw) else 0
                }, 0.5)
                .match('title_standard', {
                    'query': kw,
                    'minimum_should_match': '2<50% 20<80%'
                }, 0.2)
                .match('title_standard', {
                    'query': kw,
                    'minimum_should_match': '2<80% 20<90%'
                }, 0.5)
                .match('title_twogram', {
                    'query': kw,
                    'minimum_should_match': '100%'
                }, 0 if isalpha else 0.5)
                .match('title_twogram', {
                    'query': kw,
                    'minimum_should_match': '2<80% 20<90%'
                }, 0 if isalpha else 0.3)
                .match('title_twogram', {
                    'query': kw,
                    'minimum_should_match': '2<60% 20<80%'
                }, 0 if isalpha else 0.2)
                .match('alias', {
                    'query': kw,
                    'boost': 0.6,
                    'minimum_should_match': '100%'
                }, 2)
                .match_phrase('author', {
                    'query': kw,
                    'boost': 0.3
                }, 0.4)
                .term('tags', kw)
                .directFilter({
                    'query_string': {
                        'query': kw,
                        'fields': ['title_all'],
                        'minimum_should_match': '100%',
                        'boost': 1
                    }
                })
                .dsl()
                ))

    def buildHighlightBody(self, highlight_fields, start_tag='<b>', end_tag='</b>', fragment=0):
        highlight = {}
        highlight['pre_tags'] = [start_tag]
        highlight['post_tags'] = [end_tag]
        highlight['fields'] = {}
        for hl in highlight_fields:
            highlight['fields'][hl] = {
                'number_of_fragments': fragment
            }
        return highlight

    def buildSortBody(self, sort_fields):
        sort = []
        for sort_field in sort_fields:
            sort.append({
                sort_field: {
                    "order": sort_fields[sort_field]
                }
            })
        return sort

    async def getEmptyResult(self, fro, limit):
        body = json.dumps({
            "query": {
                "term": {
                    "status": 999
                }
            },
            "size": limit,
            "from": fro
        })

        response = await self.es_client.search(
            index=config("elasticsearch", "app_index"),
            body=body
        )

        hits = response['hits']
        return (hits, [])

    async def updateAlias(self, type):
        try:
            if App.index is not None:
                app_indices = await AppService().instance().getIndicesByAlias(type)
                await ElsService.instance().putAlias(App.index, type)
                for index in app_indices:
                    await ElsService.instance().deleteAlias(index, type)
                App.index = None
        except Exception as e:
            log_exception(e)

    def classifyKeywords(self, words):
        app_kws = []
        tag_kws = []
        author_kws = []
        filter_kws = []
        if 'tokens' in words:
            for word in words['tokens']:
                if word['type'] == 'app' and word['token'] not in const.FILTER_APP:
                    app_kws.append(word['token'])
                if word['type'] == 'tag':
                    tag_kws.append(word['token'])
                if word['type'] == 'author' and word['token'] not in const.FILTER_AUTHOR:
                    author_kws.append(word['token'])
                if word['type'] not in const.FILTER_NATURE and '的' != word['token']:
                    filter_kws.append(word['token'])
        return (app_kws, tag_kws, author_kws, filter_kws)

    async def suggestKeywords(self, request: RequestEntity):
        kw = request.getKeyword()
        fro = request.getFrom()
        limit = request.getLimit()
        highlight_fields = request.getHighlight()
        highlight_secret = request.getHighlightSecret()
        kw = suggest_search_amend(kw)

        highlight_fields = FormatService().instance().getHighlightFields(highlight_fields)
        pinyin = ''.join(lazy_pinyin(kw))

        hits, total = await self.fetchAppSuggests(kw, pinyin, request)

        result = []
        titles = []
        suggest_ids = []
        for hit in hits:
            data = {}
            #title = hit['_source']['title']
            title = chooseMatchLanguageTitle(kw, hit['_source']['title'], hit['_source']['title_all'], hit['_source'].get(
                'alias'), hit['_source'].get('pinyin'), hit['_source'].get('abbr'), hit['_source'].get('title_lan'))
            suggest_ids.append(hit['_id'])
            if title in titles:
                continue
            titles.append(title)
            data['title'] = title
            data['highlight'] = {}
            data['id'] = hit['_id']
            if len(highlight_fields) > 0:
                data['highlight']['title'] = [utils.wrapHtml(
                    title, kw, '<b data-secret="' + highlight_secret + '">{}</b>')]
            result.append(data)

        take_illustration_threshold = Stratagem._global[
            'suggest-keyword']['app']['take_illustration_threshold']
        # 小于 take_illustration_threshold 时用图鉴补
        if total < take_illustration_threshold:
            app_ids = await ReviewsKeywordsService.instance().searchAppByKeyword(kw, 0, take_illustration_threshold - total)
            if len(app_ids) > 0:
                app_builder = SearchBuilder().boolQuery().should(
                    SearchBuilder().term('id', app_ids, filter=False).dsl()
                )
                app_query_body = app_builder.limit(fro, limit).dsl()
                app_response = await ElsRepository.instance().queryDSL(config("elasticsearch", "app_index"), app_query_body)

                reviews_result = []
                for hit in app_response['hits']['hits']:
                    #title = hit['_source']['title']
                    title = chooseMatchLanguageTitle(kw, hit['_source']['title'], hit['_source']['title_all'], hit['_source'].get(
                        'alias'), hit['_source'].get('pinyin'), hit['_source'].get('abbr'), hit['_source'].get('title_lan'))
                    app_id = hit['_id']
                    if app_id in suggest_ids:
                        continue
                    index = app_ids.index(app_id)
                    data = {}
                    data['title'] = kw + ' ' + title
                    data['highlight'] = {}
                    data['id'] = app_id
                    if len(highlight_fields) > 0:
                        data['highlight']['title'] = [utils.wrapHtml(
                            title, kw, '<b data-secret="' + highlight_secret + '">{}</b>')]
                    reviews_result.insert(index, data)
                result.extend(reviews_result)
                total = total + len(reviews_result)

        # 把多取的去掉
        return result[:limit], total

    def analysisKeywords(self, words):
        analysis_kws = []
        for word in words:
            w = word['word']
            if w.strip() == '':
                continue
            analysis_kws.append(w)
        return analysis_kws

    async def searchAppByDeveloperId(self, id, limit=10):
        response = await self.es_client.search(
            index=config("elasticsearch", "app_index"),
            body={
                "query": {
                    "term": {
                        "developer_id": id
                    }
                },
                "size": limit,
                "sort": [
                    {
                        "hits_weight": {
                            "order": "desc"
                        }
                    }
                ]
            }
        )

        hits = response['hits']
        return hits

    async def fetchAppSuggests(self, kw, pinyin, request: RequestEntity):
        """根据关键词在app_index中搜索游戏（app） 规则：优先命中前缀词匹配->词匹配->拼音前缀->拼音匹配"""
        platform = request.getPlatform()
        store = request.getStore()
        fro = request.getFrom()
        limit = request.getLimit()
        _kw = kw.lower()
        identify_synonym_info = await IdentifyService().synonymIdentify(_kw)
        builder = (
            SearchBuilder()
            .prefix('title_not_analyzed', _kw, boost=80)
            .prefix('alias.keyword', _kw, boost=50)
            .matchPhrasePrefix('title_standard', _kw, boost=30)
            .prefix('full_pinyin', pinyin, boost=20)
            .wildcard('full_pinyin', f'*{pinyin}', boost=10)
            .match_phrase('pinyin', f'{pinyin}', boost=10)
            .wildcard('abbr', f'*{_kw}*', boost=10)
        )
        if identify_synonym_info:
            _kw_synonym = identify_synonym_info[0]['synonyms'][0]
            builder = (
                SearchBuilder()
                .prefix('title_not_analyzed', _kw, boost=80)
                .prefix('title_not_analyzed', _kw_synonym, boost=80)
                .prefix('alias.keyword', _kw, boost=50)
                .prefix('alias.keyword', _kw_synonym, boost=50)
                .matchPhrasePrefix('title_standard', _kw, boost=30)
                .prefix('full_pinyin', pinyin, boost=20)
                .wildcard('full_pinyin', f'*{pinyin}', boost=10)
                .match_phrase('pinyin', f'{pinyin}', boost=10)
                .wildcard('abbr', f'*{_kw}*', boost=10)
            )

        dsl_builder = (
            SearchBuilder().boolQuery()
            .must([SearchBuilder().boolQuery().should(builder.dsl()).dsl().get('query')])
            .must([self.storeDslBuilder(platform, store).get('query')])
            # .sort([{"hits_weight_suggest": "desc"}])
        )
        fs = (SearchBuilder()
              .functionScore(dsl_builder, field_value_factor={"field": "hits_weight"})
              # 取的时候额外取 take_more个, 防止去重之后个数不够
              .limit(fro, limit + Stratagem._global['suggest-keyword']['mix']['take_more']))

        response = await ElsRepository.instance().queryDSL(config("elasticsearch", "app_index"), fs.dsl())
        return response['hits']['hits'], response['hits']['total']['value']
