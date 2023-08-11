from core.entity.request import RequestEntity
from models.base import BaseModel


class AppModel(BaseModel):
    _name = 'app'

    def __init__(self):
        super().__init__()


class AppSearchRequest:
    def __init__(self, request: RequestEntity):
        self._kw = request.getKeyword()
        self._fro = request.getFrom()
        self._limit = request.getLimit()
        self._uid = request.getUserId()
        self._device_id = request.getDeviceId()
        self._store = request.getStore()
        self._platform = request.getPlatform()
        self._highlight_fields = request.getHighlight()
        self._sort_fields = request.getSort()
        self._filter = request.getFilter()
        self._session_id = request.getSessionId()

        self._filtered_kw = request.getKeyword()
        self._keyword_feature = {}
        self._stratagem = None
        self._top_apps = []
        self._synonyms = []
        self._not_show_ids = []

    @property
    def kw(self):
        return self._kw

    @kw.setter
    def kw(self, value):
        self._kw = value

    @property
    def fro(self):
        return self._fro

    @fro.setter
    def fro(self, value):
        self._fro = value

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        self._limit = value

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, value):
        self._uid = value

    @property
    def device_id(self):
        return self._device_id

    @device_id.setter
    def device_id(self, value):
        self._device_id = value

    @property
    def filter(self):
        return self._filter

    @filter.setter
    def filter(self, value):
        self._filter = value

    @property
    def filtered_kw(self):
        return self._filtered_kw

    @filtered_kw.setter
    def filtered_kw(self, value):
        self._filtered_kw = value

    @property
    def highlight_fields(self):
        return self._highlight_fields

    @highlight_fields.setter
    def highlight_fields(self, value):
        self._highlight_fields = value

    @property
    def keyword_feature(self):
        return self._keyword_feature

    @keyword_feature.setter
    def keyword_feature(self, value):
        self._keyword_feature = value

    @property
    def not_show_ids(self):
        return self._not_show_ids

    @not_show_ids.setter
    def not_show_ids(self, value):
        self._not_show_ids = value

    @property
    def platform(self):
        return self._platform

    @platform.setter
    def platform(self, value):
        self._platform = value

    @property
    def session_id(self):
        return self._session_id

    @session_id.setter
    def session_id(self, value):
        self._session_id = value

    @property
    def sort_fields(self):
        return self._sort_fields

    @sort_fields.setter
    def sort_fields(self, value):
        self._sort_fields = value

    @property
    def store(self):
        return self._store

    @store.setter
    def store(self, value):
        self._store = value

    @property
    def strategem(self):
        return self._strategem

    @strategem.setter
    def strategem(self, value):
        self._strategem = value

    @property
    def synonyms(self):
        return self._synonyms

    @synonyms.setter
    def synonyms(self, value):
        self._synonyms = value

    @property
    def top_apps(self):
        return self._top_apps

    @top_apps.setter
    def top_apps(self, value):
        self._top_apps = value
