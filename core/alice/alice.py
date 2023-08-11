# -*- coding: utf8 -*-
from core.alice.partition import Partition


class Alice:
    valid_focus = ['app', 'topic', 'video']  
    _focus = None

    def __init__(self,user_id):
        self.P = {}
        self.user_id = user_id
        self._rerank_handler = None
        self._watched = set()
        self._dislike = set()


    def focus(self, _focus: str):
        self._focus = _focus
        self.validateElement(self._focus)
        return self


    def watched(self, _watched: set):
        if _watched is None:
            _watched = set()
        self._watched = set(map(lambda x: str(x), _watched))
        return self

    def dislike(self, _dislike: set):
        if _dislike is None:
            _dislike = set()
        self._dislike = set(map(lambda x: str(x), _dislike))
        return self

    
    def partition(self, name: str, partition: Partition):
        self.P[name] = partition
        return self


    def rerank(self, handler):
        self._rerank_handler = handler
        return self


    def validateElement(self, _focus: str):
        assert _focus in self.valid_focus
        return self


    def fitting(self):
        return self

    async def predict(self) -> list:
        data = []
        unique_data = []
        F = {}
        sum_capacity = 0
        watched_keys = set(self._watched)
        dislike_keys = set(self._dislike)
        len_partition = len(self.P.items())
        count = 0
        for name, p in self.P.items():
            sum_capacity += p.getCapcity()
            count += 1
            if count == len_partition:
                empty_compensation = sum_capacity - len(unique_data)
                p.capacity(empty_compensation)
            p.setWatched(watched_keys)
            p.setDislike(dislike_keys)
            data = await p.predict()
            for i in data:
                pk = i['pk']
                watched_keys.add(str(pk))
                unique_data.append(i)

        if self._rerank_handler:
            rerank_data = await self._rerank_handler.prepare(unique_data)
        else:
            rerank_data = unique_data

        return rerank_data
