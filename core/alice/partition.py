# -*- coding: utf8 -*-
from core.alice.component.view_component import ViewComponent
from core.alice.component.util_component import UtilComponent

class Partition:
    name = None

    def __init__(self):
        self.C = []
        self.view_component = None
        self._watched = {}
        self._dislike = {}
        self._capacity = 0


    def setWatched(self, _watched: set):
        self._watched = _watched

    def setDislike(self, dislike: set):
        self._dislike = dislike

    @classmethod
    def register(cls, *components):
        assert type(components[0]) == ViewComponent
        assert isinstance(components[1], UtilComponent) == True
        self = cls()
        self.view_component = components[0]
        self.util_component = components[1]
        for component in components:
            self.C.append(component)
        return self


    def capacity(self, _capacity):
        self._capacity = _capacity
        return self

    def getCapcity(self):
        return self._capacity


    async def predict(self):
        generate_result = await self.view_component.strategy.remove_ids(self._watched, self._dislike).generate()
        strategiess = [generate_result]
        self.util_component.setStrategiesCount(len(strategiess))
        dataSet = [i.data for i in strategiess]
        return self.util_component.draw(dataSet, self.getCapcity(), self._watched)
