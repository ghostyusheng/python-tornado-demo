from core.alice.strategy.strategies import Strategies
from functools import wraps

class Strategy:
    
    def __init__(self):
        self.strategies = Strategies()


    def prepare(self):
        pass


    def through(self, strategy):
        self.strategies.setMod(Strategies.THROUGH)
        self.strategies.append(strategy)
        return self


    def alternant(self, strategy):
        self.strategies.setMod(Strategies.ALTERNANT)
        self.strategies.append(strategy)
        return self

    def remove_ids(self,ids, dislike):
        self._remove_ids = ids
        self._dislike_ids = dislike
        return self


    async def generate(self):
        self.strategies.insert(0, self)
        if not self.strategies.getMod():
            self.strategies.setMod(Strategies.DEFAULT)
        await self.strategies.apply(self._remove_ids, self._dislike_ids)
        #print(self.strategies)
        return self.strategies

