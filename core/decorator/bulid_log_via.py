import json
from functools import wraps


class BuildLogVia:

    def __init__(self, strategy):
        self.strategy = strategy

    @classmethod
    def setStrategyName(cls, strategy):
        return cls(strategy)

    def __call__(self, func):
        @wraps(func)
        async def wrap(*args, **kwargs):
            res = await func(*args, **kwargs)
            if res is None:
                res = []
            nres = []
            for index, i in enumerate(res):
                tmp = i
                tmp['logVia'] = json.dumps({
                    'strategy': self.strategy,
                    'strategy_pos': index
                }).replace(':', '#')
                nres.append(i)
            return nres
        return wrap
