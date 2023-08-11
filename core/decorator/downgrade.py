import functools


class Downgrade:

    def __init__(self):
        pass


    @classmethod
    def set(cls):
        return cls()

    @classmethod
    async def downgradeServiceCount(cls, key):
        pass

    @classmethod
    async def ServiceIsDown(cls, key):
        pass

    @classmethod
    def wrapper(cls, func):
        @functools.wraps(func)
        async def wraps(*args, **kwargs):
            pass
