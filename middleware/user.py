from function.context import context_var
from service.AbConfig import AbConfigService


class UserMiddleware:

    @classmethod
    async def checkUserId(cls, handler):
        try:
            user_id = handler.get_argument('user_id', None)
            context_var.get().USER_ID = user_id
            await cls.identifyUser()
        except Exception as e:
            raise e

    @classmethod
    async def identifyUser(cls):
        BUCKET = set()
        ctx = context_var.get()
        user_id = ctx.USER_ID
        if user_id in [None, '']:
            ctx.BUCKET = list(BUCKET)
            return
        ab_config = await AbConfigService.instance().searchAll()
        if ab_config is None or len(ab_config) == 0:
            ctx.BUCKET = list(BUCKET)
            return
        user_id = int(user_id)
        user_id_tail = user_id % 10
        for strategy in ab_config:
            (S, buckets, gray_uids) = strategy.get('name'), strategy.get('buckets'), strategy.get('gray_uids')
            if buckets and (user_id_tail in buckets):
                BUCKET.add(S)
            if gray_uids and (user_id in gray_uids):
                BUCKET.add(S)
        ctx.BUCKET = list(BUCKET)
