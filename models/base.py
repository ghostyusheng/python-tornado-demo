from repository.db import DBRepository


class BaseModel:
    _instance = None
    _name = None
    _columns = None

    def __init__(self):
        self.db_service = DBRepository()

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    async def fetchColumns(self):
        if self._name is None:
            raise Exception("table not defined")
        return await self.db_service.fetchColumns(self._name)

    async def validate(self, dct):
        if self._columns is None:
            self._columns = await self.fetchColumns()
        remove_fields = [
            field for field in dct.keys() if field not in self._columns]
        for field in remove_fields:
            dct.pop(field)
        return dct

    async def replaceOneByDict(self, dct):
        if self._name is None:
            raise Exception("insert table not defined")
        dct = await self.validate(dct)
        if len(dct) <= 1:
            return
        await self.db_service.replaceOneByDict(self._name, dct)
        return

    async def updateOneByDict(self, dct):
        if self._name is None:
            raise Exception("update table not defined")
        dct = await self.validate(dct)
        if len(dct) <= 1:
            return
        await self.db_service.updateOneByDict(self._name, dct)

    async def selectOneById(self, _id):
        sql = "select * from `{}` where `id` = '{}'".format(self._name, _id)
        return await self.db_service.selectOne(sql)

    async def deleteById(self, _id):
        await self.db_service.deleteById(self._name, _id)
