# -*- coding:utf-8 -*-

import aiomysql
from aiomysql import DictCursor

from function.function import config
from repository.base import BaseRepository
from utils.utils import log_exception

import redshift_connector
import pandas
import numpy


class DBRepository(BaseRepository):
    __pool = None

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    async def initPool(cls):
        if cls.__pool is None:
            cls.__pool = redshift_connector.connect(
                 host='dev.data.org',
                 database='test',
                 user='mydbadmin',
                 password='TestQa57lx'
            )
        return cls.__pool

    @classmethod
    async def execute(cls, sql):
        if cls.__pool is None:
            await cls.initPool()
        try:
            async with cls.__pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql)
                    await conn.commit()
        except Exception as e:
            await conn.rollback()
            log_exception(e)
            raise e
        return True

    @classmethod
    async def queryAll(cls, sql):
        if cls.__pool is None:
            await cls.initPool()
        try:
            cursor = cls.__pool.cursor()
            cursor.execute(sql)
            result: pandas.DataFrame = cursor.fetch_dataframe()
            return result
        except Exception as e:
            log_exception(e)
            raise e

    @classmethod
    async def queryOne(cls, sql):
        if cls.__pool is None:
            await cls.initPool()
        try:
            async with cls.__pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql)
                    return await cur.fetchone()
        except Exception as e:
            log_exception(e)
            raise e

    @classmethod
    async def selectOne(cls, sql):
        return await cls.queryOne(sql)

    @classmethod
    async def selectAll(cls, sql):
        return await cls.queryAll(sql)

    # todo: check security
    @classmethod
    async def replaceOneByDict(cls, table, dct):
        try:
            for key, val in dct.items():
                if type(val) == list:
                    dct[key] = ','.join(map(str, val))
                if val is None:
                    dct[key] = 0
                if type(val) == bool:
                    dct[key] = int(val)
            _fields = '`' + '`,`'.join(dct.keys()) + '`'
            _values = ','.join(list(map(lambda x: "'{}'".format(
                str(x).replace("\\", "").replace(r"'", '"')
            ), dct.values())))
            _update = ','.join(["`{}`='{}'".format(i, 
                str(j).replace("\\", "").replace(r"'", '"')
            ) for i, j in dct.items()])
            sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
                table, _fields, _values, _update)
            await cls.execute(sql)
        except Exception as e:
            log_exception('SQL ERROR =======> ' + sql)
            raise e

    @classmethod
    async def updateOneByDict(cls, table, dct, key='id'):
        try:
            _id = int(dct['id'])
            dct.pop('id')
            for key, val in dct.items():
                if type(val) == list:
                    dct[key] = ','.join(map(str, val))
                if val is None:
                    dct[key] = 0
                if type(val) == bool:
                    dct[key] = int(val)
            _update = ','.join(["`{}`='{}'".format(i,  str(j).replace("\\", "").replace(r"'", '"'))
                                for i, j in dct.items()])
            sql = "update `{0}` set {1} where `id` = {2}".format(
                table, _update, _id)
            await cls.execute(sql)
        except Exception as e:
            log_exception('SQL ERROR =======> ' + sql)
            raise e

    @classmethod
    async def deleteById(cls, table, _id):
        _id = int(_id)
        sql = "delete from `{0}` where `id` = '{1}'".format(table, _id)
        await cls.execute(sql)
        return

    @classmethod
    async def getById(cls, table, _id):
        _id = int(_id)
        sql = "select * from `{0}` where `id` = '{1}'".format(table, _id)
        print(sql)
        res = await cls.queryOne(sql)
        res = {i: str(j, "utf-8") if type(j) ==
               bytes else j for i, j in res.items()}
        return res

    @classmethod
    async def fetchColumns(cls, _name):
        sql = "desc %s" % _name
        description = await cls.queryAll(sql)
        columns = [record['Field'] for record in description]
        return columns
