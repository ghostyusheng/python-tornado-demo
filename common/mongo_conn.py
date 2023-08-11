#!/usr/bin/env python
# -*- coding:utf-8 -*-

from motor.motor_tornado import MotorClient
from function.function import config
from pymongo import MongoClient


class MongoConn:
    # 连接池对象
    __mongoclient = None

    @staticmethod
    def _mongoconn():
        if MongoConn.__mongoclient is None:
            uri = config("database", "mongo_uri")
            client = MotorClient(uri, readPreference='secondaryPreferred')
            MongoConn.__mongoclient = client
        return MongoConn.__mongoclient


    @staticmethod
    def _sync_mongoconn():
        if MongoConn.__mongoclient is None:
            uri = config("database", "mongo_uri")
            client = MongoClient(uri)
            MongoConn.__mongoclient = client
        return MongoConn.__mongoclient
