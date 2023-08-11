#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
from function.function import config


class AppConn:

    # 连接池对象
    __pool = None

    def app_conn(self):
        if AppConn.__pool is None:
            AppConn.__pool = PooledDB(
                creator=pymysql,
                mincached=5,
                maxcached=20,
                host=config("database", "mysql_app_ip"),
                port=int(config("database", "mysql_app_port")),
                user=config("database", "mysql_app_user"),
                passwd=config("database", "mysql_app_password"),
                db=config("database", "mysql_app_dbname"),
                use_unicode=False,
                charset="utf8",
                cursorclass=DictCursor
            )
        return AppConn.__pool.connection()
