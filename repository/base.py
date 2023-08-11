# -*- coding:utf-8 -*-


class BaseRepository:
    _instance = None

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance
