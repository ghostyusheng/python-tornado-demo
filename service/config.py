# -*- encoding:utf-8 -*-

from configparser import ConfigParser

from core.const import const
from service.base import BaseService


class ConfigService(BaseService):

    def __init__(self):
        self.path = '%s/config/%s.ini' % (const.BASE_DIR, const.ENV)
        self.config = self.loadConfig(self.path)

    def loadConfig(self, path):
        self.config = ConfigParser()
        self.config.read(path)
        return self.config

    def getConfig(self, section, key):
        if self.config is None:
            self.config = self.loadConfig(self.path)
        return self.config.get(section, key)

    def setConfig(self, section, key, value):
        self.config.set(section, key, value)

