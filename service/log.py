# -*- coding: utf-8 -*-

import logging
import os
import json
from function.function import config
from core.const import const
from service.base import BaseService
from utils.http import Http
from tornado.httpclient import HTTPRequest
from urllib.parse import urlencode


class LogService(BaseService):
    _instance = None
    size = 1024 * 1024 * 512
    backup_count = 10
    encoding = 'utf-8'
    template = '%(asctime)s --- %(levelname)s --- %(message)s'
    file_handler = None
    stream_handler = None

    @classmethod
    def initLogDir(cls, log_path):
        if(os.path.exists(log_path) == False):
            os.makedirs(log_path)

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def user(self, user_id):
        self.template = '%(asctime)s --- USER>{0} --- %(levelname)s --- %(message)s'.format(
            user_id)
        return self

    def setLogName(self, filename):
        self.logfile = "%s/%s.log" % (const.LOG_DIR, filename)
        return self

    def addFileHandler(self):
        formatter = logging.Formatter(self.template)
        file_handler = logging.handlers.RotatingFileHandler(
            self.logfile,
            maxBytes=self.size,
            backupCount=self.backup_count,
            encoding=self.encoding
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.file_handler = file_handler
        self.logger.addHandler(file_handler)
        return self

    def addStreamHandler(self):
        stream_handler.setLevel(logging.WARNING)
        stream_handler.setFormatter(formatter)
        self.stream_handler = stream_handler
        self.logger.addHandler(stream_handler)
        return self

    def release(self):
        if self.file_handler:
            self.logger.removeHandler(self.file_handler)
        if self.stream_handler:
            self.logger.removeHandler(self.stream_handler)

    def info(self, msg):
        self.logger.info(msg)
        self.release()

    def exception(self, e):
        self.logger.exception(e)
        self.release()

    def eslog(self, msg, typ):
        if const.ENV != 'product':
            return
        try:
            msg = json.dumps(msg, ensure_ascii=False)
            rsp = Http.async_fetch(HTTPRequest(
                config('log', typ + '_remote_url'),
                method="POST",
                body=urlencode({'msg': msg}),
                connect_timeout=0.1,
                request_timeout=0.1
            ))
        except Exception as e:
            LogService.instance().setLogName('sys').addFileHandler().exception(e)
            print('eslog  异常')
