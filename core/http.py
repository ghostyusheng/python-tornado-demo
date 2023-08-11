# -*- coding: utf8 -*-

import json

from core.httpcode import HTTPCode
from function.context import context_var
from utils.utils import log_exception,syslog


class HTTP:
    _handler = None

    def __init__(self, request_handler):
        self._handler = request_handler

    def out(self, data=[], code=HTTPCode.SUCCESS_CODE, msg=HTTPCode.SUCCESS_MSG, httpcode=HTTPCode.HTTP_SUCCESS_CODE):
        """
        自定义错误信息
        默认 内部失败 CODE 9999
        默认 HTTP失败 CODE 500
        """
        self._handler.set_header("Content-Type", "application/json")
        if msg != HTTPCode.SUCCESS_MSG:
            code = HTTPCode.FAIL_CODE
            self._handler.set_status(HTTPCode.HTTP_FAIL_SERVER_CODE)
            if httpcode != HTTPCode.HTTP_SUCCESS_CODE:
                self._handler.set_status(httpcode)

        response = {
            'code': code,
            'msg': msg,
            'data': data
        }
        context_var.set(None)
        self._handler.finish(json.dumps(response, ensure_ascii=False))
