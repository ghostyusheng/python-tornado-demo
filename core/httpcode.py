# -*- coding: utf8 -*-

import json
from tornado.web import RequestHandler


class HTTPCode:
    HTTP_FAIL_SERVER_CODE = 500
    HTTP_FAIL_CLIENT_CODE = 400
    HTTP_SUCCESS_CODE = 200

    SUCCESS_CODE = 0
    SUCCESS_MSG = 'SUCCESS'

    FAIL_CODE = 9999
    FAIL_MSG = 'FAIL'
