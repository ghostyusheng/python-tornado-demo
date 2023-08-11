import tornado
import time
from tornado.escape import json_decode, to_unicode
from tornado.web import RequestHandler

from core.entity.request import RequestEntity
from core.entity.sdebug import SdebugEntity
from core.http import HTTP
from function.context import RequestContext
from utils.redis_key_tool import *
from utils.utils import log_exception,syslog


class BaseHandler(RequestHandler):

    def initialize(self):
        arguments = self.request.arguments
        ctx = RequestContext()
        http_message = \
            ' '.join([k + "|" + str(v[0], encoding="utf-8") for k, v in arguments.items()]) + \
            ' '.join([k + "|" + str(v[0], encoding="utf-8")
                      for k, v in self.request.body_arguments.items()])
        http_message += self.request.body.decode('utf-8')
        ctx.HTTP_MESSAGE = http_message

        if 'SDEBUG' in arguments.items():
            if self.get_argument('SDEBUG') == '1':
                ctx.SEARCH_DEBUG = True
        else:
            ctx.SEARCH_DEBUG = self.get_argument('SDEBUG', False)
        ctx.HANDER = self
        ctx._REQUEST = RequestEntity(arguments, self.request.body_arguments)
        ctx._SDEBUG = SdebugEntity()
        ctx.HTTP_START_TS = time.time()
        context_var.set(ctx)
        syslog(http_message)

    def out(self, *args, **kwargs):
        HTTP(self).out(*args, **kwargs)

    def debug(self, what):
        context_var.get().HANDER.finish(what)

    def get_json_argument(self, name, default=None):
        args = json_decode(self.request.body)
        name = to_unicode(name)
        if name in args:
            return args[name]
        elif default is not None:
            return default
        else:
            raise tornado.web.MissingArgumentError(name)

    def write_error(self, status_code, **kwargs):
        msg = str(kwargs.get('exc_info')[1])
        log_exception(Exception("[uncatched exception]: " + msg))
        self.out(msg=msg, httpcode=status_code)
