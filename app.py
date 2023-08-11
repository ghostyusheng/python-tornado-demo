#coding: utf8
import tornado
from tornado import ioloop, httpserver, web
from functools import reduce

from core.const import const
from core.core import CoreDriver
from function.function import config
from handlers._health import _HealthHandler
from handlers.user_search import UserSearchHandler

ALL = [
    (r'/_health', _HealthHandler),
    (r'/search/user', UserSearchHandler)
]


def router():
    routes = ALL
    return routes


def main():
    CoreDriver()
    is_debug = int(config('sys', 'debug'))
    const.DEBUG = is_debug
    print('DEBUG', is_debug)
    if const.ENV == 'product':
        app = tornado.web.Application(
            router(),
        )
    else:
        app = tornado.web.Application(
            router(),
            debug=is_debug
        )
    http_server = tornado.httpserver.HTTPServer(app)
    port = 2002
    if not const.DEBUG and const.ENV == 'product':
        print('使用多进程启动模式 : %d' % const.run_process_count)
        #http_server.bind(port, backlog=1000) # macos use bind
        http_server.listen(port) # linux use listen
        http_server.start(const.run_process_count)
    else:
        print('使用单进程启动模式')
        http_server.listen(port)
    CoreDriver().init_apm()  # apm after fork
    #CoreDriver().init_sub()
    #CoreDriver().init_sentry()
    print('监听 {}'.format(port))
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
