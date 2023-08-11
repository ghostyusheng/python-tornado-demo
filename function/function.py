from os.path import dirname, join, abspath
from function.context import context_var
from service.config import ConfigService
from core.const import const


def envHk():
    return const.ENV == 'hkproduct'


def envCn():
    return const.ENV == 'product'


def config(section, key):
    return ConfigService.instance().getConfig(section, key)


def baseDir():
    return abspath(join(dirname(__file__), '..'))


def R():
    r = getattr(context_var.get(), '_REQUEST', None)
    if r is None:
        print("requestEntity is None!!!")
    return r


def SDEBUG(**kwargs):
    LEN = len(kwargs)
    if LEN == 2:
        typ = kwargs['type']
        dsl = kwargs['dsl']
        return setattr(context_var.get()._SDEBUG.dsl, typ, dsl)
    elif LEN == 0:
        return context_var.get()._SDEBUG.dsl.__dict__
    else:
        raise Exception('sdebug params error')


def DEBUG():
    return context_var.get().SEARCH_DEBUG


def GET(*args):
    """
    @param: key
    @param: default_value
    """
    LEN = len(args)
    if LEN == 1:
        key = args[0]
        return getattr(R().GET, key)
    elif LEN == 2:
        key, default = args
        return getattr(R().GET, key, default)
    else:
        raise Exception('GET param error')


def POST(key):
    return getattr(R().POST, key)
