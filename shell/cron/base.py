import sys
from os import getenv
from os.path import dirname, join, abspath

DIR = abspath(join(dirname(__file__), '../..'))
sys.path.append(DIR)

from core.const import const
from configparser import ConfigParser





def config(section, key):
    config = ConfigParser()
    path = '%s/config/%s.ini' % (const.BASE_DIR, const.ENV)
    config.read(path)
    return config.get(section, key)


class Base:
    """基础类"""

    def __init__(self):
        self.initEnv()

    def initEnv(self):
        const.BASE_DIR = abspath(join(dirname(__file__), '../..'))
        const.LOG_DIR = const.BASE_DIR + '/log'
        const.ENV = getenv('ENV') if getenv('ENV') else 'testing'
        const.HADOOP = config('hadoop', 'hadoop')
        const.ELS = config('elasticsearch', 'es_conn').split(',')


    def __repr__(self):
        print(globals()[type(self).__name__].__doc__)
        return ''
