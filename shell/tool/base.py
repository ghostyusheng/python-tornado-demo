from os.path import dirname, join, abspath

import sys

DIR = abspath(join(dirname(__file__), '../..'))
sys.path.append(DIR)

from core.const import const
from core.core import CoreDriver
from configparser import ConfigParser





def config(section, key):
    config = ConfigParser()
    path = '%s/config/%s.ini' % (const.BASE_DIR, const.ENV)
    config.read(path)
    return config.get(section, key)


class Base:
    """基础类"""

    def __init__(self):
        CoreDriver()
        self.initEnv()

    def initEnv(self):
        const.HADOOP = config('hadoop', 'hadoop')
        const.ELS = config('elasticsearch', 'es_conn').split(',')

    def __repr__(self):
        print(globals()[type(self).__name__].__doc__)
        return ''
