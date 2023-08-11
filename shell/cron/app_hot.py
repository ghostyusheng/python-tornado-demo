# -*- coding:utf-8 -*-
import sys
from collections import defaultdict

from cron.base import Base
from elasticsearch import Elasticsearch, helpers
from core.const import const
from share.utils.utils import transNumSuffix
from utils import utils
import datetime
import time
from function.function import config
#from dtools.access import DB


class AppHot(Base):
    """DEMO"""

    def init(self):
        pass


    def _run(self, params=[]):
        print('finish')
