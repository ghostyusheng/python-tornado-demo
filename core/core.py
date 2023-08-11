import argparse
import asyncio
from os import getenv, getpid
import aiomysql
from aiomysql import DictCursor
from elasticapm import Client
from motor import MotorClient
from core.const import const
from function.function import baseDir, envCn
from function.function import config
from service.log import LogService
from share.repository.els import ElsRepository
from share.service import cache


class CoreDriver:
    """
    主进程共享资源
    """

    def __init__(self):
        self.init_cmd()
        self.init_env()


    def init_cmd(self):
        parser = argparse.ArgumentParser(description='yeehagame')
        parser.add_argument('--apm', '-a', help='apm监控，调试阶段可以传0', default=False)
        parser.add_argument('--boot', '-b', help='module(search/log,default:all)', default=None)
        self.args = parser.parse_args()
        const.BOOT = self.args.boot


    def init_env(self):
        const.BASE_DIR = baseDir()
        const.ENV = getenv('ENV') if getenv('ENV') else 'testing'
        const.LOG_DIR = const.BASE_DIR + '/log'
        const.run_process_count = int(getenv('PROCESS_COUNT'))  if getenv('PROCESS_COUNT') else 10
        const.HOSTNAME = getenv('HOSTNAME') if getenv('HOSTNAME') else ''
        LogService.initLogDir(const.LOG_DIR)
        self.dump()
        self.init_es_conn()
        self.init_cache_service()
        #self.init_redshift()
        # self.initJavaNlpPool()

    def init_apm(self):
        print('args: ', self.args)
        if not int(self.args.apm):
            const.APM = None
            print('APM 不启动')
            return
        const.APM = Client({
            'SERVICE_NAME': 'eng-server',
            'SERVER_URL': config('apm', 'conn')
        })
        print('APM ', const.APM.config.server_url)
        
    def init_sentry(self):
        const.SENTRY = None
        #sentry_sdk.init(
        #    dsn=config("sentry", "url"),
        #    integrations=[TornadoIntegration(), RedisIntegration(), AioHttpIntegration()]
        #)
        #const.SENTRY = sentry_sdk

    # def initJavaNlpPool(self):
        # const.JAVA_NLP_CONN = HTTPConnectionPool(config('analysis', 'url'), maxsize=200, block=False, timeout=0.3)

    def dump(self):
        print('进程PID : %d 当前环境 ---> %s ' % (getpid(), const.ENV))


    #def init_redshift(self):
    #    r = "redshift"
    #    const.red = redshift_connector.connect(
    #         host=config(r, "host")
    #         database=config(r, "dbname"),
    #         user=config(r, 'user'),
    #         password=config(r, 'password')
    #    )

    async def init_search_conn(self):
        uri = config("database", "mongo_uri")
        const.search_mongo_conn = MotorClient(uri, readPreference='secondaryPreferred')


    async def init_ailab_conn(self):
        uri = config("database", "ailab_mongo_uri")
        const.ailab_mongo_conn = MotorClient(uri, readPreference='secondaryPreferred')

    async def init_feature_conn(self):
        uri = config("database", "feature_mongo_uri")
        const.feature_mongo_conn = MotorClient(uri, readPreference='secondaryPreferred')

    def init_mongo_conn(self):
        asyncio.ensure_future(self.init_search_conn())
        if envCn() or (const.ENV == 'testing'):
            asyncio.ensure_future(self.init_ailab_conn())
            asyncio.ensure_future(self.init_feature_conn())

    def init_cache_service(self):
        if const.ENV=='product':
            const.cache_service = cache.CacheService(config("redis", "redis_host"), config("redis", "redis_port"),
                                               config("redis", "redis_password"), True)
        else:
            const.cache_service = cache.CacheService(config("redis", "redis_host"), config("redis", "redis_port"),
                                               config("redis", "redis_password"))

    def init_es_conn(self):
        const.es = ElsRepository(config("elasticsearch", "es_conn").split(','))

