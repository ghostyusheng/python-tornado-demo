import copy

from tornado.web import MissingArgumentError
from core.entity.request import RequestEntity
from core.httpcode import HTTPCode
from function.function import R
from handlers.base import BaseHandler
from middleware.request import RequestMiddleware
from service.app import AppService
from service.developer import DeveloperService
from service.event import EventService
from service.group import GroupService
from service.hot_keywords import HotKeywordsService
from service.index import IndexService
from service.mix import MixService
from service.moment import MomentService
from service.simple_event import SimpleEventService
from service.topic import TopicService
from service.video import VideoService
from utils import utils
from utils.utils import log_exception, syslog


def getServiceInstance(service_name: str):
    if service_name == "DeveloperService":
        return DeveloperService.instance()
    elif service_name == "EventService":
        return EventService.instance()
    elif service_name == "SimpleEventService":
        return SimpleEventService.instance()
    elif service_name == "TopicService":
        return TopicService.instance()
    elif service_name == "VideoService":
        return VideoService.instance()
    elif service_name == "HotKeywordsService":
        return HotKeywordsService.instance()
    elif service_name == "AppService":
        return AppService.instance()
    elif service_name == "MixService":
        return MixService.instance()
    elif service_name == 'GroupService':
        return GroupService.instance()
    elif service_name == 'MomentService':
        return MomentService.instance()
    else:
        raise Exception("forbbiden service name")

# 同步数据的入口


class IndexUpdateAllHandler(BaseHandler):

    async def post(self, doc_id):
        try:
            data = self.get_json_argument('data')
            mod = self.get_json_argument('type')
            syslog('replace from %s,id:%s' % (mod, str(doc_id)))
            service_name = "{}Service".format(utils.capitalize(mod))
            service = getServiceInstance(service_name)

            property = utils.getIndex(utils.capitalize(mod))
            properties = property.properties['properties']

            index_data = copy.deepcopy(data)
            for k, v in data.items():
                if k not in properties:
                    index_data.pop(k)
                    log_exception(
                        'replace from %s illegal field %s' % (mod, k))

            await service.instance().replaceIndex(index_data, doc_id)
            self.out()
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])


class IndexUpdateHandler(BaseHandler):

    async def post(self, doc_id):
        try:
            data = self.get_json_argument('data')
            mod = self.get_json_argument('type')
            syslog('update from %s,id:%s,data:%s' % (mod, str(doc_id), data))
            service_name = "{}Service".format(utils.capitalize(mod))
            service = getServiceInstance(service_name)
            await service.instance().updateIndex(data, doc_id)
            self.out()
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])


class IndexDeleteByIdHandler(BaseHandler):

    async def post(self, doc_id):
        try:
            mod = self.get_json_argument('type')
            syslog('delete from %s,id:%s' % (mod, str(doc_id)))
            service_name = "{}Service".format(utils.capitalize(mod))
            service = getServiceInstance(service_name)
            await service.instance().deleteById(doc_id)
            self.out()
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])


class AliasUpdateHandler(BaseHandler):

    async def post(self):
        try:
            mod = self.get_json_argument('type')
            if 'app' == mod:
                await AppService().instance().updateAlias(mod)
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])


class IndexCreateHandler(BaseHandler):

    async def get(self):
        try:
            RequestMiddleware.verifyParams([
                ['index']
            ])
            request: RequestEntity = R()
            index = request.getIndex()
            await IndexService.instance().createIndexWithAlias(index)
            self.out(msg="SUCCES")
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])


class IndexReindexHandler(BaseHandler):

    async def get(self):
        try:
            RequestMiddleware.verifyParams([
                ['alias']
            ])
            request: RequestEntity = R()
            alias = request.getAlias()
            await IndexService.instance().makeReindex(alias)
            self.out(msg="SUCCESS")
        except MissingArgumentError as e:
            syslog(e.log_message)
            self.out(msg=e.log_message, httpcode=HTTPCode.HTTP_FAIL_CLIENT_CODE)
        except Exception as e:
            log_exception(e.args[0])
            self.out(msg=e.args[0])
