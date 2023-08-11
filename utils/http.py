import traceback
from typing import Any
from core.decorator.downgrade import Downgrade
import aiohttp
from aiohttp import ClientTimeout
from aiohttp.typedefs import StrOrURL
from tornado.httpclient import AsyncHTTPClient


class Http:
    __aiohttp_session = None
    _instance = None

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def getClient(cls):
        if not cls.__aiohttp_session:
            tcp_connector = aiohttp.TCPConnector(
                keepalive_timeout=300, limit=600)
            cls.__aiohttp_session = aiohttp.ClientSession(connector=tcp_connector,
                                                          headers={
                                                              'Connection': 'keep-alive'
                                                          }, timeout=ClientTimeout(total=0.5)
                                                          )
        return cls.__aiohttp_session

    @Downgrade.wrapper
    async def request(self,
                      can_downgrade=False,
                      allow_redirects=False,
                      **kwargs: Any):
        http_client = self.getClient()
        try:
            async with http_client.request(allow_redirects=allow_redirects, **kwargs) as resp:
                response = await resp.text()
        except Exception as e:
            traceback.print_exc()
            raise
        else:
            return response

    @classmethod
    def async_fetch(cls, req):
        http_client = AsyncHTTPClient()
        try:
            http_client.fetch(req)
        except Exception as e:
            print("Error: %s" % e)
