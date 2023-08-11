# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from core.const import const
from utils.keygen import Keygen

class GrantHandler(BaseHandler):

    async def post(self):
        cert = self.get_json_argument("cert","")
        uid = self.get_json_argument("uid","")
        cid = self.get_json_argument("cid","")
        ts = self.get_json_argument("ts","")
        if not ((uid or cid) and cert and ts):
            self.out(code=500, msg="FAIL1")
            return
        key = await const.cache_service.getRequisiteKey(cert)     
        if not key:
            self.out(code=500, msg="FAIL2")
            return
        ekey = Keygen.encrypt(uid,cid,ts)
        self.out(ekey)
