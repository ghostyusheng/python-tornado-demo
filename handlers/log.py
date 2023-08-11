# -*- coding: utf-8 -*-
from handlers.base import BaseHandler
from core.const import const
from share.repository.kafka import KafkaRepository
from utils.keygen import Keygen

class LogHandler(BaseHandler):

    async def post(self):
        msg = self.get_json_argument("m","")
        channel = self.get_json_argument("c","")
        tk = self.get_json_argument("_tk","")
        uid = msg.get('uid', '')
        cid = msg.get('cid', '')

        if not (msg.get('s') and msg.get('a') and msg.get('t') and (cid or uid) and msg.get('position') and msg.get('ts') and tk):
            self.out(code=500, msg="valid params")
            return
        
        dkey = Keygen.decrypt(tk)
        _uid, _cid, _ts = dkey.split('#')[1:]

        print(f"==> {uid}<->{_uid} {cid}<->{_cid}")
        if (uid != _uid or cid != _cid):
            self.out(code=500, msg="FAIL1")
            return

        msg['uid'] = msg.get('uid', '')
        msg['cid'] = msg.get('cid', '')
        msg['delay'] = msg.get('delay', -1)
        msg['referer'] = msg.get('referer', '')
        msg['ext'] = msg.get('ext', {})

        msg['ip'] = self.request.headers.get('Host')
        msg['ext']['agent'] = self.request.headers.get('User-Agent')

        KafkaRepository.instance().send(channel, msg)
        
        self.out()
