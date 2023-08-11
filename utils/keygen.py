from time import time
import random
import json
import hashlib
import math
import base64

class Keygen(object):
    secret = 'yeehagames'
    algorithm = 'HS256'

    @classmethod
    def generateCert(cls):
        t_list = [i for i in str(int(time()))]
        S = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxy"
        S2 = "abcdefghijkl0123456789mnopqrstuvwxyzabcdefghijklmnopqrstuvwxy"
        LEN = 20
        M = {}
        def getSerial(c, hide_ts=True):
            idx = random.randint(0,5)
            r = random.sample(S, idx)
            if hide_ts:
                r2 = ''.join(r) + c
            else:
                r2 = ''.join(r) + random.randint(0,9)
            r3 = r2 + ''.join(random.sample(S2, LEN - idx - 1))
            return r3

        for c in t_list:
            M[getSerial(c)] = getSerial(c)

        key = cls.setKey(M)

        return M, key

    @classmethod
    def setKey(cls, M):
        SUM = 0
        for s in M.keys():
            for i in s:
                if i.isdigit():
                    SUM += int(i)
                    break

        idx = math.floor(math.sqrt(SUM)) % len(M)
        val1 = M[list(M.keys())[idx]]
        val2 = hashlib.md5(val1.encode()).hexdigest()
        return val2


    @classmethod
    def encrypt(cls, uid, cid, ts):
        S = "abcdefghijkl0123456789mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY" 
        message = '#'.join(['navier', uid, cid, str(ts)])
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        rand = random.sample(S, len(base64_message))
        NS = ""
        for i in range(len(base64_message)):
            c = base64_message[i]
            if i < 5:
                NS += c + rand[i]
            else:
                NS += c
        NS = ''.join(random.sample(S, 3)) + NS + ''.join(random.sample(S, 3))
        return NS


    @classmethod
    def decrypt(cls, NS):
        NS_REAL = NS[3:len(NS)-3]
        NS_REAL = NS_REAL[:10:2] + NS_REAL[10:]
        base64_bytes = NS_REAL.encode('ascii')
        message_bytes = base64.b64decode(base64_bytes)
        message = message_bytes.decode('ascii')
        return message
