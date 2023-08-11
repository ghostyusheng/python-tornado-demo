from function.function import R


class RequestMiddleware:
    @classmethod
    def verifyParams(cls, params):
        try:
            for param in params:
                LEN = len(param)
                if LEN == 1:
                    k = param[0]
                    getattr(R().GET, k)
                elif LEN == 2:
                    k, v = param
                    setattr(
                        R().GET,
                        k,
                        getattr(R().GET, k, v)
                    )
                elif LEN == 3:
                    k, v, typ = param
                    _val = getattr(R().GET, k, v)
                    if typ == 'int':
                        _val = int(_val)
                    setattr(
                        R().GET,
                        k,
                        _val
                    )
                else:
                    raise Exception('fatal error')
        except AttributeError as e:
            raise e
