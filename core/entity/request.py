from uuid import uuid1


class RequestEntity:
    GET = {}
    POST = {}
    _method = "GET"

    def __repr__(self):
        return str(self.GET) + str(self.POST)

    def __init__(self, get_arguments={}, post_arguments={}):
        class _Obj:
            def __getattribute__(self, k):
                print("---> key:", k)
                return object.__getattribute__(self, k)

            def __str__(self):
                return '&'.join(['%s:%s' % item for item in self.__dict__.items()])

        _get = _Obj()
        _post = _Obj()
        for k, v in get_arguments.items():
            key = k
            setattr(_get, key, str(v[0], encoding="utf-8"))
        for k, v in post_arguments.items():
            key = k
            setattr(_post, key, str(v[0], encoding="utf-8"))
        self.GET = _get
        self.POST = _post


    def __getattribute__(self, k):
        print("---> key:", k)
        return object.__getattribute__(self, k)


    def method(self, method):
        self._method = method
        return self


    def getUserId(self):
        return

