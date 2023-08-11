import contextvars


class RequestContext(dict):
    class ConstError(TypeError):
        pass

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            return None

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" % name)
        self[name] = value


context_var = contextvars.ContextVar('context')
