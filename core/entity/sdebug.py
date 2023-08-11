# -*- coding: utf-8 -*-


class SdebugEntity:
    dsl = {}

    def __init__(self):
        class _Obj:
            pass

        _dsl = _Obj()
        self.dsl = _dsl
