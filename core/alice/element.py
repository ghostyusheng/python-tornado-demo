# -*- coding: utf8 -*-

class Element:
    name = None
    desc = ''
    capcity = 0
    LEN = 10


    def __init__(self, name: str, capcity: int):
        self.name = name
        self.capcity = capcity

