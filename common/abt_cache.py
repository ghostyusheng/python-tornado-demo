# -*- coding: utf-8 -*-
import pylru


class AbtCache:

    experCache = pylru.lrucache(20)
