class SearchBuilder:
    def __init__(self):
        self._builder = []
        self._operate = None

    def dsl(self):
        return self._builder

    def __tostring(self):
        return dsl()

    def boolQuery(self):
        self._builder = {
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                    "must_not": []
                }
            }
        }
        return self

    def limit(self, start, offset):
        self._builder['from'] = start
        self._builder['size'] = offset
        return self

    def should(self, val):
        self._operate = 'should'
        if type(val) == list:
            self._builder['query']['bool']['should'] += val
        if type(val) == dict:
            self._builder['query']['bool']['should'] = val
        return self

    def must(self, val):
        self._operate = 'must'
        if type(val) == list:
            self._builder['query']['bool']['must'] += val
        if type(val) == dict:
            self._builder['query']['bool']['must'] = val
        return self

    def bool(self, val, type):
        if 'must' == type:
            self._builder['query']['bool']['must'].append(val)
        if 'should' == type:
            self._builder['query']['bool']['should'].append(val)

    def must_not(self, val):
        self._operate = 'must_not'
        if type(val) == list:
            self._builder['query']['bool']['must_not'] += val
        if type(val) == dict:
            self._builder['query']['bool']['must_not'] = val
        return self

    def function(self, func):
        if self._builder.get('functions'):
            self._builder['functions'].append(func)
        else:
            self._builder['functions'] = [func]
        return self

    def term(self, key, value, boost=1, filter=True):
        typ = 'term'
        if type(value) == list:
            typ = 'terms'
        if filter:
            self.filter(typ, key, value, boost)
        else:
            self.query(typ, key, value)
        return self

    def match(self, key, value, boost=1, filter=True):
        if filter:
            self.filter('match', key, value, boost)
        else:
            self.query('match', key, value)
        return self

    def match_all(self):
        self._builder.append({
            'match_all': {}
        })
        return self

    def match_phrase(self, key, value, boost=1, filter=True):
        if filter:
            self.filter('match_phrase', key, value, boost)
        else:
            self.query('match_phrase', key, value)
        return self

    def matchPhrasePrefix(self, key, value, boost=1, filter=True):
        if filter:
            self.filter('match_phrase_prefix', key, value, boost)
        else:
            self.query('match_phrase_prefix', key, value)
        return self

    def prefix(self, key, value, boost=1, filter=True):
        if filter:
            self.filter('prefix', key, value, boost)
        else:
            self.query('prefix', key, value)
        return self

    def range(self, key, rangeLst):
        D = {} 
        D[key] = {}
        for operator, val in rangeLst:
            if val == "":
                continue
            D[key][operator] = val

        self._builder.append({
            'range': D
        })
        return self

    def wildcard(self, key, value, boost=1, filter=True):
        if filter:
            self.filter('wildcard', key, value, boost)
        else:
            self.query('wildcard', key, value)
        return self

    def filter(self, typ, key, value, boost):
        self._builder.append({
            'constant_score': {
                'filter': {
                    typ: {
                        key: value
                    }
                },
                'boost': boost
            }
        })

    def query(self, typ, key, value):
        self._builder.append({
            typ: {
                key: value
            }
        })

    def directFilter(self, value):
        self._builder.append({
            'constant_score': {
                'filter': value
            }
        })
        return self

    def rescore(self, builder, window_size=500):
        self._builder['rescore'] = {}
        self._builder['rescore']['query'] = {}
        self._builder['rescore']['query']['rescore_query'] = builder.dsl()[
            'query']
        self._builder['rescore']['window_size'] = window_size
        return self

    def functionScore(self, builder, func):
        self._builder = {
            "query": {
            }
        }
        self._builder['query']['function_score'] = {}
        self._builder['query']['function_score']['query'] = builder.dsl()[
            'query']
        self._builder['query']['function_score']['functions'] = func
        return self

    def sort(self, fields=[]):
        if not len(fields):
            return self
        self._builder['sort'] = []
        for item in fields:
            field = list(item)[0]
            sort = item[field]
            self._builder['sort'].append({
                field: {
                    "order": sort
                }
            })
        return self
