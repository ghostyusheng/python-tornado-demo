# -*- coding: utf-8 -*-
from function.function import config


class App():
    index = None

    def get_index(self):
        return config('elasticsearch', 'app_index')

    def get_type(self):
        return config('elasticsearch', 'app_type')

    def set_current_index(self, index):
        self.index = index

    def get_current_index(self):
        return self.index

    time_interval = 3600

    properties = {
        "properties": {
            "abbr": {
                "type": "keyword"
            },
            "alias": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "title_search",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "clear_title": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "title_search",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "area": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "area_available": {
                "type": "keyword"
            },
            "area_ios": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "author": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "title_search"
            },
            "author_id": {
                "type": "long"
            },
            "campfire_is_open": {
                "type": "boolean"
            },
            "created_at": {
                "type": "integer"
            },
            "developer_id": {
                "type": "integer"
            },
            "fans_count": {
                "type": "long"
            },
            "full_pinyin": {
                "type": "keyword"
            },
            "headpy": {
                "type": "keyword"
            },
            "hits": {
                "type": "integer"
            },
            "hits_weight": {
                "type": "float"
            },
            "id": {
                "type": "keyword"
            },
            "identifier": {
                "type": "keyword"
            },
            "is_style_simple": {
                "type": "boolean"
            },
            "is_tap_only": {
                "type": "boolean"
            },
            "page_view": {
                "type": "long"
            },
            "pinyin": {
                "type": "text",
                "analyzer": "standard"
            },
            "play_hits": {
                "type": "integer"
            },
            "publisher_id": {
                "type": "long"
            },
            "pv_search_one_day": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "recently_download": {
                "type": "long"
            },
            "reserve_canceled_count": {
                "type": "long"
            },
            "reserve_count_new": {
                "type": "long"
            },
            "status": {
                "type": "short"
            },
            "tags": {
                "type": "keyword"
            },
            "title": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "title_search"
            },
            "title_all": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "title_search"
            },
            "title_not_analyzed": {
                "type": "keyword"
            },
            "title_standard": {
                "type": "text",
                "analyzer": "title_st"
            },
            "titles": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "title_twogram": {
                "type": "text",
                "analyzer": "twogram_analyzer"
            },
            "updated_at": {
                "type": "integer"
            }
        }
    }

    settings = {
        "index": {
            "search": {
                "slowlog": {
                    "level": "warn",
                    "threshold": {
                        "fetch": {
                            "warn": "1s"
                        },
                        "query": {
                            "warn": "1s"
                        }
                    }
                }
            },
            "number_of_shards": "4",
            "analysis": {
                "filter": {
                    "chinese_stop": {
                        "type": "stop",
                        "stopwords": [
                            "之",
                            "的",
                            " "
                        ]
                    },
                    "words_delimiter": {
                        "type": "word_delimiter"
                    },
                    "english_stop": {
                        "type": "stop",
                        "stopwords": [
                            "a",
                            "an",
                            "and",
                            "are",
                            "as",
                            "at",
                            "by",
                            "for",
                            "if",
                            "in",
                            "into",
                            "is",
                            "it",
                            "of",
                            "on",
                            "or",
                            "the",
                            "to"
                        ]
                    }
                },
                "tokenizer": {
                    "two_gram": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 2,
                        "token_chars": [
                            "letter",
                            "digit"
                        ]
                    }
                },
                "analyzer": {
                    "title_search": {
                        "filter": [
                            "lowercase",
                            "chinese_stop"
                        ],
                        "tokenizer": "hanlp_smart"
                    },
                    "title_st": {
                        "filter": [
                            "lowercase",
                            "chinese_stop",
                            "hanlp_t2s"
                        ],
                        "tokenizer": "standard"
                    },
                    "twogram_analyzer": {
                        "tokenizer": "two_gram"
                    },
                    "title_analyzer": {
                        "filter": [
                            "lowercase",
                            "chinese_stop"
                        ],
                        "tokenizer": "hanlp_max_word"
                    }
                }
            }
        }
    }
