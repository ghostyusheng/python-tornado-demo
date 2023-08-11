# -*- coding: utf-8 -*-
from function.function import config


class Topic():
    index = None

    def get_index(self):
        return config('elasticsearch', 'topic_index')

    def set_current_index(self, index):
        self.index = index

    def get_current_index(self):
        return self.index

    time_interval = 3600

    properties = {
        "properties": {
            "commented_at": {
                "type": "integer"
            },
            "comments": {
                "type": "integer"
            },
            "created_at": {
                "type": "integer"
            },
            "downs": {
                "type": "integer"
            },
            "forum_id": {
                "type": "integer"
            },
            "forum_title": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "forum_type": {
                "type": "keyword"
            },
            "group_label_id": {
                "type": "long"
            },
            "group_label_name": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "group_label_type": {
                "type": "long"
            },
            "has_images": {
                "type": "boolean"
            },
            "has_videos": {
                "type": "integer"
            },
            "hits_weight": {
                "type": "float"
            },
            "hits_weight2": {
                "type": "float"
            },
            "hits_weight3": {
                "type": "float"
            },
            "hits_weight4": {
                "type": "float"
            },
            "id": {
                "type": "keyword"
            },
            "image_count": {
                "type": "integer"
            },
            "is_down": {
                "type": "boolean"
            },
            "is_elite": {
                "type": "boolean"
            },
            "is_hidden": {
                "type": "boolean"
            },
            "is_official": {
                "type": "boolean"
            },
            "pv": {
                "type": "long"
            },
            "summary": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "summary_init": {
                "type": "keyword"
            },
            "summary_max": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "summary_not_analyzed": {
                "type": "keyword"
            },
            "summary_smart": {
                "type": "text",
                "analyzer": "hanlp_smart",
                "search_analyzer": "search_analyzer"
            },
            "summary_std": {
                "type": "text",
                "analyzer": "text_analyzer"
            },
            "title": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "title_init": {
                "type": "keyword"
            },
            "title_max": {
                "type": "text",
                "norms": False,
                "analyzer": "hanlp_max_word",
                "search_analyzer": "search_analyzer"
            },
            "title_not_analyzed": {
                "type": "keyword"
            },
            "title_smart": {
                "type": "text",
                "norms": False,
                "analyzer": "hanlp_smart",
                "search_analyzer": "search_analyzer"
            },
            "title_std": {
                "type": "text",
                "norms": False,
                "analyzer": "text_analyzer"
            },
            "updated_at": {
                "type": "integer"
            },
            "ups": {
                "type": "integer"
            },
            "user_id": {
                "type": "integer"
            }
        }
    }

    settings = {
        "analysis": {
            "filter": {
                "chinese_stop": {
                    "type": "stop",
                    "stopwords": ["之", "的"]
                },
                "english_stop": {
                    "type": "stop",
                    "stopwords": ["a", "an", "and", "are", "as", "at", "by",
                                  "for", "if", "in", "into", "is", "it", "of", "on", "or", "the", "to"]
                },
                "words_delimiter": {
                    "type": "word_delimiter"
                }
            },
            "analyzer": {
                "text_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "words_delimiter",
                        "lowercase",
                        "chinese_stop"
                    ]
                },
                "search_analyzer": {
                    "tokenizer": "hanlp_smart",
                    "filter": [
                        "words_delimiter",
                        "lowercase",
                        "chinese_stop"
                    ]
                }
            }
        },
        "number_of_shards": 4,
        "number_of_replicas": 1
    }
