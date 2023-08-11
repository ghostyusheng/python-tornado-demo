# -*- coding: utf-8 -*-


class Video():

    properties = {
        "properties": {
            "app_id": {
                "type": "integer"
            },
            "app_title": {
                "type": "keyword"
            },
            "app_title_std": {
                "type": "text",
                "analyzer": "text_analyzer"
            },
            "title": {
                "type": "keyword"
            },
            "title_smart": {
                "type": "text",
                "analyzer": "hanlp_smart"
            },
            "title_max": {
                "type": "text",
                "analyzer": "hanlp_max_word"
            },
            "title_std": {
                "type": "text",
                "analyzer": "text_analyzer"
            },
            "intro": {
                "type": "keyword"
            },
            "intro_smart": {
                "type": "text",
                "analyzer": "hanlp_smart"
            },
            "intro_max": {
                "type": "text",
                "analyzer": "hanlp_max_word"
            },
            "intro_std": {
                "type": "text",
                "analyzer": "text_analyzer"
            },
            "recommended_title": {
                "type": "keyword"
            },
            "recommended_title_std": {
                "type": "text",
                "analyzer": "text_analyzer"
            },
            "id": {
                "type": "integer"
            },
            "created_at": {
                "type": "integer"
            },
            "updated_at": {
                "type": "integer"
            },
            "forum_id": {
                "type": "integer"
            },
            "forum_title": {
                "type": "text",
                "analyzer": "hanlp_max_word",
                "search_analyzer": "hanlp_smart"
            },
            "forum_type": {
                "type": "keyword"
            },
            "comments": {
                "type": "integer"
            },
            "ups": {
                "type": "integer"
            },
            "downs": {
                "type": "integer"
            },
            "views": {
                "type": "long"
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
                        "lowercase",
                        "chinese_stop"
                    ]
                }
            }
        },
        "number_of_shards": 4,
        "number_of_replicas": 1
    }
