# -*- coding:utf-8 -*-

import json;
#from aiokafka import AIOKafkaProducer;
from kafka import KafkaProducer;
from share.repository.base import BaseRepository
from function.function import config


class KafkaRepository(BaseRepository):

    def __init__(self):
        self._buildConn()


    def _buildConn(self):
        server = config("kafka", "log").split(',')
        self.sender = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=server,
            retries = 3
        )


    def send(self, channel, msg):
        channel = "bigdata_" + channel
        self.sender.send(channel, msg)
        print(f"==> {channel} << {msg}")
