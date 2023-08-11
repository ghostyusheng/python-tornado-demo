# -*- coding:utf-8 -*-
import os
import time
from queue import Queue
from threading import Thread
from aliyun.log import LogClient, LogItem, PutLogsRequest
from share.service.base import BaseService


class LogService(BaseService):
    log_queue = Queue()
    log_batch = 100
    log_interval = 2

    def __init__(self, endpoint, access_key_id, access_key, sls_project, logstore, project):
        self.log_client = LogClient(endpoint, access_key_id, access_key)
        self.host_name = os.getenv('HOSTNAME')
        self.sls_project = sls_project
        self.project = project
        self.logstore = logstore
        Thread(target=self.log_monitor).start()

    def log_monitor(self):
        log_items = []
        cur_time = int(time.time())
        while True:
            if not self.log_queue.empty():
                log_item = LogItem()
                log_item.set_time(int(time.time()))
                log_item.set_contents(self.log_queue.get(block=False))
                log_items.append(log_item)
            if len(log_items) >= self.log_batch:
                request = PutLogsRequest(self.sls_project, self.logstore, self.project, self.project, log_items, compress=False)
                self.log_client.put_logs(request)
                log_items = []
            elif ((int(time.time())-cur_time) >= self.log_interval) and (len(log_items) > 0):
                cur_time = int(time.time())
                request = PutLogsRequest(self.sls_project, self.logstore, self.project, self.project, log_items,
                                             compress=False)
                self.log_client.put_logs(request)
                log_items = []

    def log(self, interface: str, args: str, user_id: str, device_id: str, body: str, level='INFO'):
        self.log_queue.put([
            ('pod', self.host_name),
            ('project', self.project),
            ('level', level),
            ('interface', interface),
            ('args', args),
            ('user_id', user_id),
            ('device_id', device_id),
            ('ts', str(int(time.time()))),
            ('body', body)
        ])








