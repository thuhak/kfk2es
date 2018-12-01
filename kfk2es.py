#!/usr/bin/env python3.6
'''
processing data received from kafka, then send to elasticsearch

you need a config file in json format
sample:

{
  "kfk": [
    {
    "servers": ["10.0.0.1:9092", "10.0.0.2:9092", "10.0.0.3:9092"],
    "user": "user",
    "password": "password",
    "topic": "topic",
    "group_id": "group"
    }
  ],

  "elasticsearch":{
    "servers": ["http://user:pass@10.1.0.1:9200", "http://user:pass@10.1.0.2:9200"],
    "index": "index",
    "type": "log"
  }
}

'''
# author: thuhak.zhou@nio.com
from argparse import ArgumentParser
import socket
import queue
import time
from datetime import datetime
import threading
import logging

from myconf import Conf
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers


__all__ = ['StreamProcess', 'conf', 'argparser']


argparser = ArgumentParser()
argparser.add_argument('-c', '--config', default='config.json', help='config file in json format')
args = argparser.parse_args()
config_file = args.config


try:
    conf = Conf(args.config)
    KFKS = conf['kfk']
    ES_SERVERS = conf['elasticsearch']['servers']
    ES_INDEX = conf['elasticsearch']['index']
    ES_DOC_TYPE = conf['elasticsearch'].get('type', 'log')
except Exception as e:
    print('config file wrong')
    exit(127)


hostname = socket.gethostname()


class StreamProcess:
    def __init__(self, queue_size=0, es_cache_size=150, es_timeout=1):
        self.inputs = []
        for kfk_args in KFKS:
            kfk_args['client_id'] = hostname
            self.inputs.append((self.input, kfk_args))
        self.es = Elasticsearch(ES_SERVERS)
        self.es_cache_size = es_cache_size
        self.es_timeout = es_timeout
        self.handler = None
        self.q = queue.Queue(queue_size)
        self.enable = True

    def input(self, servers, topic, user, password, group_id, client_id):
        ''' kafka input'''
        consumer = KafkaConsumer(topic,
                                 bootstrap_servers=servers,
                                 security_protocol="SASL_PLAINTEXT",
                                 sasl_mechanism="PLAIN",
                                 sasl_plain_username=user,
                                 sasl_plain_password=password,
                                 client_id=client_id,
                                 group_id=group_id,
                                 auto_offset_reset="earliest",
                                 enable_auto_commit=True,
                                 auto_commit_interval_ms=5000
                                 )
        for msg in consumer:
            yield msg.value
            if not self.enable:
                logging.info('stopping kafka input')
                break
        consumer.close()

    def _push_to_queue(self, data):
        while True:
            try:
                self.q.put_nowait(data)
                break
            except queue.Full:
                time.sleep(0.1)

    def _process(self, input, kwargs):
        for event in input(**kwargs):
            logging.info('get data from kafka')
            logging.debug('processing event {}'.format(event))
            if not self.handler:
                data = event
            else:
                try:
                    data = self.handler(event)
                except:
                    data = None
                    logging.error('handler failed')
            if data:
                self._push_to_queue(data)

    def _escache(self):
        for _ in range(self.es_cache_size):
            try:
                data = self.q.get(timeout=self.es_timeout)
                t = datetime.utcnow()
                index = ES_INDEX + '-' + t.strftime('%Y-%m-%d')
                yield {'_index': index, '_type': ES_DOC_TYPE, '_source': data}
            except queue.Empty:
                break

    def output(self):
        while True:
            if not self.enable and self.q.empty():
                logging.info('stopping elasticsearch output')
                break
            helpers.bulk(self.es, self._escache())

    def run(self):
        jobs = {}
        job_id = 0
        enable = True
        for inputs in self.inputs:
            t = threading.Thread(target=self._process, args=inputs)
            t.setDaemon(True)
            jobs[job_id] = (t, inputs)
            job_id += 1
            t.start()
        outputer = threading.Thread(target=self.output)
        outputer.setDaemon(True)
        outputer.start()
        jobs[job_id] = (outputer, tuple())
        while True:
            try:
                if not enable:
                    break
                for job_id, job in jobs.items():
                    if not job[0].is_alive() and self.enable:
                        logging.error('job failed, restarting..')
                        job_arg = job[1]
                        t = threading.Thread(target=self._process, args=job_arg)
                        t.setDaemon(True)
                        t.start()
                        jobs[job_id] = (t, job_arg)
                    elif job[0].is_alive() and not self.enable:
                        break
                    elif job[0].is_alive() and self.enable:
                        pass
                    else:
                        enable = False
                time.sleep(0.5)
            except KeyboardInterrupt:
                logging.info('stopping all jobs')
                self.enable = False


def handler_sample(event):
    '''
    handler sample, do not send data to elasticsearch
    '''
    print(event)
    time.sleep(1)


def testcase():
    logging.basicConfig(level=logging.INFO)
    pipe = StreamProcess()
    pipe.handler = handler_sample
    pipe.run()


if __name__ == '__main__':
    testcase()
