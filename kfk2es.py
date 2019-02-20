#!/usr/bin/env python3.6
'''
processing data received from kafka, then send to elasticsearch

you need a config file in json or yaml format
sample:

config.json

{
  "kfk": [
    {
    "topic": "topic",
    "bootstrap_servers": ["10.0.0.1:9092", "10.0.0.2:9092", "10.0.0.3:9092"],
    "sasl_plain_username": "user",
    "sasl_plain_password": "password",
    "group_id": "group"
    }
  ],

  "elasticsearch":{
    "hosts": ["http://user:pass@10.1.0.1:9200", "http://user:pass@10.1.0.2:9200"],
    "index": "index-%Y%m%d",
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

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
from myconf import Conf



argparser = ArgumentParser()
argparser.add_argument('-c', '--config', default='config.json', help='config file in json format')
args = argparser.parse_args()
logger = logging.getLogger(__name__)
hostname = socket.gethostname()


try:
    conf = Conf(args.config)
    KFKS = conf['kfk']
    ES = conf['elasticsearch']
    ES_INDEX = ES.pop('index')
    if 'type' in ES:
        ES_DOC_TYPE = ES.pop('type')
    else:
        ES_DOC_TYPE = 'log'
except:
    logger.error('config file wrong')
    exit(127)


class StreamProcess:
    '''
    kafka inputs --> queue --> es_cache --> elasticsearch

    queue_size(int): size of queue, 0 means infinite
    es_cache_size(int): size for es_cache
    es_timeout(int): when reach timeout(seconds), force send data in cache even cache is not full
    force_exit(int): when ctrl-c is pushed, wait force_exit seconds for left data in memory
    '''
    def __init__(self, queue_size=0, es_cache_size=150, es_timeout=1, force_exit=5):
        self.inputs = KFKS
        try:
            self.es = Elasticsearch(**ES)
        except:
            logger.error('elastic config wrong')
            logger.error(Elasticsearch.__doc__)
            exit(127)
        self.es_cache_size = es_cache_size
        self.es_timeout = es_timeout
        self.handler = None
        self.q = queue.Queue(queue_size)
        self.force_exit = force_exit
        self.stop_event = threading.Event()

    def input(self, **kwargs):
        '''kafka input'''
        topic = kwargs.pop('topic')
        kfk_config = {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 5000,
            "client_id": hostname
        }
        kfk_config.update(kwargs)
        try:
            consumer = KafkaConsumer(topic, **kfk_config)
        except TypeError as e:
            logger.error('not correct kafka param {}'.format(str(e)))
            logger.error(KafkaConsumer.__doc__)
            self.force_exit = 0
            self.stop_event.set()
        while not self.stop_event.is_set():
            logger.debug('ready to data from kafka {}'.format(str(kfk_config['bootstrap_servers'])))
            for msg in consumer:
                yield msg.value
                if not kfk_config.get("enable_auto_commit", False):
                    consumer.commit()
                if self.stop_event.is_set():
                    logger.info('stopping kafka input')
                    break
        consumer.close()

    def _push_to_queue(self, data):
        while True:
            try:
                self.q.put_nowait(data)
                break
            except queue.Full:
                time.sleep(0.1)

    def _process(self, kwargs):
        for event in self.input(**kwargs):
            logger.info('get data from kafka')
            logger.debug('processing event {}'.format(event))
            if not self.handler:
                data = event
            else:
                try:
                    data = self.handler(event)
                except:
                    data = None
                    logger.error('handler failed')
            if data:
                self._push_to_queue(data)

    def _escache(self):
        for _ in range(self.es_cache_size):
            try:
                data = self.q.get(timeout=self.es_timeout)
                if isinstance(data, tuple) and len(data) == 3:
                    index_pat = data[0] if isinstance(data[0], str) else ES_INDEX
                    doc_type = data[1] if isinstance(data[1], str) else ES_DOC_TYPE
                    data = data[2]
                else:
                    index_pat = ES_INDEX
                    doc_type = ES_DOC_TYPE
                t = data['@timestamp'] if isinstance(data.get('@timestamp'), datetime) else datetime.utcnow()
                index = t.strftime(index_pat)
                yield {'_index': index, '_type': doc_type, '_source': data}
            except queue.Empty:
                break

    def output(self):
        while True:
            if self.stop_event.is_set() and self.q.empty():
                logger.info('stopping elasticsearch output')
                break
            try:
                helpers.bulk(self.es, self._escache())
            except Exception as e:
                logger.error('elastic output error')
                logger.debug(str(e))

    def run(self):
        jobs = {}
        job_id = 0
        enable = True
        deadtime = float('inf')
        for inputs in self.inputs:
            t = threading.Thread(target=self._process, args=[inputs])
            t.setDaemon(True)
            jobs[job_id] = (t, (self._process, [inputs]))
            job_id += 1
            t.start()
        outputer = threading.Thread(target=self.output)
        outputer.setDaemon(True)
        outputer.start()
        jobs[job_id] = (outputer, (self.output, tuple()))
        while True:
            try:
                if not enable:
                    break
                for job_id, job in jobs.items():
                    if not job[0].is_alive() and not self.stop_event.is_set():
                        logger.error('job failed, restarting..')
                        func, func_arg= job[1]
                        t = threading.Thread(target=func, args=func_arg)
                        t.setDaemon(True)
                        t.start()
                        jobs[job_id] = (t, (func, func_arg))
                    elif job[0].is_alive() and self.stop_event.is_set():
                        if time.time() <= deadtime:
                            logger.info('job still working,not ready for stop')
                        else:
                            logger.info('job still working, but it`s time to die')
                            enable = False
                        break
                    elif job[0].is_alive() and not self.stop_event.is_set():
                        pass
                    else:
                        enable = False
                time.sleep(0.5)
            except KeyboardInterrupt:
                logger.info('stopping all jobs')
                self.stop_event.set()
                deadtime = time.time() + self.force_exit


def handler_sample(event):
    '''
    handler sample, do not send data to elasticsearch
    '''
    logger.info(event)
    time.sleep(1)


def testcase():
    logger.info('start testing')
    pipe = StreamProcess()
    pipe.handler = handler_sample
    pipe.run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    testcase()
