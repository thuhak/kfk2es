import time
import logging
from kafka import KafkaProducer
from kfk2es import StreamProcess
from copy import deepcopy
from myconf import Conf

logging.basicConfig(level=logging.INFO)

config = Conf('config.yml')


kfk_config = deepcopy(config['kafka'][0])
kfk_config['acks'] = 1
kfk_config['compression_type'] = None
topic = kfk_config.pop('topic')
kfk_config.pop('group_id')

producer = KafkaProducer(**kfk_config)


def handler_sample(event):
    '''
    handler sample, do not send data to elasticsearch
    '''
    print(event)
    time.sleep(1)


def testcase():
    print('start testing')
    for _ in range(100):
        producer.send(topic, value=b"abc")
    pipe = StreamProcess(configfile=config)
    pipe.handler = handler_sample
    pipe.run()


if __name__ == '__main__':
    testcase()
