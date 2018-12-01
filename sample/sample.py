import time
import logging
from kfk2es import StreamProcess


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
