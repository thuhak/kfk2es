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
    "index": "index",
    "type": "log"
  }
}

'''
# author: thuhak.zhou@nio.com
from .kfk2es import StreamProcess, argparser, conf


__all__ = ['StreamProcess', 'argparser', 'conf']