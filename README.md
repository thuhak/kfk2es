# kafka to elasticsearch

从kafka读取数据，并处理后写入elastcsearch,只支持python3

## 使用方法

### 编写自己的处理程序


example.py

```python
from kfk2es import StreamProcess

def your_own_handler(event):
    """
    event的内容就是从kafka中获取的每条数据
    """
    print(event)
    #return ('esindex-%Y%m%d', '_log', event)
    #return (None, '_log', event)
    return event # 返回的结果会发到es中，如果没有返回，则不发

if __name__ == '__main__':
    pipe = StreamProcess(configfile='config.yml')
    # from myconf import Conf
    # conf = Conf('config.yml')
    # pipe = StreamProcess(configfile=conf)
    pipe.handler = your_own_handler
    pipe.run()

```

StreamProcess的configfile参数可以是一个配置文件路径，也可以是包含必要参数的字典。

如果不添加任何处理程序，那么从kafka读取的数据会直接尝试往elasticsearch发送

自己添加处理程序，只需要编写函数，并按照如下格式进行返回:

- 返回一个字典， 那么字典中的数据会被传到es中
- 返回一个3个元素的tuple， 那么第一个元素是es的索引，第二个元素则是es的类型，第三个元素为需要导入es的数据。 
通过数据直接返回的索引和类型要优先于配置文件中的配置。如果只希望动态设置其中一个，另一个使用默认，那么不需要定义的设置为None即可
- 如果不需要传输到es中，只要不返回任何数据就可以了。这样可以只测试kafka的连通性

### 编辑配置文件

配置文件可以是json或者yaml格式，以json,yaml或者yml结尾

可以使用多个kafka作为输入，elasticsearch只有一个输出

#### yaml格式例子

config.yml

```yaml
kafka:
  - topic: top
    bootstrap_servers:
      - server1:9092
      - server2:9092
      - server3:9092
    group_id: group_id
    sasl_plain_username: user
    sasl_plain_password: password

elasticsearch:
  params:               #parms for elasticsearch
    hosts:
      - http://user:pass@10.1.0.1:9200
  index: test-%Y%m%d
  type: log             # optional(default: "log")
  cache_size: 150       # optional(default: 150)
  timeout: 1            # optional(default: 1)

```

需要注意的是elasticsearch选项中的parms选项包含了elasticsearch库中Elasticsearch的参数. 而其他参数是StreamProcess需要的



