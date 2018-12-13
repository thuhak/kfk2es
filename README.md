# kafka to elasticsearch

从kafka读取数据，并处理后写入elastcsearch,只支持python3

## 使用方法

### 编写自己的处理程序

example.py

```python
from kfk2es import StreamProcess

def your_own_handler(event):
    '''
    event的内容就是从kafka中获取的每条数据
    '''
    print(event)
    #return ('esindex-%Y%m%d', '_log', event) #如果返回tuple,tupule中第一个为elasticsearch的索引名称的timeformater，第二个数据
    #return (None, '_log', event)
    return event # 返回的结果会发到es中，如果没有返回，则不发

if __name__ == '__main__':
    pipe = StreamProcess()
    pipe.handler = your_own_handler
    pipe.run()

```

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
kfk:
  - topic: top
    bootstrap_servers:
      - server1:9092
      - server2:9092
      - server3:9092
    group_id: group_id
    sasl_plain_username: user
    sasl_plain_password: password

elasticsearch:
  hosts:
    - http://user:pass@10.1.0.1:9200
  index: test-%Y%m%d
  type: log

```

#### json格式的例子

config.json

```json
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

```

上述配置的参数是必填的，如果有额外的需要传递的参数，可以参考kafka-python和elasticsearch两个库的文档


如果希望添加自定义配置, 可以在不影响kfk和elasticsearch这两项的情况下附加其他选项，或者添加include: subdir选项来支持子目录
同样，子目录下的配置文件必须也是json或者yaml结尾

使用
```python
from kfk2es import conf
```
来加载这个配置， 这个conf对象可以根据配置文件的变化进行动态更新

### 执行

```bash
python3 example.py -c config.yaml
```

```bash
python3 example.py -c config.json 

```
