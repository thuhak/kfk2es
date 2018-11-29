# kafka to elasticsearch

从kafka读取数据，并处理后写入elastcsearch

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
    return event # 返回的结果会发到es中，如果没有返回，则不发

if __name__ == '__main__':
    pipe = StreamProcess()
    pipe.handler = your_own_handler
    pipe.run()

```

### 编辑配置文件

可以使用多个kafka作为输入，elasticsearch只有一个输出

config.json

```json
{
  "kfk": [
    {
    "servers": ["10.0.0.1:9092", "10.0.0.2:9092", "10.0.0.3:9092"],
    "user": "user",
    "password": "password",
    "topic": "topic",
    "group_id": "group"}
  ],

  "elasticsearch":{
    "servers": ["http://user:pass@10.1.0.1:9200", "http://user:pass@10.1.0.2:9200"],
    "index": "index",
    "type": "doc"
  }
}
```

### 执行


```bash
python example.py -c config.json

```