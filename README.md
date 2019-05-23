# kafka-poc

### Install Kafka Server
Install kafka on your local laptop. Source and Binary downloads are available at
https://kafka.apache.org/downloads

It will install zookeeper as well as kafka.

### Kafka Configuration Changes
You can edit kafka configuration file generally available under
`/usr/local/etc/kafka/server.properties`
1. Number of partitions: 20
2. Number of replicas: 1

### Start Zookeeper and Kafka Service
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties &
kafka-server-start /usr/local/etc/kafka/server.properties

### Kafka Producer
```
from kafkatrial.producer.producer import Producer
p = Producer(bootstrap_servers='localhost:9092', client_id='openshift-service-name')
msg = {'some-key': 'some-value'}
p.send(topic='data-importer', value=msg)
```

### Kafka Consumer
```
from kafkatrial.consumer.consumer import Consumer
topics = ['ingestion', 'data-importer']
c = Consumer(bootstrap_servers='localhost:9092', group_id='openshift-service-name', topics=topics)

Seek Last N messages
messages = c.seek_last_n_messages(10)
for i in messages:
    print(i)

Seek Messages based on a timestamp
messages = c.seek_messages_by_timestamp("2019-04-12 16:30:00")
for i in messages:
    print(i)

Poll cotinuously for messages
while True:
    item = c.poll()
    for msg in item:
        print(msg)
```
