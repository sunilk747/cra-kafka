from kafkatrial.producer.producer import Producer
import time

# Creation of Producer
p = Producer(bootstrap_servers='localhost:9092', client_id='openshift-service-name')
topics = ['ingestion', 'data-importer']
x = -445
while True:
    for topic in topics:
        # Messages can be of type String or JSON
        msg = {'from': topic + " -  on date: 10-May with value = " + str(x)}

        # Producer able to send topics to different partitions automatically
        p.send(topic=topic, value=msg)

        # Producer able to produce messages to a topic as well as partition of its choice
        # p.send(topic=topic, value={'from': msg}, partition=2)
        time.sleep(1)

    x += 1

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
