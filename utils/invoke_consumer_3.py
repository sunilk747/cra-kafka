from kafkatrial.consumer.consumer import Consumer
import time

topics = ['ingestion', 'data-importer']

# Creation of Consumer
c = Consumer(bootstrap_servers='localhost:9092', group_id='openshift-service-name', topics=topics)

# One Consumer able to consume messages from multiple topics
# Load balancing of broker with multiple partitions to enable parallelism
# Load balancing of consumers if one consumer goes down, other consumer picks up the load

while True:
    # example to poll for a message every 5 seconds
    item = c.poll()
    for msg in item:
        print(msg)
        # time.sleep(1)

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
