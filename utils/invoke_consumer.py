from kafkatrial.consumer.consumer import Consumer

topics = ['ingestion', 'data-importer']

# Creation of Consumer
c = Consumer(bootstrap_servers='localhost:9092', group_id='openshift-service-name', topics=topics)

# Consumers able to read past messages given an offset
# messages = c.seek_last_n_messages(10)
# for i in messages:
#     print(str(i[0]) + " - " + str(i[1]))

# Consumers able to read past messages given a timestamp
messages = c.seek_messages_by_timestamp("2019-04-12 16:30:00")
for i in messages:
    print(str(i[0]) + " - " + str(i[1]))

# finally close the consumer
# c.close(autocommit=True)

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
