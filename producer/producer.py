from kafka import KafkaProducer
from kafka import errors
import json
# import logging

# logging.basicConfig(level=logging.DEBUG)


class Producer(object):
    def __init__(self, **configs):
        assert 'bootstrap_servers' in configs, 'Server name not provided. Please provide ' \
                                               '"bootstrap_servers=<host-name>:<port>"'
        assert 'client_id' in configs, 'client_id not provided. Please provide "client_id=<openshift-service-name>"'

        try:
            self.kp = KafkaProducer(**configs)
        except errors.NoBrokersAvailable:
            raise ValueError('No brokers available at "server={}"'.format(configs.get('bootstrap_server')))
        except AssertionError as e:
            raise AssertionError('%r' % e)

    def send(self, topic, value, key=None, headers=None, partition=None, timestamp_ms=None):
        assert topic is not None, "No topic name provided to publish messages to."
        assert value is not None, "No message value provided to publish to topic: {}".format(topic)
        assert type(value) in [str, dict, list], "Please provide values in string, json dict or json list."
        if headers:
            assert type(headers) == list
            assert all(type(item) == tuple and len(item) == 2 and type(item[0]) == str and type(item[1]) == bytes
                       for item in headers), "Please provide a list of tuples(<str>key, <bytes>value) for headers."

        try:
            if type(value) == str:
                message = str.encode(value)
                print(message)
            else:
                message = str.encode(json.dumps(value))
                print(message)

            # Send the message to the topic
            # TODO - Need to figure out why time.sleep works producing a topic without using get()
            # send().get() is more like flushing a single message every time it is sent
            # Since get() is a blocking call, its better to use timeout in seconds
            self.kp.send(topic, value=message, partition=partition,
                         key=key, headers=headers, timestamp_ms=timestamp_ms).get(timeout=5)

        except (errors.KafkaTimeoutError, AssertionError, ValueError, TypeError) as e:
            raise ValueError('%r' % e)


