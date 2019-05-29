from kafka import KafkaConsumer, TopicPartition, errors
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import six
import datetime


class Consumer(six.Iterator):
    def __init__(self, **configs):
        assert 'bootstrap_servers' in configs, 'Server name not provided. Please provide ' \
                                               '"bootstrap_servers=<host-name>:<port>"'
        # multiple consumers can poll in parallel using a same group_id.
        # ideally group_id should be our openshift-service-name.
        assert 'group_id' in configs, 'group_id not provided. Please provide "group_id=<openshift-service-name>"'
        assert 'topics' in configs, 'No topics provided to subscribe to. Please provide topics=["<topic1>", "<topic2>"]'
        assert type(configs['topics']) in [list, str], "Provide a list of topics or one topic as a string"
        self.topics = configs['topics']
        configs.pop('topics')
        configs['auto_offset_reset'] = 'latest'
        configs['auto_commit_interval_ms'] = 1000
        try:
            self.kc = KafkaConsumer(**configs)
            self.kc.subscribe(topics=self.topics, listener=ConsumerRebalanceListener())
        except errors.NoBrokersAvailable:
            raise ValueError('No brokers available at "server={}"'.format(configs.get('bootstrap_server')))
        except AssertionError as e:
            raise AssertionError('%r' % e)

    def poll(self):
        try:
            self.kc.poll()
            return self.kc

        except (errors.KafkaTimeoutError, AssertionError, ValueError, TypeError) as e:
            raise ValueError('%r' % e)
        # finally:
        #     self.kc.close()

    def seek_last_n_messages(self, last_n_offset):
        # Seek by offset
        assignments = []
        # We will manually assign topic partitions to read the messages from
        try:
            self.kc.unsubscribe()
            self.kc.topics()
            for topic in self.topics:
                partitions = self.kc.partitions_for_topic(topic)
                for p in partitions:
                    assignments.append(TopicPartition(topic, p))
            self.kc.assign(assignments)
            # self.kc.poll(timeout_ms=0)
            messages = []

            for topic in self.topics:
                partitions = self.kc.partitions_for_topic(topic)
                # print(partitions)
                last_offset = max([(self.kc.committed(TopicPartition(topic, p))) for p in partitions])
                offset = max(last_offset - last_n_offset, 0)
                print("Max offset - " + str(last_offset))

                for p in partitions:
                    last_commit_for_partition = self.kc.committed(TopicPartition(topic, p))
                    print("Topic: " + topic + "  Partition: " + str(p))
                    print("LastOffset: {} ** Position: {} ** HighWaterMark: {}".format(
                        str(last_commit_for_partition),
                        str(self.kc.position(TopicPartition(topic, p))),
                        str(self.kc.highwater(TopicPartition(topic, p)))))

                    # print(str(p) + " - " + str(last_commit_for_partition))

                    # if last_commit_for_partition > offset:
                    #     self.kc.seek(TopicPartition(topic, p), offset)
                    #     for msg in self.kc:
                    #         messages.append((msg.offset, msg.value.decode('utf-8'), msg.key))
                            # print(str(p) + " - " + str(msg.offset))
                            # if msg.offset >= (last_commit_for_partition - 1):
                            #     print("Offset 1 using offset position successful.")
                            #     break
            return messages

        except (errors.KafkaTimeoutError, AssertionError, ValueError) as e:
            raise ValueError('%r' % e)
        except TypeError:
            raise ValueError('No last commit available for the given Topics')
        finally:
            self.kc.close()

    def seek_messages_by_timestamp(self, input_dt):
        # Seek messages by timestamp
        assert datetime.datetime.strptime(input_dt, "%Y-%m-%d %H:%M:%S"), \
            'Please provide date input in the format "%Y-%m-%d %H:%M:%S"'

        try:
            assignments = []
            # We will manually assign topic partitions to read the messages from
            self.kc.unsubscribe()
            self.kc.topics()
            for topic in self.topics:
                partitions = self.kc.partitions_for_topic(topic)
                for p in partitions:
                    assignments.append(TopicPartition(topic, p))
            self.kc.assign(assignments)
            # self.kc.poll(timeout_ms=0)
            messages = []
            for topic in self.topics:
                # Get the offset based on timestamp
                offset_time = int((datetime.datetime.strptime(input_dt, "%Y-%m-%d %H:%M:%S")).timestamp() * 1000)
                # print(offset_time)
                partitions = self.kc.partitions_for_topic(topic)
                # print(partitions)

                for p in partitions:
                    dc = {
                        TopicPartition(topic, p): offset_time
                    }
                    last_commit_for_partition = self.kc.committed(TopicPartition(topic, p))
                    offset = [x[0] for x in self.kc.offsets_for_times(dc).values()][0]
                    print("Topic: " + topic + "  Partition: " + str(p) + "  Offset: " + str(offset))
                    # print(str(p) + " - " + str(last_commit_for_partition))

                    if last_commit_for_partition > offset:
                        self.kc.seek(TopicPartition(topic, p), offset)
                        for msg in self.kc:
                            messages.append((msg.offset, msg.value.decode('utf-8'), msg.key))
                            # print(str(p) + " - " + str(msg.offset))
                            if msg.offset >= (last_commit_for_partition - 1):
                                # print("Offset 1 using offset position successful.")
                                break
            return messages

        except (errors.KafkaTimeoutError, AssertionError, ValueError) as e:
            raise ValueError('%r' % e)
        except TypeError:
            raise ValueError('No last commit available for the given Topics')
        finally:
            self.kc.close()

    def close(self, autocommit=False):
        self.kc.close(autocommit=autocommit)
