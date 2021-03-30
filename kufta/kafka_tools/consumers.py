from confluent_kafka import Consumer, TopicPartition
from kufta.util.decorators import save_offsets

import json
from tqdm import tqdm
import pickle


class KafkaStreamer(Consumer):
    def __init__(self, kafka_address: str,
                 topic_name: str,
                 *args,  # stops unnamed args after topic_name
                 timeout: int = 30,
                 configs: dict = None,
                 offset_file: str = None,
                 **kwargs):
        self.kafka_address = kafka_address
        self._topic_name = topic_name
        self.timeout = timeout
        self.message_count = 0
        self.readtime_count = 0
        self.messages = []

        # Don't want to default to dict since mutable default args carry across classes
        if configs is None:
            configs = {}
        consumer_configs = {'bootstrap.servers': self.kafka_address, 'group.id': self.topic_name}
        consumer_configs.update(configs)

        super().__init__(consumer_configs, *args, **kwargs)
        # Meta data for partition info
        if offset_file is None:
            topic_partition_keys = self.list_topics(self.topic_name).topics[self.topic_name].partitions.keys()
            self.topic_partitions = [TopicPartition(self.topic_name, partition=partition, offset=0) for partition in topic_partition_keys]

        # Sets partitions to values form a specified load file
        if offset_file is not None:
            self.topic_partitions = pickle.load(offset_file)

        # Set to beginning by default
        # TODO: Should this work for user specified offset in __init__?
        self.assign(self.topic_partitions)

    def __iter__(self):
        self.message_count = 0
        self.readtime_count = len(self)
        return self

    def __next__(self):
        if self.message_count == self.readtime_count:
            raise EOFError
        self.message = self.poll(timeout=self.timeout)
        self.message = json.loads(self.message.value().decode('utf-8'))
        return self.message
        self.message_count += 1

    def __len__(self):
        total = 0
        for partition in self.topic_partitions:
            total += self.get_watermark_offsets(partition)[1]
        return total

    @property
    def partitions(self):
        return self.position(self.topic_partitions)

    @partitions.setter
    def partitions(self, params: list or str):
        """
        Sets
        Args:
            params:

        Returns:

        """
        if isinstance(params, str):
            with open(params, 'rb') as f:
                params = pickle.load(f)
        self.assign(params)
        self.topic_partitions = params

    # def partition_factory(self, topic_name: str, partitions: list, offsets: list):
    #     pass

    @property
    def topic_name(self):
        return self._topic_name

    # TODO: fix setter so that user can enter a topic name and it will automatically read from the topic with all
    #  partitions at offset 0
    # @topic_name.setter
    # def topic_name(self, topic_name: str):
    #     self.topic_name = topic_name
    #     topic_partition_keys = self.list_topics(self.topic_name).topics[self.topic_name].partitions.keys()
    #     self.topic_partitions = [TopicPartition(topic_name, offset=0, partition=partition) for partition in topic_partition_keys]
    #     self.assign(self.topic_partitions)


class KafkaReader(KafkaStreamer):
    @save_offsets
    def read_topic(self, num_messages: int = -1, **kwargs):
        """
        Reads a specified number of messages in a topic.
        Args:
            num_messages: Number of messages to read.
            **kwargs: To be passed to decorator whether you want kafka offset locations saved/loaded. Use arguments:
                        file: Path to save/load from.
                        load: Whether to attempt to load files from this location. (Bool)

        Returns: A list of messages

        """
        self.messages = []
        if num_messages == -1:
            num_messages = len(self)
        for message in tqdm(self, total=num_messages):
            self.messages.append(message)
            if self.message_count == num_messages:
                break


class StreamCache(KafkaStreamer):
    def __init__(self, kafka_address: str, cache_size: int, timeout: int = 30, **kwargs):
        kafka_params = {'bootstrap.servers': kafka_address,
                        'group.id': 'cache_test'}
        kafka_params.update(kwargs)
        super().__init__(self, kafka_params, timeout=timeout)
        self.cache_size = cache_size
        self.cache = []

    def test_function(self):
        raise NotImplementedError

    @save_offsets
    def analyze_topic(self, topic_name: str, num_messages: int = -1, *args, **kwargs):
        """
        Pass a function that has a logical test using the cached information. If the test breaks it will return False.
        The function will read as many rows as you'd like to test.
        Args:
            topic_name: Which topic to read from.
            num_messages: How many messages to test. Defaults to all messages.
            kwargs: Specify a list of offsets and partitions for the topic.

        Returns:
            a boolean signifying whether or not the testing passed.

        """
        self.cache = []
        self.assign_partition(topic_name, **kwargs)
        # adds a total estimate for the progress bar if true, otherwise shows normal iteration progress
        if num_messages == -1:
            num_messages = len(topic_name)
        for count, message in tqdm(enumerate(self), total=num_messages):
            # Comes first since count starts at 0
            if count == num_messages:
                break
            # Checks cache if full before carrying out operations
            self.cache.append(message)
            if len(self.cache) > self.cache_size:
                del self.cache[0]
            else:
                continue
            # Checks if the function criteria was met for cache state
            if not self.test_function(*args, **kwargs):
                return False

        return True


if __name__ == "__main__":
    # test = KafkaReader(kafka_address='kafka-cogynt-gadoc-V2.threatdeterrence.com:31090', topic_name="suicide_risk")
    # test2 = KafkaReader(kafka_address='kafka-cogynt-gadoc-V2.threatdeterrence.com:31090', topic_name="recent_suicide_risk")
    test3 = KafkaReader(kafka_address='kafka-rmeyer.cogilitycloud.com:31092', topic_name='plane_data')
    # print(len(test))
    print(len(test3))
    suicide_risk = test3.read_topic(num_messages=-1)
    print('f')
    # TODO: Create venv for testing this pacckage and fix auto install and fix terminal install -> import problems
#     NUM_MESSAGES = 100000
#     admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
#     consumer = KafkaReader({'bootstrap.servers': 'localhost:9092',
#                             'group.id': 'consumer_test'},
#                            timeout=1)
#     producer = Producer({'bootstrap.servers': 'localhost:9092'})
#
#     admin.delete_topics(['unit_test'])
#     time.sleep(1)
#
#     sent_messages = []
#     for i in range(NUM_MESSAGES):
#         sent_message = {'field': i}
#         sent_messages.append(sent_message)
#         producer.produce('unit_test',
#                          value=json.dumps(sent_message))
#
#     read_messages = consumer.read_topic('unit_test')
#
#     assert sent_messages == read_messages, 'Unit test failed for reading messages'
#     assert consumer.total_messages('unit_test') == NUM_MESSAGES, "Unit test failed for counting messages"
