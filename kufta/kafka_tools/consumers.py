from confluent_kafka import Consumer, TopicPartition
# from kufta.util.decorators import save_offsets

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
        self.timeout = timeout
        # for enabling property setters
        self._topic_name = topic_name
        self._message = None

        if configs is None:
            configs = {}
        consumer_configs = {'bootstrap.servers': self.kafka_address,
                            'group.id': 'test',
                            # 'socket.timeout.ms': 300000,
                            # 'session.timeout.ms': 3600000,
                            # 'heartbeat.interval.ms': 360000,
                            # 'max.poll.interval.ms': 36000000
                            }
        consumer_configs.update(configs)

        Consumer.__init__(self, consumer_configs, *args, **kwargs)
        # Meta data for partition info
        if offset_file is None:
            topic_partition_keys = self.list_topics().topics[topic_name].partitions.keys()
            self.topic_partitions = [TopicPartition(topic_name, partition=partition, offset=0) for partition in topic_partition_keys]

        # Sets partitions to values form a specified load file
        else:
            self.topic_partitions = pickle.load(offset_file)

        # Set to beginning by default
        self.assign(self.topic_partitions)

    def __iter__(self):
        return self

    def __next__(self):
        self._message = self.poll(timeout=self.timeout)
        return self._message

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

    @property
    def topic_name(self):
        return self._topic_name

    @topic_name.setter
    def topic_name(self, topic_name: str):
        self._topic_name = topic_name
        topic_partition_keys = self.list_topics(self.topic_name).topics[self.topic_name].partitions.keys()
        self.topic_partitions = [TopicPartition(topic_name, offset=0, partition=partition) for partition in topic_partition_keys]
        self.assign(self.topic_partitions)


class KafkaReader(KafkaStreamer):
    messages = []

    # @save_offsets
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
        keys = kwargs.pop('kafka_key', False)
        if num_messages == -1:
            num_messages = len(self)
        self.messages = [0] * num_messages
        for i, message in tqdm(enumerate(self, start=1), total=num_messages):
            _decoded_message = json.loads(message.value().decode('utf-8'))
            decoded_message = _decoded_message if not keys else (_decoded_message, str(message.key()))

            self.messages[i-1] = decoded_message
            if i == num_messages:
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

    # @save_offsets
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
    
