from kufta.kafka_tools import KafkaReader
from confluent_kafka.admin import AdminClient, NewTopic
from pathlib import Path
import json
import time


class KafkaManager(AdminClient):
    def __init__(self, kafka_address: str, configs: dict = None, *args, **kwargs):
        if configs is None:
            configs = {}
        self.kafka_configs = {'bootstrap.servers': kafka_address}
        self.kafka_configs.update(configs)
        AdminClient.__init__(self, self.kafka_configs, *args, **kwargs)
        # self.test = AdminClient(kafka_configs, *args, **kwargs)

        self.topic_list = None
        self.kafka_key = None
        self.messages = {}

    def topic_search(self, topics: str or list) -> list:
        self.list_topics(output=False)
        if isinstance(topics, str):
            if '*' in topics:
                all_topics = self._topic_search(topics)
            else:
                all_topics = list(topics)

        elif isinstance(topics, list):
            all_topics = []
            for topic in topics:
                if '*' in topics:
                    new_topics = self._topic_search(topics)
                    all_topics += new_topics
                else:
                    all_topics.append(topic)

        else:
            raise TypeError(f"Type{type(topics)} not supported. Use list of strings or str.")

        return all_topics

    def _topic_search(self, pattern: str) -> list:
        key_words = pattern.split('*')
        # Empty strings evaluate as false.
        # Splitting a string on a starting symbol i.e. "*test".split("*") yields ['', test]
        if not key_words[0]:
            # Would pass for '*test' and search for all topics ending in 'test'
            topics = [topic for topic in self.topic_list
                      if topic.endswith(key_words[1])]
        elif not key_words[1]:
            # Would pass for 'test*' and search for all topics beginning with 'test'
            topics = [topic for topic in self.topic_list
                      if topic.startswith(key_words[0])]
        else:
            topics = [topic for topic in self.topic_list
                      if topic.startswith(key_words[0]) and topic.endswith(key_words[1])]

        return topics

    def read_topics(self, topics: str or list, kafka_key=False):
        topic_list = self.topic_search(topics)
        for topic in topic_list:
            reader = KafkaReader(kafka_address=self.kafka_address, topic_name=topic)
            reader.read_topic(num_messages=-1, kafka_key=kafka_key)
            self.messages[topic] = reader.messages
            reader.close()

    def list_topics(self, output: bool = True, *args, **kwargs) -> list:
        topic_meta_data = super().list_topics(*args, **kwargs)
        self.topic_list = [key for key in topic_meta_data.topics.keys()]
        if output:
            return self.topic_list

    def save_messages(self, file: str, topics: str = None, kafka_key=False):
        if topics is None:
            topics = list(self.messages.keys())

        topics = self.topic_search(topics)
        for topic in topics:
            if kafka_key:
                messages = [{'kafka_key': key, 'kafka_message': message} for message, key in self.messages[topic]]
            else:
                messages = [{'kafka_message': message} for message in self.messages[topic]]

            with open(Path(file).joinpath(topic + '.json'), 'w') as out:
                json.dump(messages, out, indent=3)

    def create_topics(self, topics: list or str, partitions: int = 10, replication: int = 3, *args, **kwargs):
        if isinstance(topics, list):
            confluent_topics = [self._create_topic(topic_name, partitions, replication, *args, **kwargs)
                                for topic_name in topics]
        else:
            confluent_topics = [self._create_topic(topics, partitions, replication, *args, **kwargs)]

        attempt_num = 0
        while confluent_topics:
            if attempt_num % 3 == 0:
                super().create_topics(confluent_topics)
            created_topics = self.list_topics()

            for i, new_topic in reversed(list(enumerate(confluent_topics))):
                if new_topic.topic in created_topics:
                    del confluent_topics[i]

            attempt_num += 1
            time.sleep(1)

    @staticmethod
    def _create_topic(topic_name: str, partitions: int = 10, replication: int = 3, *args, **kwargs) -> NewTopic:
        confluent_topic = NewTopic(topic=topic_name,
                                   num_partitions=partitions,
                                   replication_factor=replication,
                                   *args, **kwargs)
        return confluent_topic

    def delete_topics(self, topics: list):
        to_delete = self.topic_search(topics)
        attempt_num = 0
        while to_delete:
            if attempt_num % 3 == 0:
                super().delete_topics(to_delete, operation_timeout=30)
            remaining_topics = self.list_topics()

            for i, topic in reversed(list(enumerate(to_delete))):
                if topic not in remaining_topics:
                    del to_delete[i]

            # Time to wait until attempting next delete.
            attempt_num += 1
            time.sleep(1)


if __name__ == "__main__":
    test = KafkaManager('kafka-rmeyer.cogilitycloud.com:31092')
    test.delete_topic_pattern('avo*')
    print(test.list_topics())
