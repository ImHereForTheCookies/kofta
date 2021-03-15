from kufta.kafka_tools import KafkaReader
from confluent_kafka.admin import AdminClient


class Admin(AdminClient):
    def __init__(self, kafka_address: str, configs: dict = None, *args, **kwargs):
        if configs is None:
            configs = {}
        admin_configs = {'bootstrap.servers': kafka_address}
        admin_configs.update(configs)

        AdminClient.__init__(self, admin_configs, *args, **kwargs)

    def delete_topic_pattern(self, pattern: str):
        topic_list = self.list_topics()
        key_words = pattern.split('*')
        assert len(key_words) == 2, "Only one wildcard symbol supported."

        # Empty strings evaluate as false.
        # Splitting a string on a starting symbol i.e. "*test".split("*") yields ['', test]
        if not key_words[0]:
            # Would pass for '*test' and search for all topics ending in 'test'
            to_delete = [topic for topic in topic_list if topic.endswith(key_words[1])]
        elif not key_words[1]:
            # Would pass for 'test*' and search for all topics beginning with 'test'
            to_delete = [topic for topic in topic_list if topic.startswith(key_words[0])]
        else:
            to_delete = [topic for topic in topic_list if
                         topic.startswith(key_words[0]) and topic.endswith(key_words[1])]

        self.delete_topics(to_delete)

    def list_topics(self):
        topic_meta_data = super().list_topics()
        return list(topic_meta_data.topics.keys())


if __name__ == "__main__":
    test = Admin('kafka-rmeyer.cogilitycloud.com:31092')
    test.delete_topic_pattern('avo*')
    print(test.list_topics())