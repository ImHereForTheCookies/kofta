from kufta.kafka_tools import KafkaStreamer
from kufta.util.generators import number_generator, name_generator
from confluent_kafka import Producer
from kufta.util.decorators import save_offsets

import json
from faker.providers.person.en import Provider


class KafkaTransformer(KafkaStreamer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transformers = {}
        self.mappings = {}

    def add_transformer(self, field_name):
        pass

    def transformer(self, message):
        raise NotImplementedError

    @save_offsets
    def transform_message(self, kafka_address: str, topic_name: str, producer_configs: dict = None):
        configs = {'bootstrap.servers': kafka_address}
        if producer_configs is not None:
            configs.update(producer_configs)

        producer = Producer(configs)
        for message in self:
            producer.produce(topic_name, json.dumps(self.transformer(message)))


# TODO: There should be a way to use a structure that iterates and maps more automatically
class GadocTransformer(KafkaTransformer):
    def __init__(self, kafka_address: str, topic_name: str, **kwargs):
        super().__init__(kafka_address, topic_name, **kwargs)

        self.phone_generator = number_generator(9e10, add_on_val=1e10)
        self.phone_mappings = {}

        self.resident_generator = number_generator(9e6, add_on_val=1e6)
        self.resident_mappings = {}

        self.name_generator = name_generator(Provider.first_names, Provider.last_names)
        self.name_mappings = {}

    def transformer(self, message):
        if message['receiver_number'] in self.phone_mappings:
            message['receiver_number'] = self.phone_mappings[message['receiver_number']]
        else:
            fake_phone = next(self.phone_generator)
            self.phone_mappings[message['receiver_number']] = fake_phone
            message['receiver_number'] = fake_phone

        if message['resident_id'] in self.resident_mappings:
            message['resident_id'] = self.resident_mappings[message['resident_id']]
        else:
            fake_id = next(self.resident_generator)
            self.resident_mappings[message['resident_id']] = fake_id
            message['resident_id'] = fake_id

        if message['resident_full_name'] in self.name_mappings:
            message['resident_full_name'] = self.name_mappings[message['resident_full_name']]
        else:
            fake_name = next(self.name_generator)
            self.name_mappings[message['resident_full_name']] = fake_name
            message['resident_full_name'] = fake_name

        return message


class GadocTransformer(KafkaStreamer):
    def __init__(self, kafka_address: str, topic_name: str, **kwargs):
        super().__init__(kafka_address, topic_name, **kwargs)
        self.transformations = {}