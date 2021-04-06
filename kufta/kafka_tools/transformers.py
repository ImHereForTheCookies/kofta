from kufta.kafka_tools import KafkaStreamer
from kufta.util.generators import number_generator, name_generator
from confluent_kafka import Producer
from kufta.util.decorators import save_offsets

import json


class KafkaTransformer(KafkaStreamer):
    def __init__(self,
                 producer_configs: dict = None,
                 *args, **kwargs):
        self.kafka_key = kwargs.pop('kafka_key', False)

        configs = {'bootstrap.servers': kwargs.get('kafka_address')}
        if producer_configs is not None:
            configs.update(producer_configs)

        self.producer = Producer(configs)

        super().__init__(*args, **kwargs)
        self.transformers = {}
        self.mappings = {}

    def add_transformer(self, field_name: str, generator: callable):
        self.transformers[field_name] = generator

    def transformer(self, message):
        raise NotImplementedError

    # @save_offsets
    # TODO: fix dictionary from add_transformer to take transformers and generators
    def transform_messages(self, topic_name: str, num_messages: int = -1):
        for i, message in enumerate(self):
            if i == num_messages:
                break

            if i % 10000 == 0:
                self.producer.flush()
            decoded = json.loads(message.value().decode('utf-8'))
            for field, transform in self.transformers.items():
                decoded[field] = next(transform)
            self.producer.produce(topic=topic_name,
                                  value=json.dumps(decoded),
                                  key=str(message.key))


# TODO: There should be a way to use a structure that iterates and maps more automatically
class GadocTransformer(KafkaTransformer):
    def __init__(self, kafka_address: str, topic_name: str, **kwargs):
        super().__init__(kafka_address, topic_name, **kwargs)

        self.add_transformer('resident_id', number_generator(9e10, shift_amount=1e10))
        self.phone_mappings = {}

        self.resident_generator = number_generator(9e6, shift_amount=1e6)
        self.resident_mappings = {}

        self.name_generator = name_generator()
        self.name_mappings = {}

    def transformer(self, message):
        # if message['receiver_number'] in self.phone_mappings:
        #     message['receiver_number'] = self.phone_mappings[message['receiver_number']]
        # else:
        #     fake_phone = next(self.phone_generator)
        #     self.phone_mappings[message['receiver_number']] = fake_phone
        #     message['receiver_number'] = fake_phone

        if message['resident_id'] in self.resident_mappings:
            message['resident_id'] = self.resident_mappings[message['resident_id']]
        else:
            fake_id = next(self.resident_generator)
            self.resident_mappings[message['resident_id']] = fake_id
            message['resident_id'] = fake_id

        if message['name'] in self.name_mappings:
            message['name'] = self.name_mappings[message['name']]
        else:
            fake_name = next(self.name_generator)
            self.name_mappings[message['name']] = fake_name
            message['name'] = fake_name

        return message


# class GadocTransformer(KafkaStreamer):
#     def __init__(self, kafka_address: str, topic_name: str, **kwargs):
#         super().__init__(kafka_address, topic_name, **kwargs)
#         self.transformations = {}

if __name__ == '__main__':
    test = GadocTransformer('kafka-cogynt-gadoc-v2.threatdeterrence.com:31092', 'recent_suicide_risk', kafka_key=True)
    test.transform_messages('kafka-develop.cogilitycloud.com:31090', 'recent_suicide_risk', num_messages=100000)
