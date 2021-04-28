from kufta.kafka_tools.consumers import KafkaStreamer
from kufta.util.generators import number_generator, name_generator
from confluent_kafka import Producer
from tqdm import tqdm

import types
import json
import random


class KafkaTransformer(KafkaStreamer):
    def __init__(self,
                 kafka_address: str,
                 input_topic: str,
                 producer_configs: dict = None,
                 *args, **kwargs):
        self.kafka_key = kwargs.pop('kafka_key', False)

        configs = {'bootstrap.servers': kafka_address}
        super().__init__(topic_name=input_topic, kafka_address=kafka_address, *args, **kwargs)

        if producer_configs is not None:
            configs.update(producer_configs)

        self.producer = Producer(configs)

        self.transformers = {}
        self.insert_transformers = {}

        self.mappings = {}

    def add_transformer(self, field_name: str, mapping: callable or iter, insert=False):
        if insert:
            self.insert_transformers[field_name] = mapping
        else:
            self.transformers[field_name] = mapping

    # @save_offsets
    def transform_messages(self, topic_name: str, num_messages: int = -1):
        size = len(self)
        num_messages = num_messages if num_messages < size else size
        num_messages = num_messages if num_messages != -1 else len(self)
        for i, message in tqdm(enumerate(self), total=num_messages):
            if i == num_messages:
                break

            if i % 10000 == 0:
                self.producer.flush()
            decoded_message = json.loads(message.value().decode('utf-8'))
            self.transform_message(decoded_message)

            self.producer.produce(topic=topic_name,
                                  value=json.dumps(decoded_message),
                                  key=str(message.key))

    def transform_message(self, message: dict, insert=False):
        if insert:
            for field, transform in self.insert_transformers.items():
                try:
                    if transform == 'delete':
                        del message[field]

                    elif isinstance(transform, types.FunctionType):
                        message[field] = transform(message)

                    elif isinstance(transform, types.GeneratorType):
                        message[field] = next(transform)

                except KeyError:
                    print(f"{field} not found for message key: {self._message.key()}")
        else:
            for field, transform in self.transformers.items():
                try:
                    if transform == 'delete':
                        del message[field]

                    elif isinstance(transform, types.FunctionType):
                        message[field] = transform(message)

                    elif isinstance(transform, types.GeneratorType):
                        message[field] = next(transform)

                except KeyError:
                    print(f"{field} not found for message key: {self._message.key()}")
        # Doesn't need to return since dict is mutable

    def insert_messages(self, topic_name: str, insert_rate: float, num_messages: int = -1):
        size = len(self)
        num_messages = num_messages if num_messages < size else size
        num_messages = num_messages if num_messages != -1 else size
        for i, message in tqdm(enumerate(self), total=num_messages):
            if i == num_messages:
                break

            if i % 10000 == 0:
                self.producer.flush()

            decoded_message = json.loads(message.value().decode('utf-8'))
            self.transform_message(decoded_message)
            self.producer.produce(topic=topic_name,
                                  value=json.dumps(decoded_message),
                                  key=str(message.key()))

            if random.random() < insert_rate:
                self.transform_message(decoded_message, insert=True)
                self.producer.produce(topic=topic_name,
                                      value=json.dumps(decoded_message),
                                      key=str(message.key()))

        self.producer.flush()


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
