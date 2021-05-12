from kufta.kafka_tools.consumers import KafkaStreamer
# from kufta.util.generators import number_generator, name_generator
from confluent_kafka import Producer
from tqdm import tqdm

import types
import json
import random


class Generator(object):
    generators = {}

    def add_generator(self, field: str, generator: types.GeneratorType):
        Warning(isinstance(generator, types.GeneratorType), "generator must be a generator object.")

        if field in self.generators:
            print(f"{field} already has a generator and is being overwritten.")

        self.generators[field] = generator

    def generate_message(self):
        data = {}
        for field, generator in self.generators.items():
            data[field] = next(generator)

        return data


class Mutator(Generator):
    transformers = {}
    deletes = set()

    def add_transformer(self, field: str, transformer: types.FunctionType):
        assert isinstance(transformer, types.Function), "transformer must be a function"

        if field in self.tranformers:
            print(f"{field} already has a function and is being overwritten. "
                  f"To combine functions modify your transformer function")

        self.transformers[field] = transformer

    def remove_field(self, field: str):
        self.deletes.add(field)

    def mutate(self, value: dict):
        for field in value.keys():
            if field in self.deletes:
                del value[field]
                continue

            if field in self.transformers.keys():
                value[field] = self.transformers[field](value)

            assert field in self.transformers.keys() and field in self.generators.keys(), \
                f"Cannot have a generator and transformer for the same field ({field})."

            generated_data = self.generate_message()
            value.update(generated_data)

        # Doesn't need return since value is a dict (mutable)


class KafkaGenerator(Generator):
    def __init__(self,
                 kafka_address: str,
                 configs: dict = None,
                 *args, **kwargs):

        producer_configs = {'bootstrap.servers': kafka_address}
        if configs is not None:
            producer_configs.update(configs)

        self.producer = Producer(producer_configs, *args, **kwargs)

        self.key_generator = None

    def generate_messages(self, topic_name: str, num_messages: int):
        for i in range(num_messages):
            if i % 10000 == 0:
                self.producer.flush()

            message = self.generate_message()
            self.producer.produce(topic=topic_name,
                                  value=json.dumps(message),
                                  key=None if self.key_generator is None else next(self.key_generator))

        self.producer.flush()


class FileGenerator(Generator):
    messages = []

    def generate_file(self, file_name: str, num_messages: int):
        for _ in range(num_messages):
            self.messages.append(self.generate_message())

        with open(file_name, 'wb') as out:
            json.dump(self.messages, out)


class KafkaMutator(Mutator):
    def __init__(self,
                 kafka_address: str,
                 input_topic: str,
                 streamer_configs: dict = None,
                 producer_configs: dict = None,
                 *args, **kwargs):
        self.kafka_key = kwargs.pop('kafka_key', False)

        configs = {'bootstrap.servers': kafka_address}
        if streamer_configs is not None:
            configs.update(streamer_configs)
        self.streamer = KafkaStreamer(topic_name=input_topic, kafka_address=kafka_address, *args, **kwargs)

        pro_configs = {'bootstrap.servers': kafka_address}
        if producer_configs is not None:
            pro_configs.update(producer_configs)

        self.producer = Producer(pro_configs)

        self.insert_mutator = Mutator()

    def mutate_messages(self, topic_name: str, num_messages: int = -1, insert_rate=0):
        """
        Alter messages as they stream from an kafka topic before producing to another kafka topic.
        Args:
            topic_name: Specify which topic to produce messages to.
            num_messages: How many messages to use from the read topic (excludes number of inserted messages)
            insert_rate: How often to insert messages on top of the input stream.
                If between 0 and 1, the value is compared to random.random().
                If the value is above one it is produced using <message number> % insert rate == 0.

        Returns:
            None, send messages to topic.
        """
        size = len(self.streamer)
        # Sets number of messages to at most the total number of messages in the input topic.
        # If specified number of messages is -1, it sets to the total size.
        num_messages = num_messages if num_messages < size and num_messages != -1 else size
        for i, message in tqdm(enumerate(self.streamer), total=num_messages):
            if i == num_messages:
                break

            if i % 10000 == 0:
                self.producer.flush()
            decoded_message = json.loads(message.value().decode('utf-8'))

            if 0 < insert_rate < 1 and random.random() < insert_rate or \
                    insert_rate > 1 and i % insert_rate == insert_rate - 1:
                message_copy = decoded_message
                self.insert_mutator.mutate(message_copy)
                self.producer.produce(topic=topic_name,
                                      value=json.dumps(message_copy),
                                      key=str(message.key))

            self.mutate(decoded_message)
            self.producer.produce(topic=topic_name,
                                  value=json.dumps(decoded_message),
                                  key=str(message.key))


# class GadocTransformer(KafkaTransformer):
#     def __init__(self, kafka_address: str, topic_name: str, **kwargs):
#         super().__init__(kafka_address, topic_name, **kwargs)
#
#         self.add_transformer('resident_id', number_generator(9e10, shift_amount=1e10))
#         self.phone_mappings = {}
#
#         self.resident_generator = number_generator(9e6, shift_amount=1e6)
#         self.resident_mappings = {}
#
#         self.name_generator = name_generator()
#         self.name_mappings = {}
#
#     def transformer(self, message):
#         if message['resident_id'] in self.resident_mappings:
#             message['resident_id'] = self.resident_mappings[message['resident_id']]
#         else:
#             fake_id = next(self.resident_generator)
#             self.resident_mappings[message['resident_id']] = fake_id
#             message['resident_id'] = fake_id
#
#         if message['name'] in self.name_mappings:
#             message['name'] = self.name_mappings[message['name']]
#         else:
#             fake_name = next(self.name_generator)
#             self.name_mappings[message['name']] = fake_name
#             message['name'] = fake_name
#
#         return message
#

# class GadocTransformer(KafkaStreamer):
#     def __init__(self, kafka_address: str, topic_name: str, **kwargs):
#         super().__init__(kafka_address, topic_name, **kwargs)
#         self.transformations = {}

if __name__ == '__main__':
    test = GadocTransformer('kafka-cogynt-gadoc-v2.threatdeterrence.com:31092', 'recent_suicide_risk', kafka_key=True)
    test.transform_messages('kafka-develop.cogilitycloud.com:31090', 'recent_suicide_risk', num_messages=100000)
