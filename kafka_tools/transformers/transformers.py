from .. import KafkaStreamer
from confluent_kafka import Producer


class KafkaTransformer(KafkaStreamer, Producer):
    def __init__(self, kafka_address, topic_name, **kwargs):
        super(KafkaStreamer).__init__(self, kafka_address)
    def transformer(self, message):
        raise NotImplementedError

    def transform_message(self):
        pass
