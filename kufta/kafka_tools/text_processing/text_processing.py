import pickle
from kufta.kafka_tools import KafkaStreamer
from kufta.util import save_offsets
import re


class KafkaParser(KafkaStreamer):
    def __init__(self, kafka_params: dict, topic_name: str, *args, **kwargs):
        super().__init__(kafka_params, *args, **kwargs)
        self.topic_name = topic_name
        self.messages = []

    def parser(self):
        raise NotImplementedError

    @save_offsets
    def save_messages(self, field_name: str, save_file: str, append=False, **kwargs):
        topic_name = kwargs.get("topic_name", False)
        if topic_name:
            self.assign_partition(topic_name)
        else:
            self.assign_partition(self.topic_name)
        with open(save_file, 'a' if append else 'w') as f:
            for message in self:
                if message[field_name] is None:
                    continue
                parsed_text = self.parser(message[field_name])
                if parsed_text is None:
                    continue
                elif type(parsed_text) is list:
                    for sentence in parsed_text:
                        f.write(sentence + "\n")
                else:
                    f.write(message + "\n")

    @save_offsets
    def store_messages(self, field_name: str, **kwargs):
        topic_name = kwargs.get("topic_name", False)
        if kwargs.get("clear_messages", False):
            self.messages = []
        if topic_name:
            self.assign_partition(topic_name)
        else:
            self.assign_partition(self.topic_name)

        for message in self:
            parsed_text = self.parser(message[field_name])
            if type(parsed_text) is list:
                for sentence in parsed_text:
                    self.messages.append(sentence)
            else:
                self.messages.append(parsed_text)


class VerusParser(KafkaParser):
    def parser(self, text):
        text = ''.join(re.findall(r"\[0[^\[]+", text))
        try:
            message_size = len(text.lower().split('thank you for using tell me')[
                                   1])  # 'Thank you for using security You may start the conversation now')[1]
            text = text[-message_size:]
        except IndexError:
            return None
        caller_only = re.sub(r'[^a-zA-Z\' ]', '', text)
        caller_only = re.sub(r'\s{2,}', ' ', caller_only)
        return caller_only.strip().split('.')


class GadocParser(KafkaParser):
    def parser(self, text):
        text = ''.join(re.findall(r"\[0[^\[]+", text))
        try:
            message_size = len(text.lower().split('thank you for using security you may start the conversation now')[1])
            text = text[-message_size:]
            # TODO: This returns all lower case text. Is this desired?
            text = re.sub(r'the caller has hung up', '', text.lower())
            if len(text) < 10:
                return None
        except IndexError:
            return None
        caller_only = re.sub(r'[^a-zA-Z\' ]', '', text)
        caller_only = re.sub(r'\s{2,}', ' ', caller_only)
        return caller_only.strip().split('.')


def text_to_lists(text_file, pickle_file, regex=r'\S+'):
    out_list = []
    with open(text_file, 'r') as infile:
        for line in infile:
            out_list.append(re.findall(regex, line))

    with open(pickle_file, 'wb') as outfile:
        pickle.dump(out_list, outfile)


if __name__ == "__main__":
    text_to_lists('../../word2vec/gadoc_sentences.txt', '../../word2vec/saved_objects/gadoc_text.pkl')
    print('f')