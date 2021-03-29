from kufta.kafka_tools import StreamCache


class AscendingField(StreamCache):
    # Forces cache size to be 2 otherwise the same __init__ as StreamCache
    def __init__(self, kafka_address: str, timeout: int = 30, **kwargs):
        super.__init__(kafka_address=kafka_address, cache_size=2, timeout=timeout, **kwargs)

    def test_function(self, message_field: str):
        """
            Function for checking if a message has a higher value than the previous ingested message. Assumes list length is 2.
            Args:
                message_field: Which field is being compared defaults to "timestamp".

            Returns:
                bool
            """
        if message_field is None:
            message_field = "timestamp"

        assert len(self.cache) == 2, "Cache size must be 2"
        return self.cache[0][message_field] <= self.cache[1][message_field]
