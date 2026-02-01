import json
from kafka import KafkaConsumer


class TransactionConsumer:
    def __init__(
        self,
        topic: str,
        bootstrap_servers: list,
        group_id: str = "anomaly-detector-group",
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def consume(self):
        """
        Generator yielding transaction events
        """
        for message in self.consumer:
            yield message.value

    def close(self):
        self.consumer.close()