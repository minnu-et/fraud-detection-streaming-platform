from kafka import KafkaProducer
import json
from datetime import datetime

class TransactionProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092", 
                 topic: str = "transactions"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def send(self, transaction) -> None:
        self.producer.send(
            self.topic,
            key=transaction.user_id,
            value=transaction.model_dump()
        )
        print(f"Sent: {transaction.transaction_id} | "
              f"user={transaction.user_id} | "
              f"amount={transaction.amount}")

    def flush(self):
        self.producer.flush()
