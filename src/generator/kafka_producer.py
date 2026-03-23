from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging

logger = logging.getLogger(__name__)

class TransactionProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092",
                 topic: str = "transactions",
                 max_retries: int = 3):
        self.topic = topic
        self.max_retries = max_retries
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=max_retries,
            acks='all'  # wait for all replicas to acknowledge
        )

    def send(self, transaction) -> bool:
        try:
            future = self.producer.send(
                self.topic,
                key=transaction.user_id,
                value=transaction.model_dump()
            )
            future.get(timeout=10)  # wait for confirmation
            print(f"Sent: {transaction.transaction_id} | "
                  f"user={transaction.user_id} | "
                  f"amount={transaction.amount}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send transaction {transaction.transaction_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending transaction: {e}")
            return False

    def flush(self):
        self.producer.flush()
