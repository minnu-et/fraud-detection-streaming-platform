from kafka import KafkaProducer
from kafka.errors import KafkaError
from src.utils.config import load_config
import json
import logging

logger = logging.getLogger(__name__)

class TransactionProducer:
    def __init__(self, config: dict = None):
        if config is None:
            config = load_config()
        
        kafka_config = config["kafka"]
        self.topic = kafka_config["topic"]
        self.max_retries = kafka_config["max_retries"]
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=self.max_retries,
            acks='all'
        )

    def send(self, transaction) -> bool:
        try:
            future = self.producer.send(
                self.topic,
                key=transaction.user_id,
                value=transaction.model_dump()
            )
            future.get(timeout=10)
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
