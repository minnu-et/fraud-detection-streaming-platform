from src.generator.transaction_generator import TransactionGenerator
from src.generator.kafka_producer import TransactionProducer
import random
import time

def main():
    generator = TransactionGenerator(num_users=100)
    producer = TransactionProducer()
    
    print("Starting transaction stream... Press Ctrl+C to stop")
    
    try:
        while True:
            user_id = random.choice(generator.users)
            transaction = generator.generate_normal(user_id)
            producer.send(transaction)
            time.sleep(0.5)  # one transaction every 0.5 seconds
    except KeyboardInterrupt:
        print("\nStopping generator...")
        producer.flush()
        print("Done.")

if __name__ == "__main__":
    main()
