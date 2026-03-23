from src.generator.transaction_generator import TransactionGenerator, Transaction
from src.generator.kafka_producer import TransactionProducer
from src.utils.config import load_config
import random
import time
from datetime import datetime

def main():
    config = load_config()
    generator = TransactionGenerator(
        num_users=config["generator"]["num_users"]
    )
    producer = TransactionProducer(config=config)
    
    print("Starting transaction stream... Press Ctrl+C to stop")
    # Send one fraud transaction manually for testing

    test_fraud = Transaction(
        transaction_id="TEST-FRAUD-001",
        user_id="USR_0001",
        amount=1500.00,
        currency="USD",
        merchant_id="MER_9999",
        merchant_category="electronics",
        country="US",
        city="New York",
        timestamp=datetime.now(),
        is_fraud=False)

    producer.send(test_fraud)
    print("Sent test fraud transaction!")
    # First establish USR_0001's home country in state
    test_normal = generator.generate_normal("USR_0001")
    producer.send(test_normal)
    print(f"Sent normal transaction for USR_0001 from {test_normal.country}")

    time.sleep(3)  # wait for Spark to process it

    # Now send from different country to trigger geo anomaly

    test_geo = Transaction(
        transaction_id="TEST-GEO-001",
        user_id="USR_0001",
        amount=150.00,
        currency="USD",
        merchant_id="MER_1111",
        merchant_category="online_retail",
        country="GB",
        city="London",
        timestamp=datetime.now(),
        is_fraud=False
    )
    producer.send(test_geo)
    print("Sent test geo anomaly transaction!")
    try:
        while True:
            user_id = random.choice(generator.users)
            transaction = generator.generate_normal(user_id)
            producer.send(transaction)
            time.sleep(config["generator"]["transaction_interval"]) #one transaction at every transaction_interval
    except KeyboardInterrupt:
        print("\nStopping generator...")
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()
