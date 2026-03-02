from src.generator.transaction_generator import TransactionGenerator
import random

gen = TransactionGenerator(5)
for i in range(5):
    print(gen.generate_normal(random.choice(gen.users)))
