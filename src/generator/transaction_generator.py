from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from faker import Faker
import random
import uuid

fake = Faker()

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant",
    "gas_station", "online_retail", "pharmacy"
]

COUNTRY_CITIES = {
    "IN": ["Mumbai", "Delhi", "Bangalore", "Chennai"],
    "US": ["New York", "San Francisco", "Chicago"],
    "GB": ["London", "Manchester"],
    "SG": ["Singapore"],
}


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    merchant_id: str
    merchant_category: str
    country: str
    city: str
    timestamp: datetime
    is_fraud: bool = False
    fraud_type: Optional[str] = None


class TransactionGenerator:
    def __init__(self, num_users: int = 100):
        self.users = [f"USR_{i:04d}" for i in range(num_users)]
        self.user_profiles = {
            uid: {
                "home_country": random.choice(list(COUNTRY_CITIES.keys())),
                "avg_amount": round(random.uniform(20, 500), 2),
            }
            for uid in self.users
        }

    def generate_normal(self, user_id: str) -> Transaction:
        profile = self.user_profiles[user_id]
        country = profile["home_country"]
        amount = round(random.gauss(profile["avg_amount"],
                       profile["avg_amount"] * 0.3), 2)
        amount = max(1.0, amount)

        return Transaction(
            transaction_id=str(uuid.uuid4()),
            user_id=user_id,
            amount=amount,
            currency="USD",
            merchant_id=f"MER_{random.randint(1000, 9999)}",
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            country=country,
            city=random.choice(COUNTRY_CITIES[country]),
            timestamp=datetime.now(),
            is_fraud=False
        )