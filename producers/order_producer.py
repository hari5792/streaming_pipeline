import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [
    "iPhone 15",
    "Samsung S24",
    "MacBook Pro",
    "AirPods",
    "Gaming Mouse"
]

while True:

    order = {
        "order_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "price": round(random.uniform(100, 2000), 2),
        "quantity": random.randint(1, 5),
        "event_time": time.time()
    }

    producer.send("orders", order)

    print(f"Sent: {order}")

    time.sleep(2)