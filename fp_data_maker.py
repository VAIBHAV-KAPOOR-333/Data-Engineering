from faker import Faker
import pandas as pd
import random

print("🚀 Starting Data Generation...")

fake = Faker()

data = []

for _ in range(1000):
    data.append({
        "customer_id": random.randint(10000, 99999),
        "name": fake.name(),
        "city": fake.city(),
        "product": random.choice(["Laptop", "Phone", "Tablet", "Headphones"]),
        "price": random.randint(1000, 100000),
        "quantity": random.randint(1, 5)
    })

df = pd.DataFrame(data)

df.to_csv("sales_data.csv", index=False)
df.to_json("sales_data.json", orient="records")

print("✅ Data Generated Successfully")
