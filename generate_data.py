import pandas as pd
import random

data = []

for i in range(10000):
    data.append({
        "customer_id": i,
        "name": f"Customer_{i}",
        "email": f"user{i}@gmail.com",
        "age": random.randint(18, 70),
        "country": random.choice(["India", "USA", "UK"]),
        "purchase_amount": random.randint(100, 5000)
    })

df = pd.DataFrame(data)
df.to_csv("raw_data.csv", index=False)

print("Data generated successfully!")