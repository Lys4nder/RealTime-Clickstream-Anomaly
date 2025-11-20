import pandas as pd
import numpy as np
import random
import uuid
import os
from datetime import datetime, timedelta

NUM_ROWS = 50000
START_DATE = datetime.now() - timedelta(days=730)
END_DATE = datetime.now()

PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "GIFT_CARD", "BANK_TRANSFER"]
ORDER_STATUSES = ["COMPLETED", "FAILED", "RETURNED", "PENDING"]
COUNTRIES = ["US", "Canada", "UK", "Germany", "France", "Australia", "Italy", "Spain"]

MONTH_WEIGHTS = {
    1: 0.9,
    2: 0.9,
    3: 1.0,
    4: 1.0,
    5: 1.1,
    6: 1.2,
    7: 1.3,
    8: 1.0,
    9: 1.1,
    10: 1.2,
    11: 1.5,
    12: 1.7,
}

HOUR_WEIGHTS = np.array([
    0.2, 0.1, 0.1, 0.1,
    0.1, 0.2, 0.3, 0.5,
    0.8, 1.0, 1.0, 0.9,
    0.9, 0.8, 0.7, 0.7,
    0.8, 1.1, 1.3, 1.2,
    1.0, 0.7, 0.4, 0.3
], dtype=float)
HOUR_WEIGHTS = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

def generate_timestamps(n_rows: int):
    all_days = pd.date_range(start=START_DATE.date(), end=END_DATE.date(), freq="D")

    day_weights = []
    for day in all_days:
        w = MONTH_WEIGHTS.get(day.month, 1.0)
        if day.weekday() >= 5:
            w *= 1.2
        w *= np.random.uniform(0.8, 1.2)
        day_weights.append(w)

    day_weights = np.array(day_weights, dtype=float)
    day_weights = day_weights / day_weights.sum()

    sampled_days = np.random.choice(all_days, size=n_rows, p=day_weights)
    sampled_hours = np.random.choice(np.arange(24), size=n_rows, p=HOUR_WEIGHTS)
    sampled_minutes = np.random.randint(0, 60, size=n_rows)
    sampled_seconds = np.random.randint(0, 60, size=n_rows)

    timestamps = []
    for d, h, m, s in zip(sampled_days, sampled_hours, sampled_minutes, sampled_seconds):
        d = pd.Timestamp(d).to_pydatetime()

        ts = datetime(d.year, d.month, d.day, int(h), int(m), int(s))
        timestamps.append(ts)

    return timestamps


def choose_order_status(payment_method: str) -> str:
    if payment_method == "BANK_TRANSFER":
        weights = [0.6, 0.15, 0.05, 0.2]
    elif payment_method == "GIFT_CARD":
        weights = [0.85, 0.05, 0.05, 0.05]
    else:
        weights = [0.82, 0.08, 0.05, 0.05]
    return random.choices(ORDER_STATUSES, weights=weights, k=1)[0]

np.random.seed(42)
random.seed(42)

transaction_timestamps = generate_timestamps(NUM_ROWS)

items_count = np.random.randint(1, 10, size=NUM_ROWS)

base_price_per_item = np.round(
    np.random.gamma(shape=2.0, scale=20.0, size=NUM_ROWS), 2
)
total_amount_usd = np.round(
    base_price_per_item * items_count + np.random.uniform(0, 10, size=NUM_ROWS),
    2
)

payment_method_list = [random.choice(PAYMENT_METHODS) for _ in range(NUM_ROWS)]
order_status_list = [choose_order_status(pm) for pm in payment_method_list]

data = {
    "order_id": [str(uuid.uuid5(uuid.NAMESPACE_DNS, f"order-{i}")) for i in range(NUM_ROWS)],
    "session_id": [random.randint(100000, 999999) for _ in range(NUM_ROWS)],
    "user_id": [f"user_{random.randint(1, 3000)}" for _ in range(NUM_ROWS)],
    "transaction_timestamp": transaction_timestamps,
    "total_amount_usd": total_amount_usd,
    "items_count": items_count,
    "payment_method": payment_method_list,
    "order_status": order_status_list,
    "shipping_country": [random.choice(COUNTRIES) for _ in range(NUM_ROWS)],
}

df = pd.DataFrame(data)

output_path = os.environ.get("OUTPUT_PATH", "Orders_Transaction_History.parquet")
df.to_parquet(output_path, index=False)
print(f"Parquet saved to: {output_path}")
