import pandas as pd
import random
from faker import Faker
from pathlib import Path

fake = Faker()
random.seed(42)

ASSET_CLASSES = ["equity", "fixed_income", "derivative", "fx", "commodity"]
RISK_WEIGHTS = [0.0, 0.20, 0.50, 0.75, 1.00, 1.50]
REPORTING_ENTITIES = ["BANK_NA", "BANK_EU", "BANK_APAC"]

def generate_positions(n: int = 1000) -> pd.DataFrame:
    records = []
    for _ in range(n):
        records.append({
            "trade_id": fake.uuid4(),
            "trade_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "counterparty_id": fake.bothify(text="CPY-####"),
            "asset_class": random.choice(ASSET_CLASSES),
            "notional_usd": round(random.uniform(100_000, 50_000_000), 2),
            "risk_weight": random.choice(RISK_WEIGHTS),
            "reporting_entity": random.choice(REPORTING_ENTITIES),
        })
    return pd.DataFrame(records)

if __name__ == "__main__":
    output_path = Path("data/raw/bronze")
    output_path.mkdir(parents=True, exist_ok=True)
    df = generate_positions(1000)
    df.to_csv(output_path / "positions_raw.csv", index=False)
    print(f"Generated {len(df)} records → {output_path / 'positions_raw.csv'}")