import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()   # create a Faker instance to generate fake data
random.seed(42)  # seed ensures same random data every run (reproducible)
NUM_RECORDS = 50000 # total number of transactions to generate

def generate_transactions(n=NUM_RECORDS):
    # empty list to store all transaction records
    records = []
    
    # pipeline start date - all transactions from Jan 1 2025
    start_date = datetime(2025, 1, 1)

    for i in range(1, n+1):
        # 3% of transcations are fradulent
        is_fraud = random.random()<0.03

        #fraud transactions have higher amounts
        amount = (
            round(random.uniform(1000,50000), 2)
            if is_fraud
            else round(random.uniform(1, 2000), 2)
        )
        # fraud transcations have more login attempts
        login_attempts = (
            random.randint(3, 10)
            if is_fraud
            else random.randint(1, 2)
        )
       # random date between jan 1 2025 and dec 31 2025
        txn_date = start_date + timedelta(
            days=random.randint(0, 364),
            hours = random.randint(0, 23),
            minutes = random.randint(0, 59)
        )

        # build one transaction record as a dictionary
        records.append({
            "TransactionID":       f"TXN_{i:07d}",
            "AccountID":           f"AC{random.randint(1000, 9999):05d}",
            "TransactionAmount":   amount,
            "TransactionDate":     txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "TransactionType":     random.choice(["Debit", "Credit", "Transfer", "Withdrawal"]),
            "Location":            fake.city(),
            "DeviceID":            f"D{random.randint(100, 999):06d}",
            "IP Address":          fake.ipv4(),
            "MerchantID":          f"M{random.randint(10, 99):03d}",
            "Channel":             random.choice(["Online", "Mobile", "ATM", "Branch"]),
            "CustomerAge":         random.randint(18, 80),
            "CustomerOccupation":  random.choice(["Engineer", "Doctor", "Teacher", "Student", "Manager", "Retired"]),
            "TransactionDuration": random.randint(1, 300),
            "LoginAttempts":       login_attempts,
            "AccountBalance":      round(random.uniform(100, 100000), 2),
            "IsFraud":             int(is_fraud),
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    # create the data/raw folder if it doesn't exist
    os.makedirs("data/raw", exist_ok=True)
    
    print(f"Generating {NUM_RECORDS:,} transactions...")
    
    # generate the data
    df = generate_transactions()
    
    # save to CSV
    df.to_csv("data/raw/transactions.csv", index=False)
    
    print(f"Done! Shape: {df.shape}")
    print(f"Fraud rate: {df['IsFraud'].mean():.1%}")
    print(f"Transaction types:\n{df['TransactionType'].value_counts()}")