import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def load_to_postgres(df: pd.DataFrame, logger) -> None:
    logger.info("Starting load to PostgreSQL")

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            dbname="banking_pipeline",
            user="ajaygopavarapu",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        records = [
            (
                row["TransactionID"],
                row["AccountID"],
                row["TransactionAmount"],
                row["TransactionDate"],
                row["TransactionType"],
                row["Location"],
                row["DeviceID"],
                row["IP Address"],
                row["MerchantID"],
                row["Channel"],
                row["CustomerAge"],
                row["CustomerOccupation"],
                row["TransactionDuration"],
                row["LoginAttempts"],
                row["AccountBalance"],
            )
            for _, row in df.iterrows()
        ]

        insert_query = """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                transaction_amount,
                transaction_date,
                transaction_type,
                location,
                device_id,
                ip_address,
                merchant_id,
                channel,
                customer_age,
                customer_occupation,
                transaction_duration,
                login_attempts,
                account_balance
            )
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
        """

        execute_values(cur, insert_query, records)
        conn.commit()

        logger.info(f"Load completed. Attempted to insert {len(records)} records.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error while loading to PostgreSQL: {e}")
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()