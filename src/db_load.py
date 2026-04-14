import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def get_max_transaction_date(conn):
    """
    Fetch the latest transaction_date already loaded in PostgreSQL.
    Used for incremental loading.
    """
    cur = conn.cursor()
    cur.execute("SELECT MAX(transaction_date) FROM public.transactions;")
    result = cur.fetchone()[0]
    cur.close()
    return result


def load_to_postgres(df: pd.DataFrame, logger, config) -> None:
    """
    Load transformed dataframe into PostgreSQL.

    Steps:
    1. Read DB connection settings from config
    2. Check latest transaction_date already in DB
    3. Filter only new records (incremental load)
    4. Bulk insert rows with ON CONFLICT protection
    """

    logger.info("Starting load to PostgreSQL")

    conn = None
    cur = None

    try:
        db_config = config["database"]

        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            host=db_config["host"],
            port=db_config["port"],
            password=db_config["password"]
        )

        cur = conn.cursor()

        # incremental load logic
        max_date = get_max_transaction_date(conn)

        if max_date:
            logger.info(f"Last loaded transaction_date in DB: {max_date}")
            df = df[df["TransactionDate"] > max_date].copy()
            logger.info(f"New records to load after filtering: {len(df)}")
        else:
            logger.info("No existing data found. Loading full dataset.")

        # nothing new to load
        if df.empty:
            logger.info("No new data to insert. Skipping load step.")
            return

        # prepare records
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
                row["load_timestamp"],
                row["batch_id"],
            )
            for _, row in df.iterrows()
        ]

        insert_query = """
            INSERT INTO public.transactions (
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
                account_balance,
                load_timestamp,
                batch_id
            )
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
        """

        chunk_size = 5000
        total_inserted_attempted = 0

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            execute_values(cur, insert_query, chunk, page_size=1000)
            conn.commit()

            total_inserted_attempted += len(chunk)
            logger.info(
                f"Processed chunk {i // chunk_size + 1}: "
                f"rows {i} to {i + len(chunk) - 1}"
            )

        logger.info(
            f"Load completed successfully. Attempted to insert {total_inserted_attempted} records."
        )

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