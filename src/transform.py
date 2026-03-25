import pandas as pd
from datetime import datetime


def transform_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    logger.info("Starting data transformation")

    try:
        # convert transaction date column
        df["TransactionDate"] = pd.to_datetime(
            df["TransactionDate"],
            format="mixed",
            errors="coerce"
        )

        # add audit columns
        df["load_timestamp"] = datetime.now()
        df["batch_id"] = "batch_001"

        logger.info("Data transformation completed successfully")
        return df

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise