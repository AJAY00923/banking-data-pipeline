import pandas as pd
from datetime import datetime


def transform_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    logger.info("Starting data transformation")

    try:
        # work on a copy to avoid chained assignment issues
        df = df.copy()

        # 1. convert transaction date column
        df["TransactionDate"] = pd.to_datetime(
            df["TransactionDate"],
            format="mixed",
            errors="coerce"
        )

        # 2. log invalid dates
        invalid_dates = df["TransactionDate"].isna().sum()
        logger.info(f"Invalid TransactionDate values: {invalid_dates}")

        # optional: remove rows with invalid dates
        df = df[df["TransactionDate"].notna()].copy()

        # 3. future date validation
        today = pd.Timestamp.today().normalize()
        future_rows = df[df["TransactionDate"] > today]
        logger.info(f"Future-dated rows: {len(future_rows)}")

        # remove future-dated rows
        df = df[df["TransactionDate"] <= today].copy()

        # 4. check and remove duplicates
        duplicates = df.duplicated(subset=["TransactionID"]).sum()
        logger.info(f"Duplicate rows: {duplicates}")

        df = df.drop_duplicates(subset=["TransactionID"]).copy()

        # 5. add audit columns
        df["load_timestamp"] = datetime.now()
        df["batch_id"] = "batch_001"

        # 6. final row count after cleaning
        logger.info(f"Rows after transformation: {len(df)}")
        logger.info("Data transformation completed successfully")

        return df

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise