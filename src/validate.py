import pandas as pd

def validate_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    logger.info("Starting data validation")

    try:
        # check null values
        null_counts = df.isnull().sum()
        logger.info(f"Null counts:\n{null_counts}")

        # check duplicate rows
        duplicate_count = df.duplicated().sum()
        logger.info(f"Duplicate rows: {duplicate_count}")

        # check invalid transaction dates
        invalid_dates = df["TransactionDate"].isnull().sum()
        logger.info(f"Invalid TransactionDate values: {invalid_dates}")

        # check negative transaction amounts
        negative_amounts = (df["TransactionAmount"] < 0).sum()
        logger.info(f"Negative TransactionAmount values: {negative_amounts}")

        logger.info("Data validation completed successfully")
        return df

    except Exception as e:
        logger.error(f"Error during validation: {e}")
        raise