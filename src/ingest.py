import pandas as pd


def load_data(file_path: str, logger) -> pd.DataFrame:
    logger.info("Starting data ingestion")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Data loaded successfully. Shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error while loading data: {e}")
        raise