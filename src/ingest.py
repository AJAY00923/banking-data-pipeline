import pandas as pd

def load_data(file_path: str, logger) ->pd.DataFrame:
    logger.info(" Starting of data ingestion")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Data loaded sucessfully. shape:{df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error while loading data : {e}")
        raise

if __name__ == "__main__":
    from logger import get_logger

    logger = get_logger("logs/pipeline.log")

    file_path = "data/raw/transactions.csv"

    df = load_data(file_path, logger)

    print(df.head())