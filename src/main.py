import logging
from ingest import load_data
from transform import transform_data
from db_load import load_to_postgres


def setup_logger():
    logging.basicConfig(
        filename="logs/pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


def main():
    logger = setup_logger()
    file_path = "data/raw/transactions.csv"

    logger.info("Pipeline started")

    try:
        df = load_data(file_path, logger)
        df = transform_data(df, logger)
        load_to_postgres(df, logger)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()