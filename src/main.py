from ingest import load_data
from transform import transform_data
from validate import validate_data
from db_load import load_to_postgres
from logger import get_logger
from config_reader import load_config


def main():
    """
    Main ETL pipeline:
    1. Read config
    2. Start logger
    3. Ingest data
    4. Transform data
    5. Validate data
    6. Load into PostgreSQL
    """

    # Load settings from YAML config file
    config = load_config("config/config.yaml")

    # Get log file path from config
    log_file = config["logging"]["log_file"]

    # Get input CSV file path from config
    file_path = config["files"]["input_path"]

    # Create logger
    logger = get_logger(log_file)

    logger.info("Pipeline started")

    try:
        # Step 1: Read raw CSV file
        df = load_data(file_path, logger)

        # Step 2: Transform data
        df = transform_data(df, logger)

        # Step 3: Validate data
        df = validate_data(df, logger)

        # Step 4: Load data into PostgreSQL
        load_to_postgres(df, logger, config)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()