from ingest import load_data
from utils.logger import get_logger

logger = get_logger("logs/pipeline.log")

df = load_data("data/raw/transactions.csv", logger)

print(df.head())