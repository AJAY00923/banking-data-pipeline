import sys
sys.path.insert(0, '/Users/ajaygopavarapu/banking-data-pipeline/src')


from airflow import DAG
from transform import transform_data
from db_load import load_to_postgres
from ingest import load_data
from airflow.operators.python import PythonOperator
from datetime import datetime
from logger import get_logger
from config_reader import load_config

dag = DAG(
    dag_id = "Banking-pipeline",
    start_date = datetime(2025, 12, 1),  # wrap in datetime()
    schedule = "0 6 * * *",
    catchup = False
)
#========================ingest============================
def ingest(**context):
    config = load_config("config/config.yaml")

    # Get log file path from config
    log_file = config["logging"]["log_file"]

    # Get input CSV file path from config
    file_path = config["files"]["input_path"]

    # Create logger
    logger = get_logger(log_file)

    logger.info("Pipeline started")

    df = load_data(file_path, logger)
    
    return df

# =============================TRANSFORM=========== 
def transform(**context):
    df = context['ti'].xcom_pull(task_ids = 'ingest')
    logger = get_logger("logs/pipeline.log")

    df = transform_data(df, logger)

    return df

#========================LOAD=========================
def load(**context):
    config = load_config("config/config.yaml")
    df = context['ti'].xcom_pull(task_ids = 'transform')
    logger = get_logger("logs/pipeline.log")

    df = load_to_postgres(df, logger, config)

t_ingest = PythonOperator(
    task_id = "ingest",
    python_callable = ingest,
    dag = dag
)

t_transform = PythonOperator(
    task_id = "transform",
    python_callable = transform,  # which function?
    dag = dag
)

t_load = PythonOperator(
    task_id = "load",
    python_callable = load,  # which function?
    dag = dag
)


t_ingest >> t_transform >> t_load