# Banking Data Pipeline

## Project Overview

This project is an end-to-end ETL pipeline built using Python, pandas, and PostgreSQL.  
It reads banking transaction data from a CSV file, applies transformation and validation rules, and loads clean data into PostgreSQL.  

The pipeline supports incremental loading, ensuring only new records are processed. It also includes duplicate handling, future-date filtering, and logging for monitoring pipeline runs.

---

## Tech Stack

- Python  
- pandas  
- PostgreSQL  
- psycopg2  
- YAML  
- Logging  
- Git/GitHub  

---

## Pipeline Flow

CSV → Ingest → Transform → Validate → Incremental Load → PostgreSQL  

The pipeline reads raw transaction data, cleans and validates it, filters only new records, and loads the final dataset into PostgreSQL.

---

## Key Features

- Modular ETL design with separate ingest, transform, and load steps  
- Transaction date conversion and invalid-date handling  
- Future-dated transaction filtering  
- Duplicate removal based on `TransactionID`  
- Incremental loading using the latest `transaction_date` in PostgreSQL  
- Idempotent inserts using PostgreSQL unique constraint and `ON CONFLICT DO NOTHING`  
- Audit columns such as `load_timestamp` and `batch_id`  
- Logging for pipeline monitoring and debugging  

---

## Project Structure
```bash
banking-data-pipeline/
├── config/
│   └── config.yaml
├── data/
│   ├── raw/
│   └── processed/
├── logs/
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── db_load.py
│   ├── logger.py
│   └── main.py
├── requirements.txt
├── README.md
└── .gitignore
---
```
## How to Run

1. Clone the repository  
2. Create and activate a virtual environment  
3. Install dependencies  
4. Update `config/config.yaml` with PostgreSQL connection details  
5. Run the pipeline  

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
```


---

