import pandas as pd
from datetime import datetime
def transform_data(df):
    #convert data column 
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], format="mixed", errors="coerce")
    # add audit columns
    df["load_timestamp"] = datetime.now()
    df["batch_id"] = "batch_001"

    return df
