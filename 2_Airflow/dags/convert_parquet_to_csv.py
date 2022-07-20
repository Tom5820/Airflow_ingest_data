import pandas as pd
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def convert_to_csv(time_file):
    PARQUET_FILE_TEMPLATE = f"{AIRFLOW_HOME}/output_{time_file}.parquet"
    print(PARQUET_FILE_TEMPLATE)
    CSV_FILE_TEMPLATE = f"{AIRFLOW_HOME}/output_{time_file}.csv"
    
    df = pd.read_parquet(PARQUET_FILE_TEMPLATE)
    csv_output = CSV_FILE_TEMPLATE
    df.to_csv(csv_output, index=False)
