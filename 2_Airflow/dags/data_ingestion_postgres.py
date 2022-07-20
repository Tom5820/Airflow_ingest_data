from asyncio import tasks
from locale import locale_alias
import os

from datetime import datetime
from tracemalloc import start

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from convert_parquet_to_csv import convert_to_csv
from ingest_postgres import ingest_postgres

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

PG_HOST = os.getenv('PG_HOST')             
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE') 

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet' #airflow jinja execution_date
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TIME_FILE = '{{ execution_date.strftime(\'%Y-%m\') }}'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
CSV_FILE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

locale_workflow = DAG(
    "LocalIngestionDAD",
    schedule_interval = "0 6 5 * *", # use https://crontab.guru
    start_date = datetime(2022, 4, 4),
    end_date =datetime(2022, 5, 4)
)

with locale_workflow :
    download_task = BashOperator(
        task_id = "download",
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )
    convert_task = PythonOperator(
        task_id = "convert",
        python_callable = convert_to_csv,
        op_kwargs = dict(
            time_file=TIME_FILE,
        )
    )

    ingest_task = PythonOperator(
        task_id = "ingest",
        python_callable = ingest_postgres,
        op_kwargs = dict(
            dtb_user = PG_USER,
            dtb_password = PG_PASSWORD,
            dtb_host = PG_HOST,
            dtb_port = PG_PORT,
            dtb_name = PG_DATABASE,
            dtb_table_name = TABLE_NAME_TEMPLATE,
            csv_file = CSV_FILE
        ),
    )
    download_task >> convert_task >> ingest_task