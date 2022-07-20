import argparse
from ast import While
from re import A
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time

def ingest_postgres(dtb_user, dtb_password, dtb_host, dtb_port, dtb_name, dtb_table_name, csv_file):
    engine = create_engine(f'postgresql://{dtb_user}:{dtb_password}@{dtb_host}:{dtb_port}/{dtb_name}')

    engine.connect()

    print('connection established successfully, inserting data...')

    df = pd.read_csv(csv_file)

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df.head(n=0).to_sql(name=dtb_table_name, con=engine, if_exists='replace')


    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=dtb_table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserting.., took %.3f second' % (t_end - t_start))