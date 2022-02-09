import os
import pandas as pd

from time import time
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    print("connection established")

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    for i, df in enumerate(df_iter):
        t_start = time()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        if i == 0:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f"Inserted chunk {i}, took {t_end - t_start} second")
