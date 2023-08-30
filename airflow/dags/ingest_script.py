import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, input_file, execution_date):
    print("\n",table_name, input_file, execution_date,"\n")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('\nconnection established successfully, inserting data...\n')

    # converting the `.parquet` file into `.csv.gz` file:
    
    df = pd.read_parquet(input_file)
    csv_file = input_file.replace('.parquet', '.csv.gz')
    df.to_csv(csv_file, index=False, compression="gzip")
    # ↪ file saved with `.csv.gz` extension, and pandas can read it now:

    t_start = time() 
    i = 1

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # inserting column names (header) and 1st chunk:
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted chunk %d, took %.3f second' % (i, t_end - t_start))
    i += 1

    # inserting the rest of the chunks:
    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("\ninsertion complete!\n")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted chunk %d, took %.3f second' % (i, t_end - t_start))
        i += 1

    os.remove(csv_file)
