import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # $ URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        file_name = 'output.csv.gz'
    elif url.endswith('.parquet'):
        file_name = 'output.parquet'
    else:
        file_name = 'output.csv'

    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if url.endswith('.parquet'):
        df = pd.read_parquet(file_name)
        file_name = 'output.csv.gz'
        # saves file in the dir where the script is running :
        df.to_csv(file_name, index=False, compression="gzip")

    # can read compressed csv files as well:
    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print("creating table ...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("inserting 1st chunk into the table ....")
    df.to_sql(name=table_name, con=engine, if_exists='append')


    i = 2
    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted chunk %d, took %.3f second' % (i, t_end - t_start))
            i += 1

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)