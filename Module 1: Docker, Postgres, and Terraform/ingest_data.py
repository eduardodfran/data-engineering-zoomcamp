#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

@click.command()
@click.option('--year', default=2021, help='Year of data')
@click.option('--month', default=1, help='Month of data')
@click.option('--url', default='', help='Source URL for CSV or Parquet')
@click.option('--pg_user', default='root', help='Postgres User')
@click.option('--pg_password', default='root', help='Postgres Password')
@click.option('--pg_host', default='localhost', help='Postgres Host')
@click.option('--pg_port', default='5432', help='Postgres Port')
@click.option('--pg_db', default='ny_taxi', help='Postgres Database')
@click.option('--table_name', default='yellow_taxi_data', help='Postgres Table Name')
@click.option('--chunk_size', default=100000, help='Chunk size for processing')
def ingest_data(year, month, url, pg_user, pg_password, pg_host, pg_port, pg_db, table_name, chunk_size):
    if not url:
        raise click.BadParameter("--url is required. Provide a CSV or Parquet URL.")



    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    # Check if the URL points to a parquet file
    if url.endswith('.parquet'):
        print(f"Detected Parquet file: {url}")
        
        # Read parquet file directly
        df = pd.read_parquet(url)
        
        # Create/Replace table schema and insert all data
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        print(f"Finished ingesting {len(df)} rows from Parquet file.")
    
    else:
        print(f"Detected CSV file: {url}")
        
        # df_iter = pd.read_csv(
        #     url,
        #     dtype=dtype,
        #     parse_dates=parse_dates,
        #     iterator=True,
        #     chunksize=chunk_size
        # )
        
        df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunk_size
    )

        # Initialize schema and insert first chunk
        try:
            df = next(df_iter)
            
            # Create/Replace table schema
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            
            # Insert first chunk
            df.to_sql(name=table_name, con=engine, if_exists='append')
            print('Inserted first chunk initialization.')

        except StopIteration:
            print("Source data is empty.")

        # Process remaining chunks with tqdm
        for df in tqdm(df_iter, desc="Ingesting data"):
            df.to_sql(name=table_name, con=engine, if_exists='append')

        print("Finished ingesting all data.")

if __name__ == '__main__':
    ingest_data()
