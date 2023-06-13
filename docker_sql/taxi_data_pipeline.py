import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(args):

    engine=create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')
    
    # download parquet
    parquet_name='output.parquet'
    print('ready to download parquet data')
    os.system(f'wget {args.url} -O {parquet_name}')
    print('data has been downloaded')
    print(os.listdir())
    data=pd.read_parquet(parquet_name)
    data.to_sql(name=args.table_name, con=engine, if_exists='replace')
    print('data has been ingested into PG database')

    
if __name__ == '__main__':

    parser=argparse.ArgumentParser(description='Ingest data to Postgres database')

    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='name of table where we will write the results to')
    parser.add_argument('--url',help='url of the csv file')
    args=parser.parse_args()

    main(args)