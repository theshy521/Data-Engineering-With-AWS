# -*- encoding: utf-8 -*-
"""
Filename: etl.py
Author: Mingxing Jin
Contact: Mingxing.Jin@bmw-brilliance.cn
Description: Load configration file, connect to AWS Redshift cluster, \
    load source data from S3 bucket to staging database on AWS Redshift \
    and insert data from staging tables into final tables.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
pd.set_option('display.max_rows', 200)


# Load source data from S3 bucket to staging tables on AWS Redshift
def load_staging_tables(cur, conn):
    """ Load source data from S3 bucket to staging tables on AWS Redshift
    Arguments:
        cur: cursor instance with the connection to the database
        conn: connection instance to the AWS Redshift cluster
        
    Returns:
        Nothing, just run the query on database
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Load source data to staging table successfully!!!.\n")
        except Exception as e:
            print("Error: Issue loading source data to staging table.\n")
            print(e)

# Insert data  from staging tables into final tables on AWS Redshift
def insert_tables(cur, conn):
    """ Insert data from staging tables into final tables on AWS Redshift
    Arguments:
        cur: cursor instance with the connection to the database
        conn: connection instance to the AWS Redshift cluster
        
    Returns:
        Nothing, just run the query on database
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Insert into target table with proper transform logic successfully!!!.\n")
        except Exception as e:
            print("Error: Issue inserting into target table with proper transform logic.\n")
            print(e)



def main():
    """ The main scrpts
    Load configration file and connect to AWS Redshift cluster
    Run load_staging_tables and insert_tables functions

    Arguments:
        Nothing,
    Returns:
        Nothing, just run the query on database
    """

    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to AWS Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Start to load source data from S3 bucket to staging database on AWS Redshift DataWarehouse.\n")
    load_staging_tables(cur, conn)

    print("Start to insert data to proper target database on AWS Redshift DataWarehouse.\n")
    insert_tables(cur, conn)

    # Close cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()