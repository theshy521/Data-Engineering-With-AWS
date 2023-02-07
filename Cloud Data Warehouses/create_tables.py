# -*- encoding: utf-8 -*-
"""
Filename: create_tables.py
Author: Mingxing Jin
Contact: Mingxing.Jin@bmw-brilliance.cn
Description: Load configration file, connect to the pre-defined AWS Redshift cluster, \
    drop tables if exists and create staging and target tables on AWS Redshift.
"""


import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# Drop tables if exists
def drop_tables(cur, conn):
    """ Drop tables if exists
    Arguments:
        cur: cursor instance with the connection to the database
        conn: connection instance to the AWS Redshift cluster

    Returns:
        Nothing, just run the query on database
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Drop table if exists successfully!!!.\n")
        except Exception as e:
            print("Error: Issue deleting table.\n")
            print(e)

# Create tables for staging and target databased on AWS Redshift
def create_tables(cur, conn):
    """ Create tables for staging and target databased on AWS Redshift
    Arguments:
        cur: cursor instance with the connection to the database
        conn: connection instance to the AWS Redshift cluster
        
    Returns:
        Nothing, just run the query on database
    """
    for query in create_table_queries:
        try:
            
            cur.execute(query)
            conn.commit()
            print("Create table successfully!!!.\n")
        except Exception as e:
            print(query)
            print("Error: Issue creating table.\n")
            print(e)

def main():
    """ The main scrpts
    Load configration file and connect to AWS Redshift cluster
    Run drop_tables and create_tables functions

    Arguments:
        Nothing,
    Returns:
        Nothing, just run the query on database
    """

    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the AWS Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)


    # Close the cursor and connection.
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()