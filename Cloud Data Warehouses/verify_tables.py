# -*- encoding: utf-8 -*-
"""
Filename: verify_tables.py
Author: Mingxing Jin
Contact: Mingxing.Jin@bmw-brilliance.cn
Description: Connect to AWS Redshift cluster and verify the tables.
"""

import configparser
import psycopg2
from sql_queries import verify_tables_queries
import pandas as pd
pd.set_option('display.max_rows', 200)

# Verify tables
def verify_tables(cur,conn):
    """ Verify tables
    Arguments:
        cur: cursor instance with the connection to the database
        conn: connection instance to the AWS Redshift cluster
        
    Returns:
        Nothing, just run the query on database
    """
    for query in verify_tables_queries:
        try:
            cur.execute(query)
            conn.commit()
            result = cur.fetchall()
            print(query)
            print(result)
        except Exception as e:
            print("Error: Issue verifying table.\n")
            print(e)



def main():
    """ The main scrpts
    Load configration file and connect to AWS Redshift cluster
    Run verify_tables function

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

    print("Start to verify counts of table.\n")
    verify_tables(cur,conn)

    # Close cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()