import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, analysis_queries


def load_staging_tables(cur, conn):
    """
    load data from S3 to staging tables in database
    :param cur: cursor of connection
    :param conn: connection to database
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    move data from staging tables to star schema's tables
    :param cur: cursor of connection
    :param conn: connection to database
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def get_most_played_song(cur, conn):
    """
    execute most played song query
    :param cur: cursor of connection
    :param conn: connection to database
    :return: dataframe of song most played
    """
    query = analysis_queries[0]
    print(query)
    cur.execute(query)
    records = cur.fetchall()
    df_output = pd.DataFrame(list(records), columns=["song_id", "title", "nums", "artist_name", "year", "duration"])
    conn.commit()
    return df_output


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    df_output = get_most_played_song(cur, conn)
    print(df_output)
    conn.close()


if __name__ == "__main__":
    main()
