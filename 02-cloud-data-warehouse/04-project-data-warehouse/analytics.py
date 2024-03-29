import configparser
import psycopg2
from sql_queries import count_rows_queries

def get_row_counts(cur, conn):
    '''
    Get the number of rows stored into each table
    '''
    for query in count_rows_queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("rows in the created table: ", row)


def main():
    '''
    Count rows on the staging and dimensional tables to validate that the project has been created successfully
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    get_row_counts(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()