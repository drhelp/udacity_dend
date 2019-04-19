import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    '''
    Creates a new database. Drops existsing databases (if any),
    so use with caution!
    '''
    # connect to default database
    conn = psycopg2.connect(
            "host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    '''
    Drops all existsing databases tables. List of queries should be imported
    into drop_table_queries

    Args:
        cur: active database cursor.
        conn: database connection. Connection to DB should be
        already established.

    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates target tables for fact and dimensions in database.
    Database should be emply - call drop_tables to clear in advance.

    Args:
        cur: active database cursor.
        conn: database connection. Connection to DB should be
        already established.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Starting point for database creating.
    Re-create the database and then creates target tables
    '''
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Database created successfully!")


if __name__ == "__main__":
    main()
