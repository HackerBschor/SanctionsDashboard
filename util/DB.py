import sys

import psycopg2


def get_connection(host="localhost", database="sanctions", user="sanctions", password="sanctions"):
    """
    Creates connection to database
    :return: postgresql connection
    """
    return psycopg2.connect(host=host, database=database, user=user, password=password)


def execute_insert_update_query(sql) -> None:
    """
    Executes SQL query
    :return:
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        exit("Usage: python3 DB.py <SQL Files>")

    with open(sys.argv[1]) as f:
        execute_insert_update_query(f.read())
