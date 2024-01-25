import psycopg2

schema: str = """
    DROP TABLE IF EXISTS entities;
    CREATE TABLE entities (
        id TEXT PRIMARY KEY,
        caption TEXT,
        schema TEXT,
        properties JSON,
        referents JSON,
        datasets JSON,
        first_seen timestamp,
        last_seen timestamp,
        last_change timestamp,
        target boolean,
        industry text
    );

    DROP TABLE IF EXISTS datasets;
    CREATE TABLE datasets (
        name TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        index_url TEXT,
        summary TEXT,
        description TEXT,
        publisher JSON
    );

    DROP TABLE IF EXISTS entries_countries;
    CREATE TABLE entries_countries (
        id varchar(200),
        target_country varchar(8),
        source_country varchar(8),
        schema varchar(16),
        first_seen timestamp,
        last_seen timestamp,
        last_change timestamp,
        target boolean,
        industry text
    );

    DROP TABLE IF EXISTS countries; 
    CREATE TABLE countries (
        alpha_2 VARCHAR(2),
        alpha_3 VARCHAR(3),
        flag VARCHAR(3),
        name VARCHAR(50)
    );

    DROP TABLE IF EXISTS orbis_companies;
    CREATE TABLE orbis_companies (
        id SERIAL PRIMARY KEY ,
        name VARCHAR(200),
        alpha_2 VARCHAR(2),
        bvd_sectors VARCHAR(60)
    );
    
    DROP TABLE IF EXISTS companies;
    CREATE TABLE companies (
        ID INTEGER PRIMARY KEY,
        name	TEXT,
        domain	TEXT,
        year_founded INTEGER,
        industry TEXT,
        size_range	TEXT,
        locality TEXT,
        country	TEXT,
        linkedin_url TEXT,
        current_employee_estimate INTEGER,
        total_employee_estimate	INTEGER
    );
    """


def get_connection(host="localhost", database="sanctions", user="sanctions", password="sanctions"):
    """
    Creates connection to database
    :return: postgresql connection
    """
    return psycopg2.connect(host=host, database=database, user=user, password=password)


def create_schema() -> None:
    """
    Creates the database schema
    :return:
    """
    conn: psycopg2.connection = get_connection()
    cursor: psycopg2.cursor = conn.cursor()
    cursor.execute(schema)
    conn.commit()
    conn.close()