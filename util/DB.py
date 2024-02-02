import psycopg2

sql_schema: str = """
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
        publisher JSON,
        type TEXT
    );

    DROP TABLE IF EXISTS entities_countries;
    CREATE TABLE entities_countries (
        id VARCHAR(255),
        caption TEXT,
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
        name VARCHAR(50),
        description VARCHAR (60)
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

sql_indices: str = """
    CREATE INDEX ON entities(LOWER(caption));
    CREATE INDEX ON companies(LOWER(name));
    CREATE INDEX ON entities(id);
    CREATE INDEX ON entities_countries(id);
        
    CREATE INDEX ON entities(LOWER(caption));
    CREATE INDEX ON entities(schema);
    CREATE INDEX ON entities_countries(source_country);
"""

sql_update_industries: str = """
    UPDATE entities e1 SET industry = c.industry 
    FROM entities e2
    JOIN (SELECT name, industry FROM companies) c ON (LOWER(name) = LOWER(caption))
    WHERE e1.id = e2.id;
    
    UPDATE entities_countries ec SET industry = e.industry
    FROM entities e
    WHERE e.id = ec.id;
"""


def get_connection(host="localhost", database="sanctions", user="sanctions", password="sanctions"):
    """
    Creates connection to database
    :return: postgresql connection
    """
    return psycopg2.connect(host=host, database=database, user=user, password=password)


def execute_insert_update_query(sql) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def create_schema() -> None:
    """
    Creates the database schema
    :return:
    """
    execute_insert_update_query(sql_schema)


def create_indexes() -> None:
    """
    Creates the database indexes
    :return:
    """
    execute_insert_update_query(sql_indices)


def update_industries() -> None:
    execute_insert_update_query(sql_update_industries)
