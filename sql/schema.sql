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