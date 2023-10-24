import json

import requests
import datetime
import psycopg2

schema = """
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
    target boolean
);

DROP TABLE IF EXISTS names;
CREATE TABLE names (
    name TEXT PRIMARY KEY
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

DROP TABLE IF EXISTS entities_datasets;
CREATE TABLE entities_datasets (
    id varchar(200) PRIMARY KEY,
    caption varchar(256),
    schema varchar(16),
    first_seen timestamp,
    last_seen timestamp,
    last_change timestamp,
    target BOOLEAN,
    properties JSON,
    datasets_names JSON,
    datasets JSON
);
"""


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="opensanctions",
        user="postgres",
        password="postgres")


def get_open_sanctions_index():
    date = datetime.date.today().strftime('%Y%m%d')
    response = requests.get(f'https://data.opensanctions.org/datasets/{date}/default/index.json')
    return response.json()


def download_data():
    sanctions_index = get_open_sanctions_index()
    for resource in sanctions_index["resources"]:
        name = resource["name"]
        url = resource["url"]

        print("Saving resource: ", name)
        response = requests.get(url)
        with open(f'data/{name}', 'wb') as f:
            f.write(response.content)


def parse_data(path):
    write_entities(path)
    # write_names(path)


def write_entities(path):
    entities = f"{path}/entities.ftm.json"
    conn = get_connection()
    cursor = conn.cursor()

    with open(entities, 'r', encoding="UTF-8") as f:
        for count, entity in enumerate(f):
            print("\rLine: ", float(count) * 100.0 / 2719888.0, end="")
            entity = json.loads(entity)

            caption = entity['caption'] if len(entity['caption']) < 256 else entity['caption'][:253] + "..."

            cursor.execute(
                """INSERT INTO public.entities 
                    (id, caption, schema, properties, referents, datasets, first_seen, last_seen, last_change, target) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (entity['id'], caption, entity['schema'], json.dumps(entity['properties']), json.dumps(entity['referents']), json.dumps(entity['datasets']), entity['first_seen'], entity['last_seen'], entity['last_change'], entity['target']))

    conn.commit()
    conn.close()


def write_names(path):
    names = f"{path}/names.txt"
    conn = get_connection()
    cursor = conn.cursor()

    with open(names, 'r', encoding="UTF-8") as f:
        for count, name in enumerate(f):
            print("\rLine: ", count, name.strip(), end="")
            cursor.execute("INSERT INTO public.names (name) VALUES (%s)", (name, ))

    conn.commit()
    conn.close()


def save_data_sources():
    data = get_open_sanctions_index()

    conn = get_connection()
    cursor = conn.cursor()

    date = datetime.date.today().strftime('%Y%m%d')

    for name in data["datasets"]:
        print("Saving dataset: ", name, end=" ")
        result = save_data_source(date, name, cursor, True)
        print("OK" if result else "Failed")

    conn.commit()
    conn.close()


def save_data_source(date, name, cursor, retry):
    response = requests.get(f"https://data.opensanctions.org/datasets/{date}/{name}/index.json")

    if response.status_code != 200:
        if retry:
            for i in range(10): # try 10 days back (Sometimes data not available for current date)
                date = datetime.date.today()
                date = (date - datetime.timedelta(days=i)).strftime('%Y%m%d')
                if save_data_source(date, name, cursor, False):
                    return True

        return False

    data = response.json()

    cursor.execute(
        """INSERT INTO datasets (name, title, url, index_url, summary, description, publisher) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (data['name'], data['title'], data['url'] if "url" in data else None, data['index_url'], data['summary'] if 'summary' in data else None, data['description'] if "description" in data else None, json.dumps(data['publisher']) if "publisher" in data else None))

    return True


def join_entries_datasets():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO entities_datasets 
            SELECT 
                id, caption, schema, first_seen, last_seen, last_change, target, properties,  
                e.datasets AS datasets_names, d.datasets AS datasets
            FROM entities e
            JOIN (
                SELECT id, jsonb_agg(dataset) AS datasets
                FROM ( SELECT id, json_array_elements_text(datasets) AS dataset_name FROM entities ) e
                LEFT JOIN (SELECT name, json_build_object('name', name, 'url', url, 'index_url', index_url, 'summary', summary, 'title', title, 'description', description, 'publisher', publisher) AS dataset FROM datasets) d ON (e.dataset_name = d.name)
                GROUP BY id
            ) AS d USING (id)""")
    conn.commit()
    conn.close()


def extract_ds_publisher():
    sql = """SELECT json_agg(jsonb_build_object('published_name', ds->'publisher'->>'name','country', ds->'publisher'->>'country', 'country_label', ds->'publisher'->>'country_label', 'official', ds->'publisher'->>'official')) dataset_publishers
    FROM entities_datasets, json_array_elements(datasets) AS ds"""


def create_subset():
    sql = """--CREATE INDEX ON entities_datasets (schema);
CREATE TABLE entities_datasets_small AS
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Address' ORDER BY random() LIMIT 1000) a0 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Airplane' ORDER BY random() LIMIT 1000) a1 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Associate' ORDER BY random() LIMIT 1000) a2 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'BankAccount' ORDER BY random() LIMIT 1000) a3 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Company' ORDER BY random() LIMIT 1000) a4 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'CryptoWallet' ORDER BY random() LIMIT 1000) a5 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Directorship' ORDER BY random() LIMIT 1000) a6 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Employment' ORDER BY random() LIMIT 1000) a7 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Family' ORDER BY random() LIMIT 1000) a8 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Identification' ORDER BY random() LIMIT 1000) a9 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'LegalEntity' ORDER BY random() LIMIT 1000) a10 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Membership' ORDER BY random() LIMIT 1000) a11 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Occupancy' ORDER BY random() LIMIT 1000) a12 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Organization' ORDER BY random() LIMIT 1000) a13 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Ownership' ORDER BY random() LIMIT 1000) a14 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Passport' ORDER BY random() LIMIT 1000) a15 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Person' ORDER BY random() LIMIT 1000) a16 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Position' ORDER BY random() LIMIT 1000) a17 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Representation' ORDER BY random() LIMIT 1000) a18 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Sanction' ORDER BY random() LIMIT 1000) a19 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Security' ORDER BY random() LIMIT 1000) a20 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'UnknownLink' ORDER BY random() LIMIT 1000) a21 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Vessel' ORDER BY random() LIMIT 1000) a22"""

if __name__ == '__main__':
    save_data_sources()
