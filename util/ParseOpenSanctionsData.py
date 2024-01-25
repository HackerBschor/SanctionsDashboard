import json
import requests
import datetime
import psycopg2
from colorama import Fore, Style

from util.DB import get_connection


def write_entities(file="../data/entities.ftm.json", schemas_file="../data/schemas.txt") -> None:
    """
    Inserts an OpenSanctions Default Dataset (https://www.opensanctions.org/datasets/default/) into the database.
    The dataset has to be in the FollowTheMoney format (entities.ftm.json).
    :return:
    """
    conn = get_connection()
    cursor = conn.cursor()
    schemas = {" "}

    with open(file, 'r', encoding="UTF-8") as fd:
        for count, entity in enumerate(fd):
            print("\rLine: ", count, end="")

            entity = json.loads(entity)

            caption = entity['caption'] if len(entity['caption']) < 256 else entity['caption'][:253] + "..."

            cursor.execute(
                """INSERT INTO public.entities 
                    (id, caption, schema, properties, referents, datasets, first_seen, last_seen, last_change, target) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (entity['id'], caption, entity['schema'], json.dumps(entity['properties']),
                 json.dumps(entity['referents']), json.dumps(entity['datasets']), entity['first_seen'],
                 entity['last_seen'], entity['last_change'], entity['target']))

            schemas.add(entity['schema'])

    conn.commit()
    conn.close()

    with open(schemas_file, 'w') as fd:
        fd.write("\n".join(schemas))


def download_datasets(sanctions_index: json) -> None:
    """
    Downloads the datasource and saves it in the database
    :parameter sanctions_index: The index file of the OpenSanctions default dataset as JSON.
        Usually at https://data.opensanctions.org/datasets/<YYYMMDD>/default/index.json
    :return:
    """
    conn = get_connection()
    cursor = conn.cursor()

    for name in sanctions_index["datasets"]:
        print("Saving dataset: ", name, end=" ")

        data = download_dataset(datetime.date.today(), name)

        if data is not None:
            cursor.execute(
                """INSERT INTO datasets (name, title, url, index_url, summary, description, publisher) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (data['name'], data['title'], data['url'] if "url" in data else None, data['index_url'],
                 data['summary'] if 'summary' in data else None, data['description'] if "description" in data else None,
                 json.dumps(data['publisher']) if "publisher" in data else None))

        print((f"{Fore.GREEN}OK" if data is not None else f"{Fore.RED}Failed") + Style.RESET_ALL)

    conn.commit()
    conn.close()


def download_dataset(date, name, retries=100) -> json:
    """
    Downloads the datasource and saves it in the database
    :parameter:
    :return: JSON
    """
    if retries < 0:
        return None

    response = requests.get(f"https://data.opensanctions.org/datasets/{date.strftime('%Y%m%d')}/{name}/index.json")

    if response.status_code == 200:
        return response.json()
    else:
        return download_dataset(date - datetime.timedelta(days=1), name, retries - 1)


def create_country_relation_table():
    sql = """
        INSERT INTO entries_countries (id, caption, schema, target_country, source_country, first_seen, last_seen, last_change, target)
        
        SELECT * FROM (
            SELECT DISTINCT id, caption, schema,
                json_array_elements_text(properties->'country') AS target_country,
                publisher->>'country' as source_country,
                first_seen, last_seen, last_change, target
            FROM (
                SELECT id, caption, schema, first_seen, last_seen, last_change, target, properties, json_array_elements_text(datasets) AS name
                FROM entities
            ) e
            JOIN datasets USING (name)
        ) f WHERE source_country IS NOT NULL AND target_country IS NOT NULL"""

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def create_subset():
    sql = """/*CREATE INDEX ON entities_datasets (schema);*/
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
    #create_schema()
    #write_entities("data")
    with open("data/index.json", "r") as f:
        download_datasets(json.load(f))
