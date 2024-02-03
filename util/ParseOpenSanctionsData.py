import json
import sys

import requests
import datetime
import psycopg2
from colorama import Fore, Style

from DB import get_connection, create_schema


def write_entities(file="../data/entities.ftm.json") -> None:
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


def download_datasets(index_file: str = "../data/index.json") -> None:
    """
    Downloads the datasource and saves it in the database
    :parameter sanctions_index: The index file of the OpenSanctions default dataset as JSON.
        Usually at https://data.opensanctions.org/datasets/<YYYMMDD>/default/index.json
    :return:
    """
    with open(index_file, 'r') as f:
        sanctions_index: json = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    for name in sanctions_index["datasets"]:
        print("Saving dataset: ", name, end=" ")

        data = download_dataset(datetime.date.today(), name)

        if data is not None:
            cursor.execute(
                """INSERT INTO datasets (name, title, url, index_url, summary, description, publisher, type) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (data['name'], data['title'], data['url'] if "url" in data else None, data['index_url'],
                 data['summary'] if 'summary' in data else None, data['description'] if "description" in data else None,
                 json.dumps(data['publisher']) if "publisher" in data else None, data['type']))

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
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO entities_countries (id, caption, schema, target_country, source_country, first_seen, last_seen, last_change, target)
        
        SELECT * FROM (
            SELECT DISTINCT id, caption, schema,
                json_array_elements_text(COALESCE(properties->'country', properties->'jurisdiction')) AS target_country,
                publisher->>'country' as source_country,
                first_seen, last_seen, last_change, target
            FROM (
                SELECT id, caption, schema, first_seen, last_seen, last_change, target, properties, json_array_elements_text(datasets) AS name
                FROM entities
            ) e
            JOIN (SELECT * FROM datasets WHERE type <> 'external') d USING (name)
        ) f WHERE source_country IS NOT NULL AND target_country IS NOT NULL"""

    cursor.execute(sql)
    conn.commit()
    conn.close()


def extract_schemas(output_file):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT schema FROM entities")

    with open(output_file, "w") as f:
        f.write("\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\n")

    conn.close()


if __name__ == '__main__':
    modes = ["create_schema", "download_datasets", "write_entities", "create_country_relation_table", "extract_schemas"]

    if len(sys.argv) == 1:
        exit(f"Please provide mode. \n Available modes {' '.join(modes)}")

    mode = sys.argv[1]

    file = None
    if len(sys.argv) > 2:
        file = sys.argv[2]

    if sys.argv[1] == modes[0]:
        create_schema()
    elif sys.argv[1] == modes[1]:
        if file is not None:
            download_datasets(file)
        else:
            download_datasets()

    elif sys.argv[1] == modes[2]:
        if file is not None:
            write_entities(file)
        else:
            write_entities()
    elif sys.argv[1] == "create_country_relation_table":
        create_country_relation_table()
    elif sys.argv[1] == "extract_schemas":
        extract_schemas(sys.argv[2])
    else:
        exit(f"Please provide mode valid mode. \n Available modes: {', '.join(modes)}")
