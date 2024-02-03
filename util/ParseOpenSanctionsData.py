import json
import sys

import requests
import datetime
from colorama import Fore, Style

from DB import get_connection


def write_entities(input_file: str) -> None:
    conn = get_connection()
    cursor = conn.cursor()

    with open(input_file, 'r', encoding="UTF-8") as fd:
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

    conn.commit()
    conn.close()


def download_datasets(input_file: str) -> None:
    with open(input_file, 'r') as f:
        sanctions_index: json = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    for name in sanctions_index["datasets"]:
        print("Saving dataset: ", name, end=" ")

        data: json = download_dataset(datetime.date.today(), name)

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


def download_dataset(date: datetime.datetime, name: str, retries: int = 100) -> json:
    if retries < 0:
        return None

    url: str = f"https://data.opensanctions.org/datasets/{date.strftime('%Y%m%d')}/{name}/index.json"
    response: requests.Response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return download_dataset(date - datetime.timedelta(days=1), name, retries - 1)


def extract_schemas(output_file: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT schema FROM entities")

    with open(output_file, "w") as f:
        f.write("\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\n")

    conn.close()


if __name__ == '__main__':
    modes: list[tuple] = [
        ("download_datasets", download_datasets, "<input: path to index.json>"),
        ("write_entities", write_entities, "<input: path to entities.ftm.json>"),
        ("extract_schemas", extract_schemas, "<output: path to schemas.txt>")
    ]

    if len(sys.argv) != 3 or sys.argv[1] not in list(map(lambda x: x[0], modes)):
        msg = "Please provide mode and input/ output file. \nAvailable modes: "
        msg += ';'.join(map(lambda x: x[0] + ' ' + x[2], modes))
        exit(msg)

    for mode in modes:
        if sys.argv[1] == mode[0]:
            mode[1](sys.argv[2])
