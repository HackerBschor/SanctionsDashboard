import sys

import pandas as pd
import numpy as np

from DB import get_connection


def parser_company_set_data(input_file):
    df = pd.read_csv(input_file)
    df = df.replace({np.nan: None})

    conn = get_connection()
    cursor = conn.cursor()

    for i, (idx, row) in enumerate(df.iterrows()):
        print(f"\r{float(i) / float(len(df))}", end="")

        year_founded = None if row['year founded'] is None else int(row['year founded'])

        cursor.execute(
            '''INSERT INTO companies (
                id, name, domain, year_founded, industry, size_range, locality, country, linkedin_url, 
                current_employee_estimate, total_employee_estimate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (i, row['name'], row['domain'], year_founded, row['industry'],
             row['size range'], row['locality'], row['country'], row['linkedin url'],
             row['current employee estimate'], row['total employee estimate']))

    conn.commit()
    conn.close()


def extract_industries(output_file):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT industry FROM companies")

    with open(output_file, "w") as f:
        f.write("\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\n")

    conn.close()


if __name__ == '__main__':
    modes = ["parser_company_set_data", "extract_industries"]

    if len(sys.argv) == 1:
        exit(f"Please provide mode. \n Available modes: {', '.join(modes)}")
    if len(sys.argv) == 2:
        exit(f"Please provide input/output file")

    mode, file = sys.argv[1], sys.argv[2]

    if mode == "parser_company_set_data":
        parser_company_set_data(file)
    elif mode == "extract_industries":
        extract_industries(file)
    else:
        exit(f"Please provide mode valid mode. \n Available modes: {', '.join(modes)}")
