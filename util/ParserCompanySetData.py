import sys

import pandas as pd
import numpy as np

from DB import get_connection


def parser_company_set_data(input_file: str):
    df: pd.DataFrame = pd.read_csv(input_file).replace({np.nan: None})

    conn = get_connection()
    cursor = conn.cursor()

    for i, (idx, row) in enumerate(df.iterrows()):
        print(f"\r{float(i) / float(len(df))}", end="")

        year_founded: int = None if row['year founded'] is None else int(row['year founded'])

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


def extract_industries(output_file: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT industry FROM companies")

    with open(output_file, "w") as f:
        f.write("\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\n")

    conn.close()


if __name__ == '__main__':
    modes = [
        ("parser_company_set_data", parser_company_set_data, "<input: path to companies_sorted.csv>"),
        ("extract_industries", extract_industries, "<output: path to industries.txt>")
    ]

    if len(sys.argv) != 3 or sys.argv[1] not in list(map(lambda x: x[0], modes)):
        msg = "Please provide mode and input/ output file. \nAvailable modes: "
        msg += ';'.join(map(lambda x: x[0] + ' ' + x[2], modes))
        exit(msg)

    for mode in modes:
        if sys.argv[1] == mode[0]:
            mode[1](sys.argv[2])