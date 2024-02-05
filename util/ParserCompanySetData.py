import sys
import csv
from io import StringIO

from DB import get_connection


def parser_company_set_data(input_file: str):
    conn = get_connection()
    cursor = conn.cursor()

    with open(input_file, encoding="UTF-8") as f:
        for i, line in enumerate(f):
            print(f"\r{float(i)}", end="")
            if i == 0:
                continue

            row = next(csv.reader(StringIO(line)))
            row = [(None if value == "" else value) for value in row]
            row[3] = None if row[3] is None else int(float(row[3]))

            cursor.execute(
                '''INSERT INTO companies (
                    id, name, domain, year_founded, industry, size_range, locality, country, linkedin_url, 
                    current_employee_estimate, total_employee_estimate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (i, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))

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
