import sys

import pandas as pd
import numpy as np

from DB import get_connection

if __name__ == '__main__':
    if len(sys.argv) == 1:
        file = "../data/companies_sorted.csv"
    else:
        file = sys.argv[1]

    df = pd.read_csv('')
    df = df.replace({np.nan: None})

    conn = get_connection()
    cursor = conn.cursor()

    for i, (idx, row) in enumerate(df.iterrows()):
        print(f"\r{float(i) / float(len(df))}", end="")

        year_founded = None if row['year founded'] is None else int(row['year founded'])

        cursor.execute(
            '''INSERT INTO companies (
                name, domain, year_founded, industry, size_range, locality, country, linkedin_url, 
                current_employee_estimate, total_employee_estimate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (row['name'], row['domain'], year_founded, row['industry'],
             row['size range'], row['locality'], row['country'], row['linkedin url'],
             row['current employee estimate'], row['total employee estimate']))

    conn.commit()
    conn.close()
