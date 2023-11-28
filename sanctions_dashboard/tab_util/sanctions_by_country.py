import pandas as pd


def generate_country_data(engine, mode, country, schema):
    print(mode, country, schema)

    col = "target_country" if "Sanctions towards" == mode else "source_country"
    cond_schema = "" if schema is None or schema == "" else ' AND schema = %(s)s'

    sql = f"""SELECT 
        id, first_seen, last_change, last_seen, schema, t.description AS target, s.description AS source 
    FROM entries_countries 
    JOIN countries t ON (t.alpha_2 = target_country) JOIN countries s ON (s.alpha_2 = source_country)
    WHERE {col} = %(c)s {cond_schema}"""

    return pd.read_sql(sql, params={"c": country, "s": schema}, con=engine)