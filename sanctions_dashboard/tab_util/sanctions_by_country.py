import pandas as pd


def generate_country_data(engine, mode, country, schema, industry, start_date, end_date):
    col = "target_country" if "Sanctions towards" == mode else "source_country"
    conditions = ["source_country != target_country", f"{col} = %(c)s"]

    if schema is not None and schema.strip() != "":
        conditions.append("schema = %(s)s")

    if industry is not None and industry.strip() != "":
        conditions.append("industry = %(i)s")

    if start_date is not None and start_date.strip() != "":
        conditions.append("first_seen > %(sd)s")

    if end_date is not None and end_date.strip() != "":
        conditions.append("first_seen > %(ed)s")

    sql = f"""SELECT 
        id, caption, first_seen, last_change, last_seen, schema, industry, t.description AS target, s.description AS source
    FROM entities_countries 
    JOIN countries t ON (t.alpha_2 = target_country) 
    JOIN countries s ON (s.alpha_2 = source_country)
    WHERE { ' AND '.join(conditions) }"""

    params = {"c": country, "s": schema, "i": industry, "sd": start_date, "ed": end_date}

    return pd.read_sql(sql, params=params, con=engine)