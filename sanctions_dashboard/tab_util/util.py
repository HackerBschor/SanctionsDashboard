import pandas as pd


def df_to_excel(df, sheet_name):
    def to_xlsx(bytes_io):
        xslx_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")
        pd.DataFrame(df).to_excel(xslx_writer, index=False, sheet_name=sheet_name)
        xslx_writer.sheets[sheet_name].autofit()
        xslx_writer.close()

    return to_xlsx


def create_country_list(engine, col=None):
    if col is not None:
        sql = f"SELECT DISTINCT {col}, description FROM entities_countries JOIN countries ON ({col} = alpha_2)"
    else:
        sql = """SELECT alpha_2, description FROM (
                        SELECT source_country AS alpha_2 FROM entities_countries
                        UNION
                        SELECT target_country AS alpha_2 FROM entities_countries
                    ) a
                    JOIN countries USING (alpha_2)"""
    country_list = pd.read_sql(sql, con=engine)
    return [{"label": row[1], "value": row[0]} for row in country_list.values]