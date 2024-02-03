import pandas as pd
from sqlalchemy import Engine


def df_to_excel(df: pd.DataFrame, sheet_name: str) -> callable:
    def to_xlsx(bytes_io):
        xslx_writer: pd.ExcelWriter = pd.ExcelWriter(bytes_io, engine="xlsxwriter")
        pd.DataFrame(df).to_excel(xslx_writer, index=False, sheet_name=sheet_name)
        xslx_writer.sheets[sheet_name].autofit()
        xslx_writer.close()

    return to_xlsx


def create_country_list(engine: Engine, col: [None, str] = None) -> list[dict]:
    if col is not None:
        sql: str = f"SELECT DISTINCT {col}, description FROM entities_countries JOIN countries ON ({col} = alpha_2)"
    else:
        sql: str = """SELECT alpha_2, description FROM (
                        SELECT source_country AS alpha_2 FROM entities_countries
                        UNION
                        SELECT target_country AS alpha_2 FROM entities_countries
                    ) a
                    JOIN countries USING (alpha_2)"""

    country_list: pd.DataFrame = pd.read_sql(sql, con=engine)

    return [{"label": row[1], "value": row[0]} for row in country_list.values]
