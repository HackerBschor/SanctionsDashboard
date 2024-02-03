import pandas as pd
import plotly.express as px
from sqlalchemy import Engine
import plotly.graph_objs as go


def generate_country_data(mode: str, country: str, schema: str, industry: str, start_date: str, end_date: str,
                          engine: Engine) -> pd.DataFrame:

    col: str = "target_country" if "Sanctions towards" == mode else "source_country"
    conditions: list[str] = ["source_country != target_country", f"{col} = %(c)s"]

    if schema is not None and schema.strip() != "":
        conditions.append("schema = %(s)s")

    if industry is not None and industry.strip() != "":
        conditions.append("industry = %(i)s")

    if start_date is not None and start_date.strip() != "":
        conditions.append("first_seen > %(sd)s")

    if end_date is not None and end_date.strip() != "":
        conditions.append("first_seen < %(ed)s")

    sql: str = f"""SELECT 
        id, caption, first_seen, schema, industry, t.description AS target, s.description AS source
    FROM entities_countries 
    JOIN countries t ON (t.alpha_2 = target_country) 
    JOIN countries s ON (s.alpha_2 = source_country)
    WHERE { ' AND '.join(conditions) }"""

    params: dict = {"c": country, "s": schema, "i": industry, "sd": start_date, "ed": end_date}

    return pd.read_sql(sql, params=params, con=engine)


def create_graphs(mode: str, country: str, schema: str, industry: str, start_date: str, end_date: str, engine: Engine
                  ) -> (go.Figure, go.Figure, go.Figure, go.Figure, dict):

    if mode is None or country is None:
        plt1 = px.bar(pd.DataFrame({"Country": [], "Amount": []}), x="Country", y="Amount")
        plt2 = px.bar(pd.DataFrame({"Date": [], "Amount": []}), x="Date", y="Amount")
        plt3 = px.bar(pd.DataFrame({"Schema": [], "Amount": []}), x="Schema", y="Amount")
        plt4 = px.bar(pd.DataFrame({"Industry": [], "Amount": []}), x="Industry", y="Amount")

        return plt1, plt2, plt3, plt4, []

    df: pd.DataFrame = generate_country_data(mode, country, schema, industry, start_date, end_date, engine)

    col: str = "source" if mode == "Sanctions towards" else "target"
    df1: pd.DataFrame = df.groupby(col)["id"].nunique().reset_index().sort_values(by="id")
    plt1: go.Figure = px.bar(df1, x=col, y="id", labels={"id": "Amount", col: "Country"})

    df_melt: pd.DataFrame = pd.melt(
        df.rename(columns={"first_seen": "First Seen", "last_seen": "Last Seen", "last_change": "Last Change"}),
        id_vars=('id',), value_vars=['First Seen'], var_name="value", value_name="date")

    df_melt["date"] = pd.to_datetime(df_melt["date"])
    df_melt["date"] = df_melt["date"].dt.date
    df_melt: pd.DataFrame = df_melt.groupby(["value", "date"])[("id",)].nunique().reset_index()

    plt2: go.Figure = px.line(df_melt, x="date", y="id", color="value", labels={"id": "Amount", "date": "Date", "value": "Type"})

    plt3: go.Figure = px.bar(df["schema"].value_counts().sort_values(), labels={"value": "Count", "schema": "Schema"})

    plt4: go.Figure = px.bar(df["industry"].value_counts().sort_values(), labels={"value": "Count", "schema": "Schema"})

    return plt1, plt2, plt3, plt4, df.to_dict("records")
