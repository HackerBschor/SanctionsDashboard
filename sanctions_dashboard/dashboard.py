import os

import pandas as pd
import pycountry
from dash import Dash, html, dcc, callback, Output, Input, dash_table, State
from flask import send_from_directory
from sqlalchemy import create_engine
import plotly.express as px

app = Dash(__name__, external_stylesheets=["/static/style.css"])


# app.css.config.serve_locally = True
# app.scripts.config.serve_locally = True

countries = [{"alpha_2": c.alpha_2, "alpha_3": c.alpha_3, "flag": c.flag, "name": c.name} for c in pycountry.countries]
countries.append({"alpha_2": "EU", "alpha_3": "EU", "flag": '🇪🇺', "name": "European Union"})
countries = pd.DataFrame(countries)

engine = create_engine("postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions")


def create_country_list(col):
    sql = f"SELECT DISTINCT UPPER({col}) AS alpha_2 FROM entries_countries"
    country_list = pd.read_sql(sql, con=engine).merge(countries, on="alpha_2")
    country_list["description"] = country_list.apply(lambda x: f"{x['name']} {x['flag']}", axis=1)
    return [{"label": row[4], "value": row[0]} for row in country_list.values]


target_countries = create_country_list("target_country")
source_countries = create_country_list("source_country")

schemas = ['Address', 'Airplane', 'Associate', 'BankAccount', 'Company', 'CryptoWallet', 'Directorship', 'Employment',
           'Family', 'Identification', 'LegalEntity', 'Membership', 'Occupancy', 'Organization', 'Ownership',
           'Passport', 'Person', 'Position', 'Representation', 'Sanction', 'Security', 'UnknownLink', 'Vessel']

app.layout = html.Div([
    dcc.Tabs(id="tabs-select", value='Sanctions by Country', children=[
        dcc.Tab(label='Sanctions by Country', value='Sanctions by Country', children=[
            html.Div([
                html.Div([
                    html.Div([dcc.Dropdown(
                        ["Sanctions towards", "Sanctions from"], value="Sanctions towards",
                        id='dd-sanction-mode', style={"width": "200px"})], style={"display": "table-cell"}),
                    html.Div(style={"display": "table-cell", "width": "10px"}),
                    html.Div(
                        [dcc.Dropdown(target_countries, id='dd-country', style={"width": "250px"})],
                        style={"display": "table-cell"}),
                    html.Div(style={"display": "table-cell", "width": "10px"}),
                    html.Div(
                        [dcc.Dropdown(schemas, id='dd-schemas', style={"width": "150px"}, placeholder="Schema")],
                        style={"display": "table-cell"}),
                    html.Div(style={"display": "table-cell", "width": "10px"}),
                    html.Div([
                        html.Button("Excel Export", id="btn-export", n_clicks=0, className="modern-button")
                    ], style={"display": "table-cell"})
                ], style={"display": "table"}),
            ], style={"display": "table", "margin-top": "10px"}),
            dcc.Graph(id="graph-sanctions-by-country"),
            html.Div([
                html.Div([dcc.Graph(id="graph-sanctions-timeline")], style={"display": "table-cell"}),
                html.Div([dcc.Graph(id="graph-types")], style={"display": "table-cell"}),
            ], style={"display": "table", "width": "100%"}),

            dash_table.DataTable(
                data=[], id='tbl', style_data={'whiteSpace': 'normal', 'height': 'auto', 'lineHeight': '15px'}, )
        ]),

        dcc.Tab(label='Individuals', value='Individuals', children=[
            html.Div([
                html.Div(
                    [dcc.Dropdown(schemas, id='dd-individ-schemas', style={"width": "150px"}, placeholder="Schema")],
                    style={"display": "table-cell", }),
                html.Div(style={"display": "table-cell", "width": "10px"}),
                html.Div(
                    [dcc.Input(id="input-search-caption", type="text", placeholder="Search", style={"width": "300px"}, debounce=True)],
                    style={"display": "table-cell"})
            ], style={"display": "table", "margin-top": "10px"}),
            # style_data={'whiteSpace': 'normal', 'height': 'auto', 'lineHeight': '15px'}
            dash_table.DataTable(data=[], id='tbl-individ-results')
        ]),
        dcc.Tab(label='Network Analysis', value='Network Analysis', children=[]),
        dcc.Tab(label='Industries', value='Industries', children=[]),
    ]),
    dcc.Download(id="download")
])


"""
@app.server.route('/static/<path:path>')
def static_file(path):
    static_folder = os.path.join(os.getcwd(), 'static')
    return send_from_directory(static_folder, path)
"""


@callback(
    Output("dd-country", "options"),
    [Input("dd-sanction-mode", "value")]
)
def update_graph(value):
    return target_countries if value == "Sanctions towards" else source_countries


def generate_country_data(mode, country, schema):
    col = "target_country" if "Sanctions towards" == mode else "source_country"
    sql = f"SELECT * FROM entries_countries WHERE UPPER({col}) = %(c)s{'' if schema is None else ' AND schema = %(s)s'}"

    df = pd.read_sql(sql, params={"c": country, "s": schema}, con=engine)
    df["target_country"] = df["target_country"].str.upper()
    df["source_country"] = df["source_country"].str.upper()

    return df.merge(countries, left_on="source_country", right_on="alpha_2") \
        .merge(countries, left_on="target_country", right_on="alpha_2", suffixes=("_source", "_target"))


@callback(
    [Output("graph-sanctions-by-country", "figure"),
     Output("graph-sanctions-timeline", "figure"),
     Output("graph-types", "figure"),
     Output('tbl', 'data')],
    [Input("dd-sanction-mode", "value"),
     Input("dd-country", "value"),
     Input("dd-schemas", "value")]
)
def update_graph(mode, country, schema):
    plt1 = px.bar(pd.DataFrame({"country": [], "amount": []}), x="country", y="amount")
    plt2 = px.bar(pd.DataFrame({"date": [], "amount": []}), x="date", y="amount")
    plt3 = px.bar(pd.DataFrame({"date": [], "amount": []}), x="date", y="amount")
    tbl1 = []
    if mode is None or country is None:
        return plt1, plt2, plt3, tbl1

    df = generate_country_data(mode, country, schema)

    col = "name_source" if mode == "Sanctions towards" else "name_target"
    df1 = df.groupby(col)["id"].nunique().reset_index()
    plt1 = px.bar(df1, x=col, y="id", labels={"id": "Amount", col: "Country"})

    df_melt = pd.melt(
        df.rename(columns={"first_seen": "First Seen", "last_seen": "Last Seen", "last_change": "Last Change"}),
        id_vars=['id'], value_vars=['First Seen', 'Last Seen', 'Last Change'], var_name='value', value_name='date')
    df_melt["date"] = pd.to_datetime(df_melt["date"])
    df_melt["date"] = df_melt["date"].dt.date
    df_melt = df_melt.groupby(["value", "date"])["id"].nunique().reset_index()

    plt2 = px.line(df_melt, x="date", y="id", color="value", labels={"id": "Amount", "date": "Date", "value": "Type"})

    plt3 = px.bar(df["schema"].value_counts(), labels={"value": "Count", "schema": "Schema"})

    tbl1 = df[["id", "caption", "schema", "name_source"]]\
        .rename(columns={"name_source": "Sanctioned By", "name_target": "Sanctions"}).to_dict("records")

    return plt1, plt2, plt3, tbl1


@callback(
    Output("download", "data"),
    [Input("btn-export", "n_clicks"),
     State("dd-sanction-mode", "value"),
     State("dd-country", "value"),
     State("dd-schemas", "value")],
    prevent_initial_call=True)
def download(clicked, mode, country, schema):
    def to_xlsx(bytes_io):
        xslx_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")  # requires the xlsxwriter package
        pd.DataFrame(generate_country_data(mode, country, schema)).to_excel(xslx_writer, index=False,
                                                                            sheet_name="sheet1")
        xslx_writer.close()

    return dcc.send_bytes(to_xlsx, "test.xlsx")


@callback(
    Output("tbl-individ-results", "data"),
    [Input("dd-individ-schemas", "value"),
     Input("input-search-caption", "value")],
    prevent_initial_call=True)
def download(schema, query):
    if query is None or len(query.strip()) == 0:
        return []

    restriction = []

    if schema is not None:
        restriction.append("schema = %(schema)s")

    sql = f"SELECT * FROM entities WHERE LOWER(caption) LIKE concat('%%', LOWER(%(query)s) ,'%%')"
    sql += "" if len(restriction) == 0 else " AND " + " AND ".join(restriction)

    df = pd.read_sql(sql, params={"schema": schema, "query": query}, con=engine)

    return df[["id", "schema", "caption"]].to_dict("records")


if __name__ == '__main__':
    app.run(debug=True)
