import json

import dash_bootstrap_components as dbc
import networkx as nx
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input, dash_table, State
from sqlalchemy import create_engine

from sanctions_dashboard.tab_util.network import plot_network, get_centralities
from sanctions_dashboard.tab_util.sanctions_by_country import generate_country_data

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# app.css.config.serve_locally = True
# app.scripts.config.serve_locally = True

engine = create_engine("postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions")


def create_country_list(col):
    sql = f"SELECT DISTINCT {col}, description FROM entries_countries JOIN countries ON ({col} = alpha_2)"
    country_list = pd.read_sql(sql, con=engine)
    return [{"label": row[1], "value": row[0]} for row in country_list.values]


target_countries = create_country_list("target_country")
source_countries = create_country_list("source_country")

schemas = ['', 'Address', 'Airplane', 'Associate', 'BankAccount', 'Company', 'CryptoWallet', 'Directorship',
           'Employment', 'Family', 'Identification', 'LegalEntity', 'Membership', 'Occupancy', 'Organization',
           'Ownership', 'Passport', 'Person', 'Position', 'Representation', 'Sanction', 'Security', 'UnknownLink',
           'Vessel']

app.layout = html.Div([
    dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Network Analysis Help")),
            dbc.ModalBody(dcc.Markdown('''
                Explanation of the various metrics.
                
                |  Centrality  |  Description  | Context |
                |---|---|---|
                | Degree Centrality | Number of edges connected to it | How many countries a particular country has sanctioned or has been sanctioned by |
                | In-Degree Centrality | Number of incoming edges | How many countries are imposing sanctions on a particular country | 
                | Out-Degree Centrality | Number of outgoing edges | How many countries a particular country is sanctioning |
                | Eigenvector Centrality | A node's importance based on the importance of its neighbors. considers quantity & quality of connections | TODO, Not trivial |
                | Closeness Centrality | How close a node is to all other nodes in the network | How quickly a country can be reached, either directly or indirectly, in terms of imposing or facing sanctions | 
                | Betweenness Centrality | How often a node lies on the shortest path between other nodes | High betweenness centrality indicates a crucial role in the flow of sanctions between other countries |
                | Clustering Centrality | The extent to which a node's neighbors are connected to each other | Indicate the tendency of groups of countries to collectively impose sanctions or be collectively sanctioned |
                | Pagerank | Assigns importance to a node based on the importance of nodes pointing to it | Country with high Pagerank would be one that is sanctioned by countries that are themselves considered important in the network |
            ''')),
            dbc.ModalFooter(dbc.Button("Close", id="btn-close-help-network", className="ms-auto", n_clicks=0)),
    ], id="modal-help-network", size="lg", is_open=False),

    dbc.Container([
        html.H1("OpenSanctions Dashboard"),
        dcc.Tabs(id="tabs-select", value='Sanctions by Country', children=[
            # TAB 1
            dcc.Tab(label='Sanctions by Country', value='Sanctions by Country', children=[
                html.Br(),

                dbc.Row([
                    dbc.Col(
                        dbc.Select(options=["Sanctions towards", "Sanctions from"], value="Sanctions towards",
                                   id='dd-sanction-mode'),
                        width=6, lg=2),
                    dbc.Col(
                        dcc.Dropdown(options=target_countries, id='dd-country', placeholder="Country"), width=6, lg=3),
                    dbc.Col(dbc.Select(options=schemas, id='dd-schemas', placeholder="Schema"), width=6, lg=2),
                    dbc.Col(dbc.Button("Search", color="light", className="me-1", n_clicks=0), width=6, lg=1),
                    dbc.Col(
                        dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-sbc", n_clicks=0),
                        width=6, lg=2)
                ]),

                dbc.Row(dcc.Graph(id="graph-sanctions-by-country")),

                dbc.Row([
                    dbc.Col([dcc.Graph(id="graph-sanctions-timeline")], lg=6),
                    dbc.Col([dcc.Graph(id="graph-types")], lg=6),
                ])
            ]),

            # TAB 2
            dcc.Tab(label='Individuals', value='Individuals', children=[
                html.Br(),
                dbc.Row([
                    dbc.Col(dbc.Input(id="input-search-caption", type="text", placeholder="Search", debounce=True),
                            width=12, lg=4),
                    dbc.Col(dbc.Select(options=schemas, id='dd-individ-schemas', placeholder="Schema"), width=6, lg=2),
                    dbc.Col(dbc.Button("Search", color="light", className="me-1", n_clicks=0), width=6, lg=1),
                    dbc.Col(dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-individ",
                                       n_clicks=0), width=6, lg=2)
                ]),
                html.Br(),
                dbc.Row([dash_table.DataTable(data=[], id='tbl-individ-results', style_cell={"whiteSpace": "pre-line"},
                                              fill_width=False)]),
            ]),

            # TAB 3
            dcc.Tab(label='Network Analysis', value='Network Analysis', children=[
                html.Br(),

                dbc.Row([
                    dbc.Col(dbc.Button("Load", color="light", id="btn-load-network", n_clicks=0), width=4, lg=1),
                    dbc.Col(dbc.Button("Help", color="success", className="me-1", id="btn-open-help-network", n_clicks=0),
                            width=4, lg=1),
                    dbc.Col(
                        dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-network",
                                   n_clicks=0), width=4, lg=2),
                ]),

                dcc.Graph(id="graph-network-analysis"),

                dash_table.DataTable(
                    data=[], id='tbl-network-analysis-statistics',
                    style_data={'whiteSpace': 'normal', 'height': 'auto', 'lineHeight': '15px'})
            ]),

            # TAB 4
            dcc.Tab(label='Industries', value='Industries', children=[]),

        ]),
        dcc.Download(id="download")
    ], fluid=True)
])


"""
@app.server.route('/static/<path:path>')
def static_file(path):
    static_folder = os.path.join(os.getcwd(), 'static')
    return send_from_directory(static_folder, path)
"""


@app.callback(
    Output("modal-help-network", "is_open"),
    [Input("btn-open-help-network", "n_clicks"), Input("btn-close-help-network", "n_clicks")],
    [State("modal-help-network", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open


@callback(
    Output("dd-country", "options"),
    [Input("dd-sanction-mode", "value")]
)
def update_graph(value):
    return target_countries if value == "Sanctions towards" else source_countries


@callback(
    [Output("graph-sanctions-by-country", "figure"),
     Output("graph-sanctions-timeline", "figure"),
     Output("graph-types", "figure")],
    [Input("dd-sanction-mode", "value"),
     Input("dd-country", "value"),
     Input("dd-schemas", "value")]
)
def update_graph(mode, country, schema):
    plt1 = px.bar(pd.DataFrame({"Country": [], "Amount": []}), x="Country", y="Amount")
    plt2 = px.bar(pd.DataFrame({"Date": [], "Amount": []}), x="Date", y="Amount")
    plt3 = px.bar(pd.DataFrame({"Date": [], "Amount": []}), x="Date", y="Amount")

    if mode is None or country is None:
        return plt1, plt2, plt3

    df = generate_country_data(engine, mode, country, schema)

    col = "source" if mode == "Sanctions towards" else "target"
    df1 = df.groupby(col)["id"].nunique().reset_index()
    plt1 = px.bar(df1, x=col, y="id", labels={"id": "Amount", col: "Country"}, title='Number of Entries by Country')

    df_melt = pd.melt(
        df.rename(columns={"first_seen": "First Seen", "last_seen": "Last Seen", "last_change": "Last Change"}),
        id_vars=['id'], value_vars=['First Seen', 'Last Seen', 'Last Change'], var_name='value', value_name='date')
    df_melt["date"] = pd.to_datetime(df_melt["date"])
    df_melt["date"] = df_melt["date"].dt.date
    df_melt = df_melt.groupby(["value", "date"])["id"].nunique().reset_index()

    plt2 = px.line(df_melt, x="date", y="id", color="value", labels={"id": "Amount", "date": "Date", "value": "Type"},
                   title='Number of Entries by Date')

    plt3 = px.bar(df["schema"].value_counts(), labels={"value": "Count", "schema": "Schema"},
                  title='Number of Entries by Schema')

    return plt1, plt2, plt3


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

    sql = f"""SELECT caption AS "Caption", datasets AS "Datasets", properties AS "Info",
    DATE(first_seen) AS "First Seen", DATE(last_seen) AS "Last Seen", DATE(last_change) AS "Last Change" 
    FROM entities WHERE LOWER(caption) LIKE concat('%%', LOWER(%(query)s) ,'%%')"""
    sql += "" if len(restriction) == 0 else " AND " + " AND ".join(restriction)

    df = pd.read_sql(sql, params={"schema": schema, "query": query}, con=engine)
    df["Datasets"] = df["Datasets"].apply(lambda x: "\n".join(x))
    df["Info"] = df["Info"].apply(lambda x: bytes(json.dumps(x), 'utf-8').decode('unicode_escape')[0:50] + "...")

    # df["referents"] = df["referents"].apply(str)
    # df["datasets"] = df["datasets"].apply(str)

    return df.to_dict("records")


@callback(
    [Output("graph-network-analysis", "figure"),
     Output("tbl-network-analysis-statistics", "data")],
    Input("btn-load-network", "n_clicks"),
    prevent_initial_call=True)
def network(_):
    sql = """SELECT s.description AS source, t.description AS target, count(DISTINCT id) AS weight 
    FROM entries_countries 
    JOIN countries s ON (s.alpha_2 = source_country) 
    JOIN countries t ON (t.alpha_2 = target_country)  
    GROUP BY 1,2"""

    df = pd.read_sql(sql, params={}, con=engine)
    graph = nx.from_pandas_edgelist(df, source="source", target="target", edge_attr=["weight"],
                                    create_using=nx.DiGraph())

    metrics = get_centralities(graph)

    return plot_network(graph), metrics.to_dict("records")


@callback(
    Output("download", "data"),
    [Input("btn-export-sbc", "n_clicks"),
     State("dd-sanction-mode", "value"),
     State("dd-country", "value"),
     State("dd-schemas", "value")],
    prevent_initial_call=True)
def download(_, mode, country, schema):
    def to_xlsx(bytes_io):
        xslx_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")  # requires the xlsxwriter package
        pd.DataFrame(generate_country_data(mode, country,
                                           schema)).to_excel(xslx_writer, index=False,
                                                             sheet_name="sheet1")
        xslx_writer.close()

    return dcc.send_bytes(to_xlsx, "test.xlsx")


if __name__ == '__main__':
    app.run(debug=True)
