import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import networkx as nx
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input, dash_table, State
from sqlalchemy import create_engine
from datetime import date

from tab_util.network import plot_network, get_centralities
from tab_util.sanctions_by_country import generate_country_data

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])


engine = create_engine("postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions")


def create_country_list(col=None):
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


target_countries = create_country_list("target_country")
source_countries = create_country_list("source_country")
all_countries = create_country_list()

with open("../data/schemas.txt", "r") as f:
    schemas = [{"label": v, "value": v} for v in f.read().split("\n")]


with open("../data/industries.txt", "r") as f:
    industries = [{"value": v, "label": " ".join([x.capitalize() for x in v.replace("/", " / ").split(" ")])} for v in f.read().split("\n")]

entity_search_header = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ["Title", "Country", "First Seen", "Last Seen", "Last Change", "Datasets"]]

network_metrics_header = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ['Country', 'Degree', 'In-Degree', 'Out-Degree', 'Eigenvector', 'Closeness', 'Betweenness', 'Clustering']]

tooltip_header = {
    'Country': 'Country',
    'Degree': 'Relative Number of edges connected to it (How many countries a particular country has sanctioned or has been sanctioned by)',
    'In-Degree': 'Relative Number of incoming edges (How many countries are imposing sanctions on a particular country)',
    'Out-Degree': "Relative Number of outgoing edges (How many countries a particular country is sanctioning)",
    'Eigenvector': "A node's importance based on the importance of its neighbors. considers quantity & quality of connections (TODO, Not trivial)",
    'Closeness': "How close a node is to all other nodes in the network (How quickly a country can be reached, either directly or indirectly, in terms of imposing or facing sanctions)",
    'Betweenness': "How often a node lies on the shortest path between other nodes (High betweenness centrality indicates a crucial role in the flow of sanctions between other countries)",
    'Clustering': "The extent to which a node's neighbors are connected to each other (Indicate the tendency of groups of countries to collectively impose sanctions or be collectively sanctioned)",
    'Pagerank': "Assigns importance to a node based on the importance of nodes pointing to it (Country with high Pagerank would be one that is sanctioned by countries that are themselves considered important in the network)"
}

app.layout = html.Div([

    dbc.Container([
        # html.H1("OpenSanctions Dashboard"),
        html.H4(""),
        dcc.Tabs(id="tabs-select", value='Sanctions by Country', children=[
            # TAB 1
            dcc.Tab(label='Sanctions by Country', value='Sanctions by Country', children=[
                html.H4("Filter"),
                dbc.Row([
                    dbc.Col(dmc.Select(id='dd-sanction-mode', data=["Sanctions towards", "Sanctions from"], value="Sanctions towards"), width=6, lg=2),
                    dbc.Col(dmc.Select(data=target_countries, id='dd-country', placeholder="Country", searchable=True), width=6, lg=3),
                    dbc.Col(dmc.DatePicker(id="dd-start-date", placeholder="Start Date", minDate=date(201, 5, 21)), width=6, lg=2),
                    dbc.Col(dmc.DatePicker(id="dd-end-date", placeholder="End Date", minDate=date(201, 5, 21)), width=6, lg=2),
                    dbc.Col(dbc.Button("Excel Export", color="primary", id="btn-export-sbc", n_clicks=0), width=12, lg=2)
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col(dmc.Select(data=schemas, id='dd-schemas', placeholder="Schema", searchable=True), width=6, lg=2),
                    dbc.Col(dmc.Select(data=industries, id='dd-industries', placeholder="Industries", searchable=True), width=6, lg=3),
                ]),

                html.Br(),
                html.H4("Number of Entities by Country"),
                html.P("How many sanctions (entities) have been imposed on this countries or are imposed by this country"),
                dbc.Row(dcc.Graph(id="graph-sanctions-by-country")),
                html.H4("Number of Entries by Date"),
                html.P("On which date was the first occurrence of a sanctioned entities (first_seen) undergo a change (last_change) or experience the last change (last_seen)"),
                dbc.Row(dcc.Graph(id="graph-sanctions-timeline")),

                dbc.Row([
                    dbc.Col(html.H4("Number of Entries by Schema"), lg=6),
                    dbc.Col(html.H4("Number of Entries by Industry"), lg=6),
                ]),

                dbc.Row([
                    dbc.Col(html.P("Which type of entities (in or from a specific country) experience the most sanctions towards them"), lg=6),
                    dbc.Col(html.P("Which company industries (in or from a specific country) experience the most sanctions towards them"), lg=6),
                ]),


                dbc.Row([
                    dbc.Col([dcc.Graph(id="graph-types")], lg=6),
                    dbc.Col([dcc.Graph(id="graph-sanctions-industry")], lg=6),
                ])
            ]),

            # TAB 2
            dcc.Tab(label='Entities', value='entities', children=[
                html.H4("Search"),
                dbc.Row([
                    dbc.Col(dmc.TextInput(id="input-search-caption", type="text", placeholder="Query", debounce=True), width=12, lg=4),
                    dbc.Col(dmc.Select(data=[""] + target_countries, id='dd-search-country', placeholder="Country", searchable=True), width=6, lg=3),
                    dbc.Col(dmc.Select(data=schemas, id='dd-individ-schemas', placeholder="Schema", searchable=True), width=6, lg=2),
                    dbc.Col(dbc.Button("Search", id='btn-search-entity', color="light", className="me-1", n_clicks=0), width=6, lg=1),
                    dbc.Col(dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-individ", n_clicks=0), width=6, lg=2)
                ]),
                html.Br(),
                html.H4("Result"),
                dbc.Row([dash_table.DataTable(
                    id='tbl-individ-results', columns=entity_search_header, data=[],
                    style_cell={"whiteSpace": "pre-line"}, sort_action="native", sort_mode='multi',
                    row_deletable=False, page_action='native', page_current=0, page_size=10
                )])
            ]),

            # TAB 3
            dcc.Tab(label='Network Analysis', value='Network Analysis', children=[
                html.H4("Filter"),
                dbc.Row([
                    dbc.Col(dmc.Select(data=schemas, id='dd-network-schemas', placeholder="Schema", searchable=True), width=6, lg=2),
                    dbc.Col(dmc.Select(data=industries, id='dd-network-industries', placeholder="Industries", searchable=True), width=6, lg=3),
                    dbc.Col(dmc.DatePicker(id="dd-network-start-date", placeholder="Start Date", minDate=date(201, 5, 21)), width=6, lg=2),
                    dbc.Col(dmc.DatePicker(id="dd-network-end-date", placeholder="End Date", minDate=date(201, 5, 21)), width=6, lg=2),
                    dbc.Col(dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-network", n_clicks=0), width=4, lg=2),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col(dmc.MultiSelect(id="dd-network-countries", searchable=True, placeholder="Countries", data=all_countries), width=12, lg=9),
                    dbc.Col(dbc.Button("Load", color="light", id="btn-load-network", n_clicks=0), width=4, lg=1),
                ]),
                html.Br(),
                html.H4("Who Sanctions Whom"),
                dcc.Graph(id="graph-network-analysis"),

                html.Br(),
                html.H4("Centrality Scores"),
                dash_table.DataTable(
                    id='tbl-network-analysis-statistics',
                    tooltip_header=tooltip_header, tooltip_delay=0, tooltip_duration=None,
                    columns=network_metrics_header, data=[],
                    style_cell={"whiteSpace": "pre-line"}, sort_action="native", sort_mode='multi',
                    row_deletable=False, page_action='native', page_current=0, page_size=10)
            ])
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


@callback(
    Output("dd-country", "options"),
    [Input("dd-sanction-mode", "value")]
)
def update_graph(value):
    return target_countries if value == "Sanctions towards" else source_countries


@callback(
    [Output("graph-sanctions-by-country", "figure"),
     Output("graph-sanctions-timeline", "figure"),
     Output("graph-types", "figure"),
     Output("graph-sanctions-industry", "figure")],
    [Input("dd-sanction-mode", "value"),
     Input("dd-country", "value"),
     Input("dd-schemas", "value"),
     Input("dd-industries", "value"),
     Input("dd-start-date", "value"),
     Input("dd-end-date", "value")]
)
def update_graph(mode, country, schema, industry, start_date, end_date):
    if mode is None or country is None:
        plt1 = px.bar(pd.DataFrame({"Country": [], "Amount": []}), x="Country", y="Amount")
        plt2 = px.bar(pd.DataFrame({"Date": [], "Amount": []}), x="Date", y="Amount")
        plt3 = px.bar(pd.DataFrame({"Schema": [], "Amount": []}), x="Schema", y="Amount")
        plt4 = px.bar(pd.DataFrame({"Industry": [], "Amount": []}), x="Industry", y="Amount")

        return plt1, plt2, plt3, plt4

    df = generate_country_data(engine, mode, country, schema, industry, start_date, end_date)

    col = "source" if mode == "Sanctions towards" else "target"
    df1 = df.groupby(col)["id"].nunique().reset_index().sort_values(by="id")
    plt1 = px.bar(df1, x=col, y="id", labels={"id": "Amount", col: "Country"})

    df_melt = pd.melt(
        df.rename(columns={"first_seen": "First Seen", "last_seen": "Last Seen", "last_change": "Last Change"}),
        id_vars=('id',), value_vars=['First Seen', 'Last Seen', 'Last Change'], var_name="value", value_name="date")

    df_melt["date"] = pd.to_datetime(df_melt["date"])
    df_melt["date"] = df_melt["date"].dt.date
    df_melt = df_melt.groupby(["value", "date"])[("id", )].nunique().reset_index()
    
    plt2 = px.line(df_melt, x="date", y="id", color="value", labels={"id": "Amount", "date": "Date", "value": "Type"})

    plt3 = px.bar(df["schema"].value_counts().sort_values(), labels={"value": "Count", "schema": "Schema"})

    plt4 = px.bar(df["industry"].value_counts().sort_values(), labels={"value": "Count", "schema": "Schema"})

    return plt1, plt2, plt3, plt4


@callback(
    Output("tbl-individ-results", "data"),
    [State("dd-individ-schemas", "value"),
     State("input-search-caption", "value"),
     State("dd-search-country", "value"),
     Input("btn-search-entity", "n_clicks")],
    prevent_initial_call=True)
def download(schema: str, query: str, country: str, _):
    if query is None or len(query.strip()) == 0:
        return []

    country_join = ""
    restriction = ["LOWER(caption) LIKE concat('%%', LOWER(%(query)s) ,'%%')"]

    if schema is not None and schema.strip() != "":
        restriction.append("schema = %(schema)s")

    if country is not None and country.strip() != "":
        country_join = "JOIN (SELECT id FROM entities_countries WHERE source_country = %(country)s) ec USING (id)"

    sql = f"""SELECT caption, country_descr, e.first_seen, e.last_seen, e.last_change, 
        STRING_AGG(CONCAT(d.title, CASE WHEN flag IS NULL THEN '' ELSE CONCAT(' (', flag, ')') END), '\n') AS datasets
        FROM (
            SELECT id, caption, first_seen, last_seen, last_change, json_array_elements_text(datasets) AS name
            FROM entities 
            {country_join}
            WHERE {" AND ".join(restriction)}
        ) e
        LEFT JOIN (SELECT id, target_country FROM entities_countries) ec USING (id)
        LEFT JOIN (SELECT alpha_2 AS target_country, description AS country_descr FROM countries) c USING (target_country)
        JOIN datasets d USING (name)
        LEFT JOIN (SELECT alpha_2, flag FROM countries) c2 ON (d.publisher->>'country' = c2.alpha_2)
        GROUP BY 1,2,3,4,5"""

    df = pd.read_sql(sql, params={"schema": schema, "query": query, "country": country}, con=engine)

    df['first_seen'] = pd.to_datetime(df['first_seen']).dt.date
    df['last_seen'] = pd.to_datetime(df['last_seen']).dt.date
    df['last_change'] = pd.to_datetime(df['last_change']).dt.date

    df.rename(columns={"caption": "Title", "country_descr": "Country", "datasets": "Datasets",
                       "first_seen": "First Seen", "last_seen": "Last Seen", "last_change": "Last Change"},
              inplace=True)

    return df.to_dict("records")


@callback(
    [Output("graph-network-analysis", "figure"),
     Output("tbl-network-analysis-statistics", "data")],
    [Input("btn-load-network", "n_clicks"),
     State("dd-network-schemas", "value"),
     State("dd-network-industries", "value"),
     State("dd-network-start-date", "value"),
     State("dd-network-end-date", "value"),
     State("dd-network-countries", "value")],
    prevent_initial_call=True)
def network(_, schema, industry, start_date, end_date, countries):
    conditions = ["source_country != target_country"]

    if schema is not None and schema != "":
        conditions.append('schema = %(s)s')

    if industry is not None and industry != "":
        conditions.append('industry = %(i)s')

    if start_date is not None and start_date != "":
        conditions.append('first_seen > %(sd)s')

    if end_date is not None and end_date != "":
        conditions.append('first_seen < %(ed)s')

    if countries is not None and countries != "":
        countries = ", ".join(map(lambda x: f"'{x}'", countries))
        conditions.append(f'source_country IN ({countries}) AND target_country IN ({countries})')

    condition = ' AND '.join(conditions)

    sql = f"""SELECT s.description AS source, t.description AS target, count(DISTINCT id) AS weight 
    FROM entities_countries 
    JOIN countries s ON (s.alpha_2 = source_country) 
    JOIN countries t ON (t.alpha_2 = target_country)  
    WHERE {condition}
    GROUP BY 1, 2"""

    df = pd.read_sql(sql, params={"s": schema, "i": industry, "sd": start_date, "ed": end_date}, con=engine)

    if len(df) == 0:
        fig = px.scatter(title='No Data')
        fig.update_layout(annotations=[
            dict(x=0.5, y=0.5, xref="paper", yref="paper", text="No data", showarrow=False, font=dict(size=20), ) ] )
        return fig, []

    graph = nx.from_pandas_edgelist(
        df, source="source", target="target", edge_attr=["weight"], create_using=nx.DiGraph())

    if graph.number_of_nodes() == 0:
        return px.bar(pd.DataFrame({"Date": [], "Amount": []}), x="Date", y="Amount")

    metrics = get_centralities(graph)

    return plot_network(graph), metrics.to_dict("records")


@callback(
    Output("download", "data"),
    [Input("btn-export-sbc", "n_clicks"),
     State("dd-sanction-mode", "value"),
     State("dd-country", "value"),
     State("dd-schemas", "value"),
     State("dd-industries", "value")],
    prevent_initial_call=True)
def download(_, mode, country, schema, industry):
    def to_xlsx(bytes_io):
        xslx_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")
        df = generate_country_data(engine, mode, country, schema, industry, None, None) # TODO
        pd.DataFrame(df).to_excel(xslx_writer, index=False, sheet_name="sheet1")
        xslx_writer.close()

    return dcc.send_bytes(to_xlsx, "test.xlsx")


if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=3000)
