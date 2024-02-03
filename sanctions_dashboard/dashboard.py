import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import Dash, html, dcc, callback, Output, Input, dash_table, State
from sqlalchemy import create_engine
from datetime import date

from tab_util.network import build_output, build_edge_list
from tab_util.sanctions_by_country import generate_country_data, create_graphs
from tab_util.entity_search import search_entity
from tab_util.util import df_to_excel, create_country_list

###################################################################
# Definitions
###################################################################

engine = create_engine("postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions")

target_countries = create_country_list(engine, "target_country")
source_countries = create_country_list(engine, "source_country")
all_countries = create_country_list(engine)

with open("../data/schemas.txt", "r") as f:
    schemas = [{"label": v, "value": v} for v in f.read().split("\n")]


with open("../data/industries.txt", "r") as f:
    industries = [{"value": v, "label": " ".join([x.capitalize() for x in v.replace("/", " / ").split(" ")])} for v in f.read().split("\n")]

entity_search_header = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ["Title", "Country", "First Seen", "Last Seen", "Last Change", "Datasets"]
]

network_metrics_header = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ['Country', 'Degree', 'In-Degree', 'Out-Degree', 'Closeness', 'Betweenness', 'Clustering']
]

tooltip_header = {
    'Country': 'Country',
    'Degree': 'Relative Number of edges connected to it (How many countries a particular country has sanctioned or has been sanctioned by)',
    'In-Degree': 'Relative Number of incoming edges (How many countries are imposing sanctions on a particular country)',
    'Out-Degree': "Relative Number of outgoing edges (How many countries a particular country is sanctioning)",
    'Closeness': "How close a node is to all other nodes in the network (How quickly a country can be reached, either directly or indirectly, in terms of imposing or facing sanctions)",
    'Betweenness': "How often a node lies on the shortest path between other nodes (High betweenness centrality indicates a crucial role in the flow of sanctions between other countries)",
    'Clustering': "The extent to which a node's neighbors are connected to each other (Indicate the tendency of groups of countries to collectively impose sanctions or be collectively sanctioned)",
    'Pagerank': "Assigns importance to a node based on the importance of nodes pointing to it (Country with high Pagerank would be one that is sanctioned by countries that are themselves considered important in the network)"
}

tbl_inter_company_header = [
    {'name': "ID", 'id': "id", 'deletable': True},
    {'name': "Entity Name", 'id': "caption", 'deletable': True},
    {'name': "First Seen", 'id': "first_seen", 'deletable': True},
    {'name': "Type", 'id': "schema", 'deletable': True},
    {'name': "Industry", 'id': "industry", 'deletable': True},
    {'name': "Target Country", 'id': "target", 'deletable': True},
    {'name': "Source Country", 'id': "source", 'deletable': True}
]

###################################################################
# Dash Layout
###################################################################

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

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
                html.P("On which date the entity occurred on one of the sanctions datasets"),
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
                ]),

                html.Br(),
                html.H4("Results"),
                dbc.Row([dash_table.DataTable(
                    id='tbl-inter-company-results', data=[], columns=tbl_inter_company_header,
                    style_cell={"whiteSpace": "pre-line"}, sort_action="native", sort_mode='multi',
                    row_deletable=False, page_action='native', page_current=0, page_size=10
                )])
            ]),

            # TAB 2
            dcc.Tab(label='Entities', value='entities', children=[
                html.H4("Search"),
                dbc.Row([
                    dbc.Col(dmc.TextInput(id="input-search-caption", type="text", placeholder="Query", debounce=True), width=12, lg=4),
                    dbc.Col(dmc.Select(data=[""] + target_countries, id='dd-search-country', placeholder="Country", searchable=True), width=6, lg=3),
                    dbc.Col(dmc.Select(data=schemas, id='dd-individ-schemas', placeholder="Schema", searchable=True), width=6, lg=2),
                    dbc.Col(dbc.Button("Search", id='btn-search-entity', color="light", className="me-1", n_clicks=0), width=6, lg=1),
                    dbc.Col(dbc.Button("Excel Export", color="primary", className="me-1", id="btn-export-search-entity", n_clicks=0), width=6, lg=2)
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
        ])
    ], fluid=True),

    html.Hr(),

    dbc.Container([

        html.Footer([
            html.H3("Sanctions Dashboard"),
            html.H5("Disclaimer"),
            html.P([
                "This dashboard was created by TU Vienna student ",
                html.A("Nicolas Bschor", href="https://github.com/HackerBschor"),
                " in collaboration with WU Vienna Professor ",
                html.A("Dr. Jakob Müllner", href="https://www.wu.ac.at/iib/iib/faculty/muellner/"),
                " during the 'Interdisciplinary Project in Data Science' course.",
                html.Br(),
                "The data used in this dashboard is sourced from ",
                html.A("OpenSanctions", href="https://www.opensanctions.org/"),
                " (Sanctions Information) and ",
                html.A("People Data Labs", href="https://www.peopledatalabs.com/"),
                """ (Company Industries). We only applied various transformation techniques in order to allow the analysis
                without changing the information. Therefore, we cannot ensure completeness, correctness, 
                or if it is up-to-date.
                """,
                html.Br(),
                """
                Users are advised to verify information independently, 
                and the developers assume no responsibility for the consequences of its use. 
                Use the dashboard at your own risk."""
            ])
        ])
    ], fluid=True),

    dcc.Download(id="download"),
    dcc.Download(id="download-entity-search"),
    dcc.Download(id="download-graph"),
])


###################################################################
# TAB: Sanctions by Country
###################################################################
@callback(Output("dd-country", "options"), Input("dd-sanction-mode", "value"))
def update_graph(value):
    return target_countries if value == "Sanctions towards" else source_countries


@callback(
    [Output("graph-sanctions-by-country", "figure"),
     Output("graph-sanctions-timeline", "figure"),
     Output("graph-types", "figure"),
     Output("graph-sanctions-industry", "figure"),
     Output("tbl-inter-company-results", "data")],
    [Input("dd-sanction-mode", "value"),
     Input("dd-country", "value"),
     Input("dd-schemas", "value"),
     Input("dd-industries", "value"),
     Input("dd-start-date", "value"),
     Input("dd-end-date", "value")]
)
def update_graph(mode, country, schema, industry, start_date, end_date):
    return create_graphs(mode, country, schema, industry, start_date, end_date, engine)


@callback(
    Output("download", "data"),
    [Input("btn-export-sbc", "n_clicks"),
     State("dd-sanction-mode", "value"),
     State("dd-country", "value"),
     State("dd-schemas", "value"),
     State("dd-industries", "value"),
     State("dd-start-date", "value"),
     State("dd-end-date", "value")],
    prevent_initial_call=True)
def download(_, mode, country, schema, industry, start_date, end_date):
    df = generate_country_data(mode, country, schema, industry, start_date, end_date, engine)
    to_xlsx = df_to_excel(df, "Sanctions by Country")
    return dcc.send_bytes(to_xlsx, f"{mode}_{country}.xlsx".replace(' ', '_').lower())


###################################################################
# TAB: Entities
###################################################################
@callback(
    Output("tbl-individ-results", "data"),
    [State("dd-individ-schemas", "value"),
     State("input-search-caption", "value"),
     State("dd-search-country", "value"),
     Input("btn-search-entity", "n_clicks")],
    prevent_initial_call=True)
def update_table_individ_results(schema: str, query: str, country: str, _):
    return search_entity(schema, query, country, engine).to_dict("records")


@callback(
    Output("download-entity-search", "data"),
    [State("dd-individ-schemas", "value"),
     State("input-search-caption", "value"),
     State("dd-search-country", "value"),
     Input("btn-export-search-entity", "n_clicks")],
    prevent_initial_call=True)
def export_search(schema: str, query: str, country: str, _):
    df = search_entity(schema, query, country, engine)
    to_xlsx = df_to_excel(df, "Entities")
    return dcc.send_bytes(to_xlsx, f"entities.xlsx")


###################################################################
# TAB: Network Analysis
###################################################################
@callback(
    [Output("graph-network-analysis", "figure"),
     Output("tbl-network-analysis-statistics", "data")],
    [State("dd-network-schemas", "value"),
     State("dd-network-industries", "value"),
     State("dd-network-start-date", "value"),
     State("dd-network-end-date", "value"),
     State("dd-network-countries", "value"),
     Input("btn-load-network", "n_clicks")],
    prevent_initial_call=True)
def network(schema, industry, start_date, end_date, countries, _):
    return build_output(schema, industry, start_date, end_date, countries, engine)


@callback(
    Output("download-graph", "data"),
    [State("dd-network-schemas", "value"),
     State("dd-network-industries", "value"),
     State("dd-network-start-date", "value"),
     State("dd-network-end-date", "value"),
     State("dd-network-countries", "value"),
     Input("btn-export-network", "n_clicks")],
    prevent_initial_call=True)
def download_graph(schema, industry, start_date, end_date, countries, _):
    df = build_edge_list(schema, industry, start_date, end_date, countries, engine)
    to_xlsx = df_to_excel(df, "Network")
    return dcc.send_bytes(to_xlsx, f"network.xlsx")


if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=3000)
