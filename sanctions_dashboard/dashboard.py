import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import Dash, html, dcc, callback, Output, Input, dash_table, State
from sqlalchemy import create_engine, Engine
from datetime import date

from tab_util.network import build_output, build_edge_list
from tab_util.sanctions_by_country import generate_country_data, create_graphs
from tab_util.entity_search import search_entity
from tab_util.util import df_to_excel, create_country_list

###################################################################
# Definitions
###################################################################

engine: Engine = create_engine("postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions")

target_countries = create_country_list(engine, "target_country")
source_countries = create_country_list(engine, "source_country")
all_countries = create_country_list(engine)

with open("../data/schemas.txt", "r") as f:
    schemas = [{"label": v, "value": v} for v in f.read().split("\n")]


with open("../data/industries.txt", "r") as f:
    industries = [{"value": v, "label": " ".join([x.capitalize() for x in v.replace("/", " / ").split(" ")])} for v in
                  f.read().split("\n")]

entity_search_header: list = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ["Title", "Country", "First Seen", "Last Seen", "Last Change", "Datasets"]
]

network_metrics_header: list = [
    {'name': i, 'id': i, 'deletable': True} for i in
    ['Country', 'Degree', 'In-Degree', 'Out-Degree', 'Closeness', 'Betweenness', 'Clustering']
]

tooltip_header: dict = {
    'Country': 'Country',
    'Degree': 'Relative Number of edges connected to it (How many countries a particular country has sanctioned or has been sanctioned by)',
    'In-Degree': 'Relative Number of incoming edges (How many countries are imposing sanctions on a particular country)',
    'Out-Degree': "Relative Number of outgoing edges (How many countries a particular country is sanctioning)",
    'Closeness': "How close a node is to all other nodes in the network (How quickly a country can be reached, either directly or indirectly, in terms of imposing or facing sanctions)",
    'Betweenness': "How often a node lies on the shortest path between other nodes (High betweenness centrality indicates a crucial role in the flow of sanctions between other countries)",
    'Clustering': "The extent to which a node's neighbors are connected to each other (Indicate the tendency of groups of countries to collectively impose sanctions or be collectively sanctioned)",
    'Pagerank': "Assigns importance to a node based on the importance of nodes pointing to it (Country with high Pagerank would be one that is sanctioned by countries that are themselves considered important in the network)"
}

tbl_inter_company_header: list = [
    {'name': "ID", 'id': "id", 'deletable': True},
    {'name': "Entity Name", 'id': "caption", 'deletable': True},
    {'name': "First Seen", 'id': "first_seen", 'deletable': True},
    {'name': "Type", 'id': "schema", 'deletable': True},
    {'name': "Industry", 'id': "industry", 'deletable': True},
    {'name': "Target Country", 'id': "target", 'deletable': True},
    {'name': "Source Country", 'id': "source", 'deletable': True}
]


def create_app():
    ###################################################################
    # Dash Layout
    ###################################################################

    app: Dash = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

    app.layout = html.Div([
        dbc.Container([
            # html.H1("OpenSanctions Dashboard"),
            html.H4(""),
            dcc.Tabs(value='Sanctions by Country', children=[
                # TAB 1
                dcc.Tab(label='Sanctions by Country', value='Sanctions by Country', children=[
                    html.H4("Filter"),
                    dbc.Row([
                        dbc.Col(dmc.Select(id='sbc-filter-mode', data=["Sanctions towards", "Sanctions from"], value="Sanctions towards"), width=6, lg=2),
                        dbc.Col(dmc.Select(id='sbc-filter-country', data=target_countries, placeholder="Country", searchable=True), width=6, lg=3),
                        dbc.Col(dmc.DatePicker(id="sbc-filter-start-date", placeholder="Start Date", minDate=date(201, 5, 21)), width=6, lg=2),
                        dbc.Col(dmc.DatePicker(id="sbc-filter-end-date", placeholder="End Date", minDate=date(201, 5, 21)), width=6, lg=2),
                        dbc.Col(dbc.Button(id="sbc-btn-export", children="Excel Export", color="primary", n_clicks=0), width=12, lg=2)
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col(dmc.Select(id='sbc-filter-schema', data=schemas, placeholder="Schema", searchable=True), width=6, lg=2),
                        dbc.Col(dmc.Select(id='sbc-filter-industries', data=industries, placeholder="Industries", searchable=True), width=6, lg=3),
                    ]),

                    html.Br(),
                    html.H4("Number of Entities by Country"),
                    html.P("How many sanctions (entities) have been imposed on this countries or are imposed by this country"),
                    dbc.Row(dcc.Graph(id="sbc-graph-sanctions-by-country")),
                    html.H4("Number of Entries by Date"),
                    html.P("On which date the entity occurred on one of the sanctions datasets"),
                    dbc.Row(dcc.Graph(id="sbc-graph-sanctions-timeline")),

                    dbc.Row([
                        dbc.Col(html.H4("Number of Entries by Schema"), lg=6),
                        dbc.Col(html.H4("Number of Entries by Industry"), lg=6),
                    ]),

                    dbc.Row([
                        dbc.Col(html.P("Which type of entities (in or from a specific country) experience the most sanctions towards them"), lg=6),
                        dbc.Col(html.P("Which company industries (in or from a specific country) experience the most sanctions towards them"), lg=6),
                    ]),

                    dbc.Row([
                        dbc.Col([dcc.Graph(id="sbc-graph-sanctions-schemas")], lg=6),
                        dbc.Col([dcc.Graph(id="sbc-graph-sanctions-industry")], lg=6),
                    ]),

                    html.Br(),
                    html.H4("Results"),
                    dbc.Row([dash_table.DataTable(
                        id='sbc-tbl-results', data=[], columns=tbl_inter_company_header,
                        style_cell={"whiteSpace": "pre-line"}, sort_action="native", sort_mode='multi',
                        row_deletable=False, page_action='native', page_current=0, page_size=10
                    )])
                ]),

                # TAB 2
                dcc.Tab(label='Entities', value='entities', children=[
                    html.H4("Search"),
                    dbc.Row([
                        dbc.Col(dmc.TextInput(id="entities-filter-query", type="text", placeholder="Query", debounce=True), width=12, lg=4),
                        dbc.Col(dmc.Select(id='entities-filter-country', data=[""] + target_countries, placeholder="Country", searchable=True), width=6, lg=3),
                        dbc.Col(dmc.Select(id='entities-filter-schema', data=schemas, placeholder="Schema", searchable=True), width=6, lg=2),
                        dbc.Col(dbc.Button(id='entities-btn-search', children="Search", color="light", className="me-1", n_clicks=0), width=6, lg=1),
                        dbc.Col(dbc.Button(id='entities-btn-export', children="Excel Export", color="primary", className="me-1", n_clicks=0), width=6, lg=2)
                    ]),
                    html.Br(),
                    html.H4("Result"),
                    dbc.Row([dash_table.DataTable(
                        id='entities-tbl-results', columns=entity_search_header, data=[],
                        style_cell={"whiteSpace": "pre-line"}, sort_action="native", sort_mode='multi',
                        row_deletable=False, page_action='native', page_current=0, page_size=10
                    )])
                ]),

                # TAB 3
                dcc.Tab(label='Network Analysis', value='Network Analysis', children=[
                    html.H4("Filter"),
                    dbc.Row([
                        dbc.Col(dmc.Select(id='network-filter-schema', data=schemas, placeholder="Schema", searchable=True), width=6, lg=2),
                        dbc.Col(dmc.Select(id='network-filter-industry', data=industries, placeholder="Industries", searchable=True), width=6, lg=3),
                        dbc.Col(dmc.DatePicker(id="network-filter-start-date", placeholder="Start Date", minDate=date(201, 5, 21)), width=6, lg=2),
                        dbc.Col(dmc.DatePicker(id="network-filter-end-date", placeholder="End Date", minDate=date(201, 5, 21)), width=6, lg=2),
                        dbc.Col(dbc.Button(id="network-btn-export", children="Excel Export", color="primary", className="me-1", n_clicks=0), width=4, lg=2),
                    ]),
                    html.Br(),
                    dbc.Row([
                        dbc.Col(dmc.MultiSelect(id="network-filter-countries", searchable=True, placeholder="Countries", data=all_countries), width=12, lg=9),
                        dbc.Col(dbc.Button(id="network-btn-load", children="Load", color="light", n_clicks=0), width=4, lg=1),
                    ]),
                    html.Br(),
                    html.H4("Who Sanctions Whom"),
                    dcc.Graph(id="network-graph"),

                    html.Br(),
                    html.H4("Centrality Scores"),
                    dash_table.DataTable(
                        id='network-tbl-centralises',
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
                html.H5(["Find the Project on ", html.A("GitHub", href="https://github.com/HackerBschor/SanctionsDashboard")]),
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
                    or if the data is up-to-date.
                    """,
                    html.Br(),
                    """
                    Users are advised to verify information independently, 
                    and the developers assume no responsibility for the consequences of its use. 
                    Use the dashboard at your own risk."""
                ])
            ])
        ], fluid=True),

        dcc.Download(id="sbc-download"),
        dcc.Download(id="entities-download"),
        dcc.Download(id="network-download"),
    ])

    return app


###################################################################
# TAB: Sanctions by Country
###################################################################
@callback(Output("sbc-filter-country", "options"), Input("sbc-filter-mode", "value"))
def update_sbc_filter_country(value: str):
    return target_countries if value == "Sanctions towards" else source_countries


@callback(
    [Output("sbc-graph-sanctions-by-country", "figure"),
     Output("sbc-graph-sanctions-timeline", "figure"),
     Output("sbc-graph-sanctions-schemas", "figure"),
     Output("sbc-graph-sanctions-industry", "figure"),
     Output("sbc-tbl-results", "data")],
    [Input("sbc-filter-mode", "value"),
     Input("sbc-filter-country", "value"),
     Input("sbc-filter-schema", "value"),
     Input("sbc-filter-industries", "value"),
     Input("sbc-filter-start-date", "value"),
     Input("sbc-filter-end-date", "value")]
)
def update_sbc_graphs_table(mode: str, country: str, schema: str, industry: str, start_date: str, end_date: str):
    return create_graphs(mode, country, schema, industry, start_date, end_date, engine)


@callback(
    Output("sbc-download", "data"),
    [Input("sbc-btn-export", "n_clicks"),
     State("sbc-filter-mode", "value"),
     State("sbc-filter-country", "value"),
     State("sbc-filter-schema", "value"),
     State("sbc-filter-industries", "value"),
     State("sbc-filter-start-date", "value"),
     State("sbc-filter-end-date", "value")],
    prevent_initial_call=True)
def sbc_download(_, mode: str, country: str, schema: str, industry: str, start_date: str, end_date: str):
    df = generate_country_data(mode, country, schema, industry, start_date, end_date, engine)
    to_xlsx = df_to_excel(df, "Sanctions by Country")
    filename = f"{mode}_{country}.xlsx".replace(' ', '_').lower()
    return dcc.send_bytes(to_xlsx, filename)


###################################################################
# TAB: Entities
###################################################################
@callback(
    Output("entities-tbl-results", "data"),
    [State("entities-filter-schema", "value"),
     State("entities-filter-query", "value"),
     State("entities-filter-country", "value"),
     Input("entities-btn-search", "n_clicks")],
    prevent_initial_call=True)
def update_tbl_results(schema: str, query: str, country: str, _):
    return search_entity(schema, query, country, engine).to_dict("records")


@callback(
    Output("entities-download", "data"),
    [State("entities-filter-schema", "value"),
     State("entities-filter-query", "value"),
     State("entities-filter-country", "value"),
     Input("entities-btn-export", "n_clicks")],
    prevent_initial_call=True)
def entities_download(schema: str, query: str, country: str, _):
    df = search_entity(schema, query, country, engine)
    to_xlsx = df_to_excel(df, "Entities")
    return dcc.send_bytes(to_xlsx, f"entities.xlsx")


###################################################################
# TAB: Network Analysis
###################################################################
@callback(
    [Output("network-graph", "figure"),
     Output("network-tbl-centralises", "data")],
    [State("network-filter-schema", "value"),
     State("network-filter-industry", "value"),
     State("network-filter-start-date", "value"),
     State("network-filter-end-date", "value"),
     State("network-filter-countries", "value"),
     Input("network-btn-load", "n_clicks")],
    prevent_initial_call=True)
def update_network_graph_table(schema: str, industry: str, start_date: str, end_date: str, countries: str, _):
    return build_output(schema, industry, start_date, end_date, countries, engine)


@callback(
    Output("network-download", "data"),
    [State("network-filter-schema", "value"),
     State("network-filter-industry", "value"),
     State("network-filter-start-date", "value"),
     State("network-filter-end-date", "value"),
     State("network-filter-countries", "value"),
     Input("network-btn-export", "n_clicks")],
    prevent_initial_call=True)
def download_network(schema: str, industry: str, start_date: str, end_date: str, countries: str, _):
    df = build_edge_list(schema, industry, start_date, end_date, countries, engine)
    to_xlsx = df_to_excel(df, "Network")
    return dcc.send_bytes(to_xlsx, f"network.xlsx")


if __name__ == '__main__':
    app = create_app()
    app.run(debug=False, host="0.0.0.0", port=3000)
