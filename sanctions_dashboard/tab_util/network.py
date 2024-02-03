import numpy as np
import plotly.graph_objs as go
import networkx as nx
import plotly.express as px
import pandas as pd


def build_edge_list(schema, industry, start_date, end_date, countries, engine):
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

    return pd.read_sql(sql, params={"s": schema, "i": industry, "sd": start_date, "ed": end_date}, con=engine)


def build_graph(df):
    graph = nx.DiGraph()
    graph = nx.from_pandas_edgelist(df, source="source", target="target", edge_attr=["weight"], create_using=graph)
    return graph


def build_output(schema, industry, start_date, end_date, countries, engine):
    df = build_edge_list(schema, industry, start_date, end_date, countries, engine)
    graph = build_graph(df)

    if len(df) == 0 or graph.number_of_nodes() == 0:
        fig = px.scatter(title='No Data')
        fig.update_layout(annotations=[
            dict(x=0.5, y=0.5, xref="paper", yref="paper", text="No data", showarrow=False, font=dict(size=20), )])
        return fig, []

    print(graph)

    return plot_network(graph), get_centralities(graph).to_dict("records")


def plot_network(graph: nx.Graph):
    pos = nx.kamada_kawai_layout(graph, weight='weight')

    weights = [d["weight"] for (_, _, d) in graph.edges(data=True)]
    min_weights = min(weights)
    max_weights = max(weights)

    nodes_x, nodes_y, nodes_color, nodes_text = [], [], [], []
    annotations = []

    for node, adj in graph.adjacency():
        nodes_x.append(pos[node][0])
        nodes_y.append(pos[node][1])

        weights = 0
        scs = []
        for edge in adj:
            weight = adj[edge]["weight"]
            weights += weight

            try:
                weight_norm = (weight - min_weights) / (max_weights - min_weights)
            except ZeroDivisionError as _:
                weight_norm = 0

            annotations.append(go.Scatter(x=[pos[node][0], pos[edge][0]], y=[pos[node][1], pos[edge][1]],
                                          hoverinfo='skip',
                                          marker={"size": 5+(10*weight_norm),
                                                  "symbol": "arrow-bar-up", "angleref": "previous",
                                                  "color": "rgba(0,0,0,.5)"}))

            scs.append(edge)

        nodes_color.append(weights)
        nodes_text.append(f"{node} is sanctioning {weights} entities ({len(adj)} Countries)")

    node_trace = go.Scatter(x=nodes_x, y=nodes_y, text=nodes_text, mode='markers', hoverinfo='text', opacity=0.5,
                            marker={
                                "showscale": True, "colorscale": 'RdBu', "reversescale": True, "color": nodes_color,
                                "size": 15, "colorbar": {"thickness": 10, "title": 'Number of sanctioned entities',
                                                         "xanchor": 'left', "titleside": 'right'}, "line": {"width": 0}
                            })

    ticks = {"showgrid": True, "zeroline": True, "showticklabels": False}

    fig = go.Figure(data=annotations + [node_trace], layout=go.Layout(
                         titlefont={"size": 16}, showlegend=False, hovermode='closest',
                         margin={"b": 20, "l": 5, "r": 5, "t": 40}, xaxis=ticks, yaxis=ticks))
    return fig


def get_centralities(graph: nx.Graph):
    metrics = {
        "Degree": nx.degree_centrality(graph),
        "In-Degree": nx.in_degree_centrality(graph),
        "Out-Degree": nx.out_degree_centrality(graph),
        "Closeness": nx.closeness_centrality(graph),
        "Betweenness": nx.betweenness_centrality(graph, weight="weight"),
        "Clustering": nx.clustering(graph, weight="weight"),
        "Pagerank": nx.pagerank(graph, weight="weight")
    }

    names = list(metrics.keys())

    data = []

    for key in sorted(graph.nodes, reverse=True):
        row = [key]
        for metric in names:
            if key in metrics[metric]:
                row.append(np.array(metrics[metric][key]).round(2))
            else:
                row.append(None)

        data.append(row)

    return pd.DataFrame(data, columns=["Country"] + names)
