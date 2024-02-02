import numpy as np
import plotly.graph_objs as go
import networkx as nx
import pandas as pd


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

            weight_norm = (weight - min_weights) / (max_weights - min_weights)
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
            "size": 15, "colorbar": {"thickness": 10, "title": 'Number of sanctioned entities', "xanchor": 'left', "titleside": 'right'},
            "line": {"width": 0}
        })

    ticks = {"showgrid": True, "zeroline": True, "showticklabels": False}

    fig = go.Figure(data=annotations + [node_trace], layout=go.Layout(#annotations=annotations,
                         titlefont={"size": 16}, showlegend=False, hovermode='closest',
                         margin={"b": 20, "l": 5, "r": 5, "t": 40}, xaxis=ticks, yaxis=ticks))
    #fig.add_trace(node_trace)
    #fig.update_layout(annotations=annotations)
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

    try:
        metrics["Eigenvector"] = nx.eigenvector_centrality(graph, weight="weight", max_iter=1000)
    except nx.exception.PowerIterationFailedConvergence as ignored:
        pass

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
