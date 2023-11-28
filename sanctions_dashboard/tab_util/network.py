import numpy as np
import plotly.graph_objs as go
import networkx as nx
import pandas as pd


def plot_network(graph: nx.Graph):
    pos = nx.spring_layout(graph, k=10, iterations=100)

    for n, p in pos.items():
        graph.nodes[n]['pos'] = p

    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    for edge in graph.edges():
        x0, y0 = graph.nodes[edge[0]]['pos']
        x1, y1 = graph.nodes[edge[1]]['pos']
        edge_trace['x'] += tuple([x0, x1, None])
        edge_trace['y'] += tuple([y0, y1, None])

    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='RdBu',
            reversescale=True,
            color=[],
            size=15,
            colorbar=dict(
                thickness=10,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=0)))

    for node in graph.nodes():
        x, y = graph.nodes[node]['pos']
        node_trace['x'] += tuple([x])
        node_trace['y'] += tuple([y])

    for node, adjacencies in enumerate(graph.adjacency()):
        node_trace['marker']['color'] += tuple([len(adjacencies[1])])
        node_info = adjacencies[0] + ' # of connections: ' + str(len(adjacencies[1]))
        node_trace['text'] += tuple([node_info])

    return go.Figure(data=[edge_trace, node_trace],
                     layout=go.Layout(
                         titlefont=dict(size=16),
                         showlegend=False,
                         hovermode='closest',
                         margin=dict(b=20, l=5, r=5, t=40),
                         annotations=[dict(
                             text="No. of connections",
                             showarrow=False,
                             xref="paper", yref="paper")],
                         xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                         yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))


def get_centralities(graph: nx.Graph):
    metrics = {
        "Degree": nx.degree_centrality(graph),
        "In-Degree": nx.in_degree_centrality(graph),
        "Out-Degree": nx.out_degree_centrality(graph),
        "Eigenvector": nx.eigenvector_centrality(graph),
        "Closeness": nx.closeness_centrality(graph),
        "Betweenness": nx.betweenness_centrality(graph),
        "Clustering": nx.clustering(graph),
        "Pagerank": nx.pagerank(graph)
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
