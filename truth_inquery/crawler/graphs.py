import pandas as pd
import networkx as nx
import os
import sys
import random
import matplotlib.pyplot as plt
from crawler import CPCIN, CPCOUT, HPCIN, HPCOUT, STATES

random.seed(1234)
OPTIONAL = {
    'CPC':{'node_color': 'red',
           'alpha': 0.7,
           'node_size': 300,
           'width': 1},
    'HPC':{'node_color': 'blue',
           'alpha': 0.7,
           'node_size': 300,
           'width': 1}
    }

TOKEN_IGNORE = (
    "var",
    "label",
    "format",
    "instanceof",
    "blockquote",
    "solid",
    "text",
    "val",
    "column",
    "settings",
    "function",
    "return"
)

def top_clinic_tokens(df, col, top_num):
    """
    Cleans token-count pandas dataframe for single URL (clinic)

    Inputs
        - df: (pd dataframe) dataframe as token-count columns
        - col (string): column with token counts of URL
        - top_num (int): number of top tokens to return

    Returns standardized dataframe of nlargest tokens by count 
    """
    # filter and sort
    node = df[~df['token'].str.contains('|'.join(TOKEN_IGNORE), na = False)]
    node = node[['token', col]].sort_values(by=[col], ascending=False)
    node = node.rename(columns = {col: 'Count'})

    return node.nlargest(top_num, 'Count').to_dict('records')

def initialize_graph(state, clinic):
    """
    Load data and initalize a networkx graph using 
    state abbrevation input as base node.

    Inputs
        - state: (str) state abbreviation
        - clinic: (str) CPC or HPC

    Returns G (nx graph object) and df (pd dataframe)
    """
    if clinic == "HPC":
        csv = HPCOUT.replace("state", state)
    else:
        csv = CPCOUT.replace("state", state)

    # Load in data
    try:
        df = pd.read_csv(csv, dtype={0:int, 1:str})

    except FileNotFoundError:
        print("No data on", state, "due to abortion ban or lack of data.")
        sys.exit(1)
    
    # Initialize graph
    G = nx.Graph()
    G.add_node(state, label=state)

    return G, df

def clinic_graph(state, num_edges, clinic):
    """
    Create clinic-level networkx graph with each node a top token 
    by count and the edges labeled with the token counts.

    Inputs:
        - state: (str) state abbrevation
        - num_edges: (int) number of tokens to include in graph
        - clinic: (str) CPC or HPC
    
    Returns graph object with labeled nodes and edges
    """
    G, df = initialize_graph(state, clinic)
    num_counts = len(df.columns) - 3
    col = random.randint(1, num_counts)
    col = "Total" + str(col)

    # Catch large entries for number of edges
    if num_edges > num_counts:
        num_edges = 10

    # Build graph
    for edge in top_clinic_tokens(df, col, num_edges):
        G.add_node(edge['token'], label=edge['token'])
        G.add_edge(state, edge['token'], label=int(edge['Count']))

    return G

# def state_graph(state, num_edges, clinic):
#     """
#     """
#     G, df = initialize_graph(state, clinic)
#     for col in df.columns[2:]:
#         top = top_clinic_tokens(df, col, num)

# example (test with poetry)
# python truth_inquery/crawler/graphs.py "MN" 10 "CPC"
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(
            "usage: python {} <state abbreviation> <number of edges> <CPC or HPC>".format(
                sys.argv[0]
            )
        )
        sys.exit(1)

    # Set arguments
    state, num_edges, clinic = sys.argv[1:4]
    num_edges = int(num_edges)

    filename = "truth_inquery/output_graphs/" + \
                state + "-" + clinic + "-" + str(num_edges) + "-token-graph.png"

    if os.path.isfile(filename):
        os.remove(filename)

    # Draw graph
    G = clinic_graph(state, num_edges, clinic)
    pos = nx.spring_layout(G)

    # Add options using clinic as key
    nx.draw(G, pos, **OPTIONAL[clinic])
    nx.draw_networkx_labels(G, pos, nx.get_node_attributes(G, 'label'), \
                                    verticalalignment='baseline')
    nx.draw_networkx_edge_labels(G,pos,edge_labels=nx.get_edge_attributes(G,'label'))
    plt.savefig(filename)
    # plt.show()
