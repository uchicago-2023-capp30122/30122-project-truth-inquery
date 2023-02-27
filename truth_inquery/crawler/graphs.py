# Aaron Haefner
import pandas as pd
import networkx as nx
import os
import sys
import random
import matplotlib.pyplot as plt
from crawler import CPCIN, CPCOUT, HPCIN, HPCOUT, STATES

random.seed(1234)

# Options for nx graphs
OPTIONAL = {
    'CPC':{'node_color': 'red',
           'alpha': 0.6,
           'node_size': 600,
           'width': 1},
    'HPC':{'node_color': 'blue',
           'alpha': 0.6,
           'node_size': 600,
           'width': 1}
    }
OPTIONAL_STATE = {
    'CPC':{'node_color': 'red',
           'alpha': 0.6,
           'node_size': 10000,
           'width': 2},
    'HPC':{'node_color': 'blue',
           'alpha': 0.6,
           'node_size': 10000,
           'width': 2}
    }

# Hand identified
TOKEN_IGNORE = (
    "var",
    "label",
    "format",
    "instanceof",
    "blockquote",
    "solid",
    "text",
    "auto",
    "val",
    "column",
    "settings",
    "function",
    "return",
    "opacity",
    "cta",
    "display",
)

def top_clinic_tokens(df, col, top_num):
    """
    Cleans token-count pandas dataframe for single URL (clinic)

    Inputs
        - df: (pd dataframe) dataframe as token-count columns (2)
        - col (string): column with token counts of URL
        - top_num (int): number of top tokens to return

    Returns standardized dataframe of nlargest tokens by count 
    """
    # filter and sort
    node = df[~df['token'].str.contains('|'.join(TOKEN_IGNORE), na = False)]
    node = node[['token', col]].sort_values(by=[col], ascending=False)
    node = node.rename(columns = {col: 'Count'})

    # Top largest as list of dicts (key:val = token:count)
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
        df = pd.read_csv(csv)

    except FileNotFoundError:
        print("No data on", state, "due to abortion ban or lack of data.")
        sys.exit(1)

    G = nx.Graph()
    G.add_node(state)

    return G, df

def clinic_graph(state, num_edges, col=0):
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

    if col == 0 or col > num_counts:
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

def state_graph(state, clinic, top=3, num_edges=10):
    """
    Create state-level nx graph with num_edge nodes 
    where each node represents a clinic containing 
    the top N tokens crawled from the clinic's 'URL network'

    Inputs:
        - state: (str) state
        - clinic: (str) CPC or HPC
        - top: (int) top number of tokens 
    """
    G, df = initialize_graph(state, clinic)

    for col in df.columns[2:]:
        tokenlst = top_clinic_tokens(df, col, top)
        tokens = [(dct['token'], int(dct['Count'])) for dct in tokenlst]
        G.add_node(col, label = tokens)
        G.add_edge(state, col, label = col.replace('Total',''))
        if len(G) == num_edges:
            return G

    return G

def draw_graph(graph, state, clinic, filename, col = ""):
    """
    """
    pos = nx.spring_layout(graph)
    if col == "":
        plt.title(STATES[state])
        nx.draw(graph, pos, **OPTIONAL_STATE[clinic])
    # Add options using clinic as key
    else:
        plt.title(STATES[state]+" "+clinic+" ("+str(col)+")")
        nx.draw(graph, pos, **OPTIONAL[clinic])

    nx.draw_networkx_labels(graph, pos, nx.get_node_attributes(graph, 'label'), \
                            verticalalignment='center', \
                            font_size = 8, font_weight="bold")
    
    nx.draw_networkx_edge_labels(graph, pos, \
                                edge_labels=nx.get_edge_attributes(graph,'label'))

    plt.savefig(filename)
    plt.show()

# example (test with poetry)
# python truth_inquery/crawler/graphs.py "MN" 10 "CPC"
if __name__ == "__main__":

    if len(sys.argv) not in [4,5]:
        print(
            "usage: python {} <state abbrev> <num tokens> <CPC or HPC> <optional: col/url number>".format(
                sys.argv[0]
            )
        )
        sys.exit(1)

    # Set arguments
    state, num_edges, clinic = sys.argv[1:4]
    num_edges = int(num_edges)

    filename = "truth_inquery/output_graphs/" + \
                state + "-" + clinic + "-" + str(num_edges) + "-state-graph.png"
                
    filename2 = "truth_inquery/output_graphs/" + \
                state + "-" + clinic + "-" + str(num_edges) + "-token-graph.png"
    # Comment out this and savefig to show new graphs without overwriting
    if os.path.isfile(filename) and os.path.isfile(filename2):
        os.remove(filename)
        os.remove(filename2)

    # Draw graph
    # You can find a "good" URL here by state-clinic type
    # e.g. {state: 'MN', clinic: 'CPC', col: 16}, 0 = randomint in range of col
    col = 16
    G = state_graph(state, clinic, 3, num_edges)
    draw_graph(G, state, clinic, filename)

    G2 = clinic_graph(state, num_edges, col)
    draw_graph(G2, state, clinic, filename2, col)