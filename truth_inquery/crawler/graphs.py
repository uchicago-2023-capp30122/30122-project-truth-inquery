# Aaron Haefner
# Generate graphs from token data
"""
- clone repo - 
cd 30122-project-truth-inquery
poetry install
poetry run python truth_inquery/crawler/graphs.py <state abbrev> <num edges (int)>
"""
import sys
import re
import glob 
import pandas as pd
import networkx as nx
import random
import matplotlib.pyplot as plt
from hpc_urls import CPCOUT, HPCOUT
from truth_inquery.analysis_model.states import STATES

random.seed(1234)

PATTERN = r'[^a-z]'
CPC = "CPC"
HPC = "HPC"
STATE_CPC = "truth_inquery/output_graphs/CPC-state-clinics.pdf"
STATE_HPC = "truth_inquery/output_graphs/HPC-state-clinics.pdf"
CPC_GRAPH = "truth_inquery/output_graphs/CPC-state-tokens.pdf"
HPC_GRAPH = "truth_inquery/output_graphs/HPC-state-tokens.pdf"

# Options for nx graphs
OPTIONAL = {
    'CPC':{'node_color': 'red',
           'alpha': 0.6,
           'node_size': 1000,
           'width': 1},
    'HPC':{'node_color': 'blue',
           'alpha': 0.6,
           'node_size': 1000,
           'width': 1}
    }
OPTIONAL_STATE = {
    'CPC':{'node_color': 'red',
           'alpha': 0.6,
           'node_size': 5000,
           'width': 2},
    'HPC':{'node_color': 'blue',
           'alpha': 0.6,
           'node_size': 5000,
           'width': 2}
    }
LABELS = {
    'NODE': {"verticalalignment":'center',
           "font_size":8,
           "font_weight":"bold"
        },
    'EDGE': {"font_size":12}
}

TOKEN_IGNORE = (
    "var", "label", "format", "instanceof", "blockquote", "solid", "text",
    "auto", "val", "column", "settings", "function", "return", 'solid',
    "opacity", "cta", "display", "and", "are", "for", "from", "has", "its",
    "that", "the", "was", "were", "will", "with", "breakpoint", "thead", "tbody",
)

def initialize_graph(state, clinic):
    """
    Load data and initalize a networkx graph using
    state abbrevation input as base node.

    Inputs
        - state: (str) state abbreviation
        - clinic: (str) CPC or HPC

    Returns G (nx graph object) and df (pd dataframe)
    """
    if clinic == HPC:
        csv = HPCOUT.replace("state", state)
    else:
        csv = CPCOUT.replace("state", state)

    # Load in data
    try:
        df = pd.read_csv(csv)

    except FileNotFoundError:
        print("No data on", state, "due to abortion ban or scheduling pause.")
        return None

    # Create graph
    G = nx.Graph()
    G.add_node(state)

    return G, df

def get_token(df, ind, i):
    """
    Extract i_th top token from dataframe

    Inputs:
        - df: (pandas df) tidy url-level token, count df
        - ind: (int) row index
        - i: (int) column index

    Returns token (str) and count (int)
    """
    token = df.loc[ind,'token'+str(i)].replace("'","")
    count = df.loc[ind,'count'+str(i)]
    return token, count

def build_graph(G, df, state, num_edges):
    """
    Build clinic-level graph where each node is a toke and edges are labeled with counts

    Inputs:
        - G (nx graph): graph with only base node
        - df: token, count df
        - state: (str) state abbreviation
        - num_edges: (int) number of nodes/edges to add

    Returns graph object
    """
    pattern = re.compile(PATTERN)
    ind = random.randint(0, max(df.index)-1)
    i = 1

    # Build until desired size
    while len(G) < num_edges:
        token, count = get_token(df, ind, i)

        # Filter tokens
        if token not in TOKEN_IGNORE and re.search(pattern, token) is None:
            G.add_node(token, label=token)
            G.add_edge(state, token, label=int(count))
        i += 1

    return G

def clinic_graph(state, clinic, num_edges=10):
    """
    Create clinic-level networkx graph with each node a top token
    by count and the edges labeled with the token count for the clinic input

    Inputs:
        - state: (str) state abbrev
        - clinic: (str) CPC or HPC
        - num_edges: (int) number of tokens to include in graph

    Returns graph object with labeled nodes and edges
    """
    G, df = initialize_graph(state, clinic)
    return build_graph(G, df, state, num_edges)

def state_graph(state, clinic, num_edges=10):
    """
    Create state-level nx graph with num_edge nodes 
    where each node represents a clinic containing 
    the top 5 tokens crawled from the clinic's 'URL network'

    Inputs:
        - state: (str) state
        - clinic: (str) CPC or HPC
        - num_edges: (int) max number of edges

    Returns graph object
    """
    pattern = re.compile(PATTERN)
    G, df = initialize_graph(state, clinic)
    i = 1

    # Build graph until run out of URLs or num edges
    while len(G) < min(len(df), num_edges):

        # Token labels for node
        tokens = []
        ind = random.randint(0, max(df.index))
        while len(tokens) < 5:
            token, count = get_token(df, ind, i)

            if token not in TOKEN_IGNORE and re.search(pattern, token) is None:
                tokens.append(token+" "+str(count))
            i += 1

        # Write token-counts on separate lines in node
        G.add_node(ind, label = ' \n'.join(tokens))
        G.add_edge(state, ind, label = ind)

    return G

def draw_graph(graph, state, clinic, outpath):
    """
    Draw nx graph

    Inputs:
        - graph: (nx graph object): contains token-count nodes
        - state: (str) state abbrev
        - clinic: (str) CPC or HPC
        - outpath: (str) .pdf path to save graph
    """
    pos = nx.spring_layout(graph)
    plt.title(STATES[state]+" "+clinic)

    # State graph
    if 'clinics' in outpath:
        nx.draw_networkx(graph, pos, with_labels=False, **OPTIONAL_STATE[clinic])
    # Individual clinic graph
    else:
        nx.draw_networkx(graph, pos, with_labels=False, **OPTIONAL[clinic])

    #Node labels
    nx.draw_networkx_labels(graph, pos, nx.get_node_attributes(graph, 'label'), \
                            **LABELS['NODE'])

    # Edge labels
    nx.draw_networkx_edge_labels(graph, pos, \
                                edge_labels=nx.get_edge_attributes(graph,'label'), \
                                **LABELS['EDGE'])

    plt.savefig(outpath, dpi=500)
    plt.show()
    plt.clf()

if __name__ == "__main__":
    files = glob.glob("truth_inquery/output/*.csv")

    if len(sys.argv) != 3:
        print(
            "usage: python {} <state abbrev> <num edges>".format(sys.argv[0])
        )
        sys.exit(1)

    # Set arguments
    state, num_edges = sys.argv[1:3]
    num_edges = int(num_edges)
    check = CPCOUT.replace('state', state)

    # Output paths
    cpcpath = CPC_GRAPH.replace("state",state)
    hpcpath = HPC_GRAPH.replace("state",state)
    state_cpc = STATE_CPC.replace("state",state)
    state_hpc = STATE_HPC.replace("state",state)

    if check not in files:
        print(state,"data not available.")
        sys.exit(1)

    # Clinic-level graphs
    G_cpc = clinic_graph(state, CPC, num_edges)
    draw_graph(G_cpc, state, CPC, cpcpath)

    G_hpc = clinic_graph(state, HPC, num_edges)
    draw_graph(G_hpc, state, HPC, hpcpath)

    # State-level graphs
    CPC_state = state_graph(state, CPC)
    draw_graph(CPC_state, state, CPC, state_cpc)

    HPC_state = state_graph(state, HPC)
    draw_graph(CPC_state, state, HPC, state_hpc)

    print("Graphs drawn")
