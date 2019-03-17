import sqlite3
import networkx as nx
from numpy import linalg as LA
import matplotlib.pyplot as plt
import os
from idx_bit_translate import key_to_keys

NOR_WEIGHT = True


def get_docs_sim_from_db(dataset, col_name):
    global conn, cur
    conn = sqlite3.connect('%s/workspace/data/%s.db' % (os.environ['HOME'], dataset))
    cur = conn.cursor()
    cur.execute('SELECT doc_id_pair, "%s" from docs_sim' % col_name)
    rows = cur.fetchall()
    return rows


def get_words_sim_from_db(dataset):
    global conn, cur
    conn = sqlite3.connect('%s/workspace/data/%s.db' % (os.environ['HOME'], dataset))
    cur = conn.cursor()
    cur.execute('SELECT word_pair_idx, sim from words_sim')
    rows = cur.fetchall()
    return rows


def build_graph(full_dicts, c_type):
    G = nx.Graph()
    for each in full_dicts:
        if c_type == 'word':
            id1, id2 = key_to_keys(each[0])
        else:
            id1, id2 = each[0].split('#')
        if NOR_WEIGHT:
            G.add_edge(int(id1), int(id2), weight=(float(each[1])+1)/2.0)
        else:
            G.add_edge(int(id1), int(id2), weight=float(each[1]))
    return G


def find_eig_gap(eig_values, num):
    diffs = []
    pos = []
    for i, eig in enumerate(eig_values):
        if i+1 < len(eig_values):
            diff = eig_values[i+1] - eig
            if len(diffs)<num:
                diffs.append(diff)
                pos.append(i+1)
            elif diff > min(diffs):
                to_rm = diffs.index(min(diffs))
                diffs.pop(to_rm)
                pos.pop(to_rm)
                diffs.append(diff)
                pos.append(i + 1)
        else:
            return dict(zip(pos, diffs))


def main(project_name, clustering_type):
    if clustering_type == 'word':
        full_list = get_words_sim_from_db(project_name)
    else:
        dataset_name, col_name = project_name[:project_name.find('_')], project_name[project_name.find('_') + 1:]
        full_list = get_docs_sim_from_db(dataset_name, col_name)
    G = build_graph(full_list, clustering_type)
    eig_values, eig_vectors = LA.eigh(nx.normalized_laplacian_matrix(G).todense())
    num_of_top_eig_values = 5
    eig_gaps = find_eig_gap(eig_values, num_of_top_eig_values)
    print "\n%s\n\neig_gaps: %s\n\neig_values:\n" % (project_name, eig_gaps)
    for each_e in eig_values:
        print each_e

    # nx.draw(G)
    plt.show()

# For word clustering
main('20news50short10', 'word')
# For doc clustering
# main('leefixsw_nasari_40_rmsw_w3-2', 'doc')
