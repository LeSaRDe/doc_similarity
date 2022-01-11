import networkx as nx
import sqlite3
# import urllib
import dionysus as di
import persim
import numpy as np
import gudhi

g_db_path = '/home/fcmeng/workspace/word_embedding_persistent_homology/wordsim.db'
g_word_sim_threshold = 0.6
g_model_1 = 'sim'
g_model_2 = 'fasttext'


def get_all_vertices():
    db_conn = sqlite3.connect(g_db_path)
    db_cur = db_conn.cursor()
    db_cur.execute('select idx from words_idx')
    l_word_ids = [row[0] for row in db_cur.fetchall()]
    db_conn.close()
    return l_word_ids

# 'key_to_keys' and 'keys_to_key' are functions originally defined in 'idx_bit_translate.py'
def key_to_keys(key):
    key_b = '{0:032b}'.format(key)
    return int(key_b[:16], 2), int(key_b[16:], 2)


def keys_to_key(key1, key2):
    key_b = '{0:016b}'.format(key1) + '{0:016b}'.format(key2)
    return int(key_b, 2)


def build_word_graph(l_word_ids, word_sim_col):
    # first add all vertices
    word_graph = nx.Graph()
    word_graph.add_nodes_from(l_word_ids)
    # second add considered edges
    db_conn = sqlite3.connect(g_db_path)
    db_cur = db_conn.cursor()
    db_cur.execute('select word_pair_idx, %s from words_sim' % word_sim_col)
    db_row = db_cur.fetchone()
    while db_row is not None:
        if db_row[1] >= g_word_sim_threshold:
            w1, w2 = key_to_keys(db_row[0])
            word_graph.add_edge(w1, w2, weight=db_row[1])
        db_row = db_cur.fetchone()
    db_conn.close()
    return word_graph


def build_simplices(l_max_cliques):
    all_simplices = []
    for max_clique in l_max_cliques:
        simplex = di.Simplex(max_clique)
        closure = di.closure([simplex], len(simplex)-1)
        all_simplices = all_simplices + closure
    return all_simplices


def assign_filtration_value(simplex, word_graph):
    if len(simplex) <= 1:
        simplex.data = 0.0
        return simplex
    edges = word_graph.edges
    filtration_value = 0.0
    for i in range(0, len(simplex)-1):
        for j in range(i+1, len(simplex)):
            if word_graph.has_edge(simplex[i], simplex[j]):
                weight = word_graph[simplex[i]][simplex[j]]["weight"]
                weight = 1.0 - weight
                if weight > filtration_value:
                    filtration_value = weight
    simplex.data = filtration_value
    return simplex


def build_filatration(word_graph, l_simplices):
    filtration = di.Filtration()
    for simplex in l_simplices:
        simplex = assign_filtration_value(simplex, word_graph)
        filtration.append(simplex)
    filtration.sort()
    return filtration


def get_persistent_homology(filtration):
    # filtration.sort()
    ph = di.homology_persistence(filtration)
    return ph


def plot_diagrams(persistent_homology, filtration):
    diagram = di.init_diagrams(persistent_homology, filtration)
    nonzero_dims = []
    for dim, diag in enumerate(diagram):
        if len(diag) != 0:
            nonzero_dims.append(dim)
            print("[INF] Dimension %s: pairs: %s" % (dim, len(diag)))
    for dim in nonzero_dims:
        di.plot.plot_diagram(diagram[dim], labels=True, show=True)
        di.plot.plot_bars(diagram[dim], show=True)
        di.plot.plot_diagram_density(diagram[dim], show=True)


def diagram_distance(persistent_homology_1, filtration_1, persistent_homology_2, filtration_2):
    diagram_1 = di.init_diagrams(persistent_homology_1, filtration_1)
    diagram_2 = di.init_diagrams(persistent_homology_2, filtration_2)
    # wdist = di.wasserstein_distance(diagram_1[1], diagram_1[1], q=2)
    # bdist = di.bottleneck_distance(diagram_1[1], diagram_1[1])
    diag1_arr = [[pair.birth, pair.death] for pair in diagram_1[1]]
    diag1_arr = np.array(diag1_arr)
    diag2_arr = [[pair.birth, pair.death] for pair in diagram_2[1]]
    diag2_arr = np.array(diag2_arr)
    # fix all inf to 1.0
    for pair in diag1_arr:
        if pair[0] == float('Inf'):
            pair[0] = 1.0
        if pair[1] == float('Inf'):
            pair[1] = 1.0
    for pair in diag2_arr:
        if pair[0] == float('Inf'):
            pair[0] = 1.0
        if pair[1] == float('Inf'):
            pair[1] = 1.0
    wdist = 0
    bdist = 0
    bn_matching, (matchidx, D) = persim.bottleneck(diag1_arr, diag2_arr, matching=True)
    persim.bottleneck_matching(diag1_arr, diag2_arr, matchidx, D)
    # wn_matching, (matchidx, D) = persim.wasserstein(diag1_arr, diag2_arr, matching=True)
    # persim.wasserstein_matching(diag1_arr, diag2_arr, matchidx, D)
    # gudhi_bdist = gudhi.bottleneck_distance(diag1_arr, diag2_arr, 0)
    return bn_matching


def main():
    l_word_ids = get_all_vertices()
    print("[INF] Total number of words: %s" % len(l_word_ids))

    word_graph_1 = build_word_graph(l_word_ids, g_model_1)
    print("[INF] Word graph info:")
    print(nx.info(word_graph_1))
    l_max_cliques_1 = list(nx.find_cliques(word_graph_1))
    l_simplices_1 = build_simplices(l_max_cliques_1)
    print("[INF] Total number of simplices: %s" % len(l_simplices_1))
    filtration_1 = build_filatration(word_graph_1, l_simplices_1)
    persistent_homology_1 = get_persistent_homology(filtration_1)
    # plot_diagrams(persistent_homology, filtration)
    #print persistent_homology

    word_graph_2 = build_word_graph(l_word_ids, g_model_2)
    print("[INF] Word graph info:")
    print(nx.info(word_graph_2))
    l_max_cliques_2 = list(nx.find_cliques(word_graph_2))
    l_simplices_2 = build_simplices(l_max_cliques_2)
    print("[INF] Total number of simplices: %s" % len(l_simplices_2))
    filtration_2 = build_filatration(word_graph_2, l_simplices_2)
    persistent_homology_2 = get_persistent_homology(filtration_2)

    bdist = diagram_distance(persistent_homology_1, filtration_1, persistent_homology_2, filtration_2)

    print('[INF] Bottlenect distance = %s' % bdist)

if __name__ == '__main__':
    main()
