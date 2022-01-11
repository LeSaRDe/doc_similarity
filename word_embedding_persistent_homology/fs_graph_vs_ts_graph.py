import networkx as nx
import sqlite3
# import urllib
import dionysus as di
import persim
import numpy as np
import gudhi
import json
import logging


g_l_weeks = ['20180415_20180422', '20180429_20180506', '20180513_20180520',
            '20180527_20180603', '20180610_20180617', '20180624_20180701',
            '20180708_20180715', '20180722_20180729', '20180805_20180812',
            '20180819_20180826']
g_fs_graph_path_format = '/home/mf3jh/workspace/data/white_helmet/White_Helmet/Twitter/sampled_data/updated_data/{0}/tw_wh_fsgraph_{1}_10_sample_weight.json'
g_ts_graph_path_format = '/home/mf3jh/workspace/data/white_helmet/White_Helmet/Twitter/sampled_data/updated_data/{0}/tw_wh_tsgraph_{1}_10_sample.json'


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
    for week in g_l_weeks:
        logging.debug('Week: %s' % week)
        with open(g_fs_graph_path_format.format(week, week), 'r') as in_fs_fd:
            with open(g_ts_graph_path_format.format(week, week), 'r') as in_ts_fd:
                fs_graph = nx.adjacency_graph(json.load(in_fs_fd))
                ts_graph = nx.adjacency_graph(json.load(in_ts_fd))

                l_max_cliques_fs = list(nx.find_cliques(fs_graph))
                l_simplices_fs = build_simplices(l_max_cliques_fs)
                print("[INF] Total number of simplices for fs_graph: %s" % len(l_simplices_fs))
                filtration_fs = build_filatration(fs_graph, l_simplices_fs)
                persistent_homology_fs = get_persistent_homology(filtration_fs)

                l_max_cliques_ts = list(nx.find_cliques(ts_graph))
                l_simplices_ts = build_simplices(l_max_cliques_ts)
                print("[INF] Total number of simplices for ts_graph: %s" % len(l_simplices_ts))
                filtration_ts = build_filatration(ts_graph, l_simplices_ts)
                persistent_homology_ts = get_persistent_homology(filtration_ts)

                bdist = diagram_distance(persistent_homology_fs, filtration_fs, persistent_homology_ts, filtration_ts)
                print('[INF] Bottlenect distance = %s' % bdist)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()