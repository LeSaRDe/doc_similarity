import networkx as nx
import json
import phrase_graph_utils
import os
import time
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading

data_path = '%s/workspace/data/' % os.environ['HOME']
data_set = ''
run_name = ''
graph_dump_folder_suffix = '_neighbor_graphs/'
MAX_THREADS = 10


def build_whole_phrase_graph(doc_comp_res_folder):
    l_phrase_pairs = phrase_graph_utils.extract_phrase_pairs(doc_comp_res_folder)
    print '[INF]: phrase extraction is done!'
    whole_phrase_graph = phrase_graph_utils.create_phrase_graph(l_phrase_pairs)
    print '[INF]: whole phrase graph is done!'

    # whole_phrase_graph = nx.Graph()
    # node1_name = 'a'
    # node1_data = 0.1
    # whole_phrase_graph.add_node(node1_name, arc=node1_data)
    # node2_name = 'b'
    # node2_data = 0.2
    # whole_phrase_graph.add_node(node2_name, arc=node2_data)
    # node3_name = 'c'
    # node3_data = 0.3
    # whole_phrase_graph.add_node(node3_name, arc=node3_data)
    # node4_name = 'd'
    # node4_data = 0.4
    # whole_phrase_graph.add_node(node4_name, arc=node4_data)
    # node5_name = 'e'
    # node5_data = 0.5
    # whole_phrase_graph.add_node(node5_name, arc=node5_data)
    #
    # whole_phrase_graph.add_edge(node1_name, node2_name, weight=0.5)
    # whole_phrase_graph.add_edge(node1_name, node3_name, weight=0.8)
    # whole_phrase_graph.add_edge(node1_name, node4_name, weight=0.8)
    # whole_phrase_graph.add_edge(node1_name, node5_name, weight=0.3)
    # whole_phrase_graph.add_edge(node3_name, node4_name, weight=0.6)

    return whole_phrase_graph


def dump_one_phrase_neighbor_graph(dump_path, dump_name, phrase_neighbor_graph):
    start = time.time()
    neigbhor_graph_data = nx.adjacency_data(phrase_neighbor_graph)
    graph_dump_fd = open(dump_path + dump_name + '.json', 'w')
    json.dump(neigbhor_graph_data, graph_dump_fd, indent=4)
    graph_dump_fd.close()
    print '[DBG]: graph dump for %s is done! time = %s' % (dump_name, time.time() - start)


def thread_cool_down(l_threads):
    while len(l_threads) >= MAX_THREADS:
        for thread in l_threads:
            thread.join(1)
            if not thread.is_alive():
                l_threads.remove(thread)


def dump_phrase_neighbor_graphs(dump_path, whole_phrase_graph):
    l_phrase_nodes = list(whole_phrase_graph.nodes)
    l_threads = []
    print '[DBG]: %s nodes in total to be dumped.' % len(l_phrase_nodes)
    for phrase_node in l_phrase_nodes:
        neighbor_graph_nodes = [phrase_node]
        neighbors = whole_phrase_graph.neighbors(phrase_node)
        neighbor_graph_nodes.extend(list(neighbors))
        neighbor_graph = nx.subgraph(whole_phrase_graph, neighbor_graph_nodes)
        thread = _threading.Thread(target=dump_one_phrase_neighbor_graph, args=(dump_path, phrase_node, neighbor_graph), name=phrase_node)
        l_threads.append(thread)
        thread.start()
        thread_cool_down(l_threads)
    thread_cool_down(l_threads)


def main(folder):
    global data_set, run_name, data_path
    data_set = folder[:folder.find('_')]
    run_name = folder[folder.find('_') + 1:]
    phrase_graph_utils.init(data_path, data_set)
    whole_phrase_graph = build_whole_phrase_graph(data_path + folder + '/')
    dump_phrase_neighbor_graphs(data_path+data_set+'_'+run_name+graph_dump_folder_suffix, whole_phrase_graph)
    # dump_graph(my_graph)
    # my_graph_recover = load_graph('graph_dump.json')
    # adj_mat = nx.to_numpy_matrix(my_graph_recover)
    # print '[DBG]: adj mat = '
    # print adj_mat
    # gsp_graph = build_gsp_graph(adj_mat)
    # print '[DBG]: gsp_graph = '
    # print gsp_graph
    # print "[DBG]: Laplacian = "
    # print gsp_graph.L
    # print "[DGB]: Fourier Basis  = "
    # print gsp_graph.U
    # print "[DBG]: Eigenvals = "
    # print gsp_graph.e
    # signal = build_signal('p', 'a', my_graph_recover)
    # print '[DBG]: Signal = '
    # print signal
    # filtered_signal = signal_filtering(signal, gsp_graph)
    # print '[DBG]: Filtered signal = '
    # print filtered_signal
    # pv = build_pv(list(my_graph_recover.nodes), filtered_signal)
    # print '[DBG]: PV = '
    # print pv


main('20news50short10_nasari_30_rmswcbwexpws_w3-3')
# main('phrase_neigbhor_test')