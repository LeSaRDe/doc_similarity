from pygsp import graphs, filters
import numpy as np
import networkx as nx
import json
import time
import os
import copy
import sqlite3
import math
import multiprocessing
import build_single_doc_apv
import phrase_graph_utils


data_path = '%s/workspace/data/' % os.environ['HOME']
data_set = ''
run_name = ''
graph_dump_folder_suffix = '_neighbor_graphs/'
doc_pv_folder_suffix = '_doc_pv/'
TOP_SIM_DOCS = 5
MAX_DOC_PV_PROCS = 10
MULTI_PROC = True

# imagine that 'compare_phrase_neighbor_graph' is a piece of steel sheet, and 'compare_phrase_neighbor_graph'
# is a point on this sheet. now we heat up this point by 'phrase_sim' degree. thus, at the moment 0, only this
# point is heated up, and hits a temperature at 'phrase_sim' degree. also, the rest of this sheet keeps cool
# at temperature 0. this initial temperature state of the whole sheet is our signal.
# it should be guaranteed that the returned signal will always be non-zero
def build_signal(phrase, compare_phrase, compare_phrase_neighbor_graph, word_pair_sim_dict):
    # if compare_phrase not in signal_order:
    #     raise Exception('[ERR]: %s is not in phrase_neighbor_graph' % compare_phrase)
    phrase_sim = build_single_doc_apv.phrase_phrase_sim(phrase.split('#'), compare_phrase.split('#'), word_pair_sim_dict)
    # if 'phrase' is not similar to 'compare_phrase', then sure of course 'compare_phrase' cannot contribute
    # any semantics to 'phrase', and thus we don't have to proceed with this 'compare_phrase' anymore.
    # to save some time, we directly return None, None for this 'compare_phrase'.
    if phrase_sim == 0.0:
        return None, None
    signal_order = list(compare_phrase_neighbor_graph.nodes)
    signal_vals = np.zeros(len(signal_order))
    idx_compare_phrase = signal_order.index(compare_phrase)
    signal_vals[idx_compare_phrase] = phrase_sim

    # TODO
    # only for test
    if np.sum(signal_vals) == 0.0:
        raise Exception('[ERR]: zero-valued signal occurs!')

    return signal_order, signal_vals


# the input signal should always be non-zero. and this should be guaranteed in 'build_signal()'.
def signal_filtering(signal_order, signal_vals, gsp_graph, compare_phrase):
    if gsp_graph.n_vertices == 0:
        raise Exception('[ERR]: Empyt gsp_graph for %s' % compare_phrase)
    # sending a single-node graph to filtering will lead to NaN filtered signal
    elif gsp_graph.n_vertices <= 1:
        return signal_vals
    # freq_response = gsp_graph.gft(np.asarray(signal_dict.values()))
    # print "[DBG]: Frequence Response = "
    # print freq_response
    filter_g = filters.Heat(gsp_graph, scale=[1, 5, 10], normalize=False)
    # print "[DBG]: Heat kernel = "
    # print filter_g
    filtered_signal = filter_g.filter(signal_vals, method='exact')
    # print "[DBG]: Filter Results = "
    # print filtered_signal
    # print filtered_signal[:,0] + filtered_signal[:,1]
    try:
        ret_signal = filtered_signal[:, 0] + filtered_signal[:, 1] + filtered_signal[:, 2]
    except:
        print '[ERR]: filtered signal error!'
    # if compare_phrase not in signal_order:
    #     raise Exception('[ERR]: %s is not in signal!' % compare_phrase)
    idx_compare_phrase = signal_order.index(compare_phrase)
    ret_signal[idx_compare_phrase] = signal_vals[idx_compare_phrase]
    return ret_signal


def build_signal_dict(signal_order, signal_vals):
    signal_dict = dict()
    for idx, phrase in enumerate(signal_order):
        # if phrase in signal_dict.keys():
        #     raise Exception('[ERR]: Repeated phrase %s in signal!' % phrase)
        signal_dict[phrase] = signal_vals[idx]
    return signal_dict


def build_gsp_graph(adj_mat):
    gsp_graph = graphs.Graph(adj_mat, lap_type='normalized')
    gsp_graph.compute_laplacian(lap_type='normalized')
    gsp_graph.compute_fourier_basis()
    # for the single-node graph case, we don't really compute any filtered signal,
    # instead, we will directly return the input signal as the filtered signal,
    # because there is nothing to filter. then, we don't actually need the differential
    # operator or the max eigenval of laplacian.
    if gsp_graph.n_vertices > 1:
        gsp_graph.compute_differential_operator()
        gsp_graph.estimate_lmax()
    return gsp_graph


def load_one_neighbor_graph(graph_dump_file_path):
    graph_dump_fd = open(graph_dump_file_path, 'r')
    graph_dump = json.load(graph_dump_fd)
    my_graph = nx.adjacency_graph(graph_dump)
    # print '[DBG]: type = %s' % type(my_graph)
    # print list(my_graph.nodes(data=True))
    # print list(my_graph.edges(data=True))
    return my_graph


def get_avg_arc(phrase, neighbor_graph):
    if phrase not in list(neighbor_graph.nodes):
        raise Exception('[ERR]: %s is not in its neighbor graph!' % phrase)
    sum_arc = 0
    arc_count = 0
    l_all_arcs = neighbor_graph.nodes(data=True)[phrase]['loc'].values()
    for each_arc_list in l_all_arcs:
        arc_count += len(each_arc_list)
        sum_arc += sum(each_arc_list)
    if arc_count == 0:
        raise Exception('[ERR]: %s has no arc!' % phrase)
    ret_avg_arc = sum_arc / arc_count
    return ret_avg_arc


# all phrases have to be two-word.
def load_all_phrase_neighbor_graphs(graph_data_folder_path):
    phrase_neighbor_graph_dict = dict()
    for i, each_graph_data_file_name in enumerate(os.listdir(graph_data_folder_path)):
        if each_graph_data_file_name.endswith('.json'):
            each_neighbor_graph = load_one_neighbor_graph(graph_data_folder_path + each_graph_data_file_name)
            each_phrase_name = each_graph_data_file_name.replace('.json', '').strip()
            l_phrases = each_phrase_name.split('#')
            if len(l_phrases) == 2:
                alt_each_phrase_name = l_phrases[1] + '#' + l_phrases[0]
            else:
                raise Exception('[ERR]: Invalid phrase %s' % each_phrase_name)
            if each_phrase_name in phrase_neighbor_graph_dict.keys() or alt_each_phrase_name in phrase_neighbor_graph_dict.keys():
                raise Exception('[ERR]: Repeated phrase %s or %s' % (each_phrase_name, alt_each_phrase_name))
            else:
                avg_arc = get_avg_arc(each_phrase_name, each_neighbor_graph)
                phrase_neighbor_graph_dict[each_phrase_name] = {'neighbor_graph' : each_neighbor_graph, 'avg_arc' : avg_arc}
    return phrase_neighbor_graph_dict


def weight_phrase_vs_compare_phrase(phrase_item, compare_phrase_avg_arc):
    ret_w = 0.0
    for l_phrase_arc in phrase_item[1].values():
        for phrase_arc in l_phrase_arc:
            ret_w += math.exp(3.0 / (math.pow(phrase_arc, 3) + math.pow(compare_phrase_avg_arc, 3)))
    return ret_w


# compare 'phrase' extracted from the given doc to each each phrase in 'phrase_neighbor_graph_dict'
# and obtain a phrase-value dict for 'phrase'
# we only record those non-zero dimensions, otherwise the complexity would be too high.
def build_pv_for_one_phrase(phrase_item, phrase_neighbor_graph_dict, word_pair_sim_dict):
    phrase_pv_dict = dict()
    for compare_phrase, compare_phrase_neighbor_graph_dict in phrase_neighbor_graph_dict.items():
        signal_order, signal_vals = build_signal(phrase_item[0], compare_phrase, compare_phrase_neighbor_graph_dict['neighbor_graph'], word_pair_sim_dict)
        if signal_order is None or signal_vals is None:
            continue
        adj_mat = nx.adjacency_matrix(compare_phrase_neighbor_graph_dict['neighbor_graph'])
        gsp_graph = build_gsp_graph(adj_mat)
        filtered_signal = signal_filtering(signal_order, signal_vals, gsp_graph, compare_phrase)

        # TODO
        # this shouldn'd happen, but just in case, it happens, we directly skip it.
        if np.sum(filtered_signal) == 0:
            continue

        filtered_signal_dict = build_signal_dict(signal_order, filtered_signal)
        for key in filtered_signal_dict.keys():
            if filtered_signal_dict[key] == 0:
                continue
            if key in phrase_pv_dict.keys():
                phrase_pv_dict[key] += math.exp(filtered_signal_dict[key] * 2) * weight_phrase_vs_compare_phrase(phrase_item, compare_phrase_neighbor_graph_dict['avg_arc'])
            else:
                phrase_pv_dict[key] = math.exp(filtered_signal_dict[key] * 2) * weight_phrase_vs_compare_phrase(phrase_item, compare_phrase_neighbor_graph_dict['avg_arc'])
    return phrase_pv_dict


def find_top_sim_doc_pairs(doc_id):
    db_conn = sqlite3.connect(data_path + data_set + '.db')
    db_cur = db_conn.cursor()
    if TOP_SIM_DOCS == -1:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like doc_id_pair like "%s#%%" or doc_id_pair like "%%#%s" order by "%s"''' % (doc_id, doc_id, run_name)
    else:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like "%s#%%" or doc_id_pair like "%%#%s" order by "%s" DESC limit %s''' % (doc_id, doc_id, run_name, TOP_SIM_DOCS)
    db_cur.execute(query)
    rows = db_cur.fetchall()
    db_conn.close()
    return rows


def build_pv_for_one_doc(doc_id, json_files_path, phrase_neighbor_graph_dict, word_pair_sim_dict, output_folder):
    start = time.time()
    # get top similar docs
    doc_pairs = find_top_sim_doc_pairs(doc_id)
    # extract all phrases
    all_phrase_dict = build_single_doc_apv.collect_all_phrases(doc_id, doc_pairs, json_files_path)
    # build pv
    doc_pv_dict = dict()
    for phrase_item in all_phrase_dict.items():
        phrase_pv_dict = build_pv_for_one_phrase(phrase_item, phrase_neighbor_graph_dict, word_pair_sim_dict)
        for key in phrase_pv_dict.keys():
            if key in doc_pv_dict.keys():
                doc_pv_dict[key] += phrase_pv_dict[key]
            else:
                doc_pv_dict[key] = phrase_pv_dict[key]
    # output
    output_one_doc_pv(doc_id, doc_pv_dict, output_folder)
    print '[DBG]: doc_pv_dict for %s is done in %s secs!' % (doc_id, time.time() - start)
    # return doc_pv_dict


def output_one_doc_pv(doc_id, doc_pv_dict, output_folder):
    output_fd = open(output_folder + doc_id.replace('/', '_') + '.json', 'w')
    json.dump(doc_pv_dict, output_fd, indent=4)
    output_fd.close()
    print '[DBG]: %s dump is done!' % doc_id


def get_doc_ids():
    db_conn = sqlite3.connect(data_path + data_set + '.db')
    db_cur = db_conn.cursor()
    db_cur.execute('''SELECT doc_id from docs order by doc_id''')
    rows = db_cur.fetchall()
    db_conn.close()
    return [row[0] for row in rows]


def proc_cool_down(l_doc_pv_procs, doc_pv_procs_threshold):
    while len(l_doc_pv_procs) >=  doc_pv_procs_threshold:
        for proc in l_doc_pv_procs:
            if proc.pid != os.getpid():
                proc.join(1)
            if not proc.is_alive():
                l_doc_pv_procs.remove(proc)


def main(doc_folder_name):
    global data_set, run_name, data_path
    data_set = doc_folder_name[:doc_folder_name.find('_')]
    run_name = doc_folder_name[doc_folder_name.find('_') + 1:]
    doc_pv_output_folder_name = doc_folder_name + doc_pv_folder_suffix
    phrase_graph_utils.init(data_path, data_set)
    start = time.time()
    word_pair_sim_dict = phrase_graph_utils.get_word_pair_sims()
    print '[DBG]: word_pair_sim_dict is done in %s secs!' % (time.time()-start)
    start = time.time()
    phrase_neighbor_graph_dict = load_all_phrase_neighbor_graphs(data_path + doc_folder_name + graph_dump_folder_suffix)
    l_doc_pv_procs = []
    print '[DBG]: phrase_neighbor_graph_dict is done in %s secs!' % (time.time()-start)
    l_doc_pv_done = []
    for i, each_graph_data_file_name in enumerate(os.listdir(data_path + doc_pv_output_folder_name)):
        if each_graph_data_file_name.endswith('.json'):
            l_doc_pv_done.append(each_graph_data_file_name.replace('.json', '').replace('_', '/').strip())
    doc_id_list = list(set(get_doc_ids()) - set(l_doc_pv_done))
    for doc_id in doc_id_list:
        if MULTI_PROC:
            doc_pv_proc = multiprocessing.Process(target=build_pv_for_one_doc, args=(doc_id, data_path + doc_folder_name, phrase_neighbor_graph_dict, word_pair_sim_dict, data_path + doc_pv_output_folder_name))
            l_doc_pv_procs.append(doc_pv_proc)
            doc_pv_proc.start()
            proc_cool_down(l_doc_pv_procs, MAX_DOC_PV_PROCS)
        else:
            build_pv_for_one_doc(doc_id, data_path + doc_folder_name, phrase_neighbor_graph_dict, word_pair_sim_dict, data_path + doc_pv_output_folder_name)
    if MULTI_PROC:
        proc_cool_down(l_doc_pv_procs, 1)

main('reuters_nasari_30_rmswcbwexpws_w3-3')