import networkx as nx
import os
import json
import sqlite3
import copy
import idx_bit_translate


PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']
WORD_SIM_MODE = 'nasari'
data_set = ''
data_path = ''
WORD_SIM_THRESHOLD = 0.3

# init this utils module before anything going
def init(in_data_path, in_data_set):
    global data_set, data_path
    data_set = in_data_set
    data_path = in_data_path


def get_word_pair_sims():
    conn = sqlite3.connect("%s%s.db" % (data_path, data_set))
    cur = conn.cursor()
    pairwise_sims = dict()
    all_sims = cur.execute('SELECT * from words_sim WHERE sim >= %s order by word_pair_idx' % WORD_SIM_THRESHOLD).fetchall()
    print "Sim >= %s, total word pairs=%s " % (WORD_SIM_THRESHOLD, len(all_sims))
    all_words_idx = cur.execute('SELECT * from words_idx').fetchall()
    all_idx_word = dict()
    for word, idx in all_words_idx:
        all_idx_word[idx] = word
    for idx, sim in all_sims:
        w1_idx, w2_idx = idx_bit_translate.key_to_keys(idx)
        pairwise_sims[all_idx_word[w1_idx]+"#"+all_idx_word[w2_idx]] = sim
    conn.close()
    return pairwise_sims


def get_word_sim_for_phrase(w1, w2):
    conn = sqlite3.connect("%s%s.db" % (data_path, data_set))
    db_cur = conn.cursor()
    if w1 is None or w2 is None:
        raise Exception('[ERR]: Invalid word input!')
    db_cur.execute('SELECT idx from words_idx where word = ?', (w1,))
    w1_idx = db_cur.fetchone()[0]
    if w1_idx is None:
        raise Exception('[ERR]: Cannot find %s' % w1)
    db_cur.execute('SELECT idx from words_idx where word = ?', (w2,))
    w2_idx = db_cur.fetchone()[0]
    if w2_idx is None:
        raise Exception('[ERR]: Cannot find %s' % w2)
    if w1_idx == w2_idx:
        w_sim = 1.0
        conn.close()
        return w_sim
    elif w1_idx < w2_idx:
        word_pair_id = idx_bit_translate.keys_to_key(w1_idx, w2_idx)
    else:
        word_pair_id = idx_bit_translate.keys_to_key(w2_idx, w1_idx)
    db_cur.execute('SELECT sim from words_sim where word_pair_idx = ?', (word_pair_id,))
    w_sim = db_cur.fetchone()[0]
    if w_sim is None:
        raise Exception('[ERR]: Cannot find sim for (%s, %s)' % (w1, w2))
    conn.close()
    return w_sim


def get_phrase_sim(p1, p2):
    ret_sim = 0.0

    if len(p1) == 0 or len(p2) == 0 or len(p1) + len(p2) > 4 or len(p1) + len(p2) < 3:
        print p1
        print p2
        raise Exception('[ERR]: p1, p2 are not valid:')
    if len(p1) == 2 and len(p2) == 2:
        # case 1: p1[0]-p2[0], p1[1]-p2[1]
        p_sim1 = 0
        w_sim1 = get_word_sim_for_phrase(p1[0], p2[0])
        #p_sim1 += w_sim1
        w_sim2 = get_word_sim_for_phrase(p1[1], p2[1])
        p_sim1 = min(w_sim1, w_sim2)

        # case 2: p1[0]-p2[1], p1[1]-p2[0]
        p_sim2 = 0
        w_sim1 = get_word_sim_for_phrase(p1[0], p2[1])
        #p_sim2 += w_sim1
        w_sim2 = get_word_sim_for_phrase(p1[1], p2[0])
        p_sim2 = min(w_sim1, w_sim2)

        ret_sim = max(p_sim1, p_sim2)
    else:
        if len(p1) == 1:
            tmp_p1 = copy.deepcopy(p1)
            tmp_p2 = copy.deepcopy(p2)
        else:
            tmp_p1 = copy.deepcopy(p2)
            tmp_p2 = copy.deepcopy(p1)

        w_sim1 = get_word_sim_for_phrase(tmp_p1[0], tmp_p2[0])
        w_sim2 = get_word_sim_for_phrase(tmp_p1[0], tmp_p2[1])

        ret_sim = min(w_sim1, w_sim2)

    # we have to normalize word sims for nasari, because it ranges in [-1, 1],
    # which will lead to trouble for spectral clustering.
    if WORD_SIM_MODE == 'nasari':
        ret_sim = (ret_sim - (-1)) / (1 - (-1))
    return ret_sim


# we keep the orig words without lowercasing them here
# what are returned from this function can contain repeated phrases
def extract_phrase_pairs(doc_comp_res_folder):
    db_conn = sqlite3.connect(data_path + data_set + '.db')
    db_cur = db_conn.cursor()
    rows = db_cur.execute("SELECT doc_id from docs order by doc_id").fetchall()
    docs = {row[0]: i for i, row in enumerate(rows)}

    l_phrase_pairs = []
    for i, doc_comp_res in enumerate(os.listdir(doc_comp_res_folder)):
        if doc_comp_res.endswith('.json'):
            with open(doc_comp_res_folder + doc_comp_res, 'r') as doc_comp_res_fd:
                doc1_name, doc2_name = doc_comp_res.replace('_', '/').replace('.json', '').split('#')
                s1_doc_idx, s2_doc_idx = docs[doc1_name], docs[doc2_name]
                sent_pair_cycles = json.load(doc_comp_res_fd)['sentence_pair']
                for sent_pair_key, cycles in sent_pair_cycles.items():
                    s1_sent_idx, s2_sent_idx = sent_pair_key.split('-')
                    for each_cyc in cycles['cycles']:
                        # extract two phrases from a cycle
                        p1 = []
                        p2 = []
                        p1_arc = 1
                        p2_arc = 1
                        p1_sent_locs = []
                        p2_sent_locs = []
                        for idx_w, each_w in enumerate(each_cyc):
                            if ':L:' in each_w:
                                if each_w[:2] == 's2':
                                    tmp_p = each_w[5:].split('#')
                                    p2_sent_locs.append(int(tmp_p[2]))
                                    if tmp_p[3].strip() in PRESERVED_NER_LIST:
                                        p2.append(tmp_p[0].strip())
                                    else:
                                        p2.append(tmp_p[0].strip().lower())
                                elif each_w[:2] == 's1':
                                    tmp_p = each_w[5:].split('#')
                                    p1_sent_locs.append(int(tmp_p[2]))
                                    if tmp_p[3].strip() in PRESERVED_NER_LIST:
                                        p1.append(tmp_p[0].strip())
                                    else:
                                        p1.append(tmp_p[0].strip().lower())
                            else:
                                if each_w[:2] == 's1':
                                    p1_arc += 1
                                elif each_w[:2] == 's2':
                                    p2_arc += 1
                        p1_sent_locs.sort()
                        p2_sent_locs.sort()
                        # collect all paired phrases
                        if len(p1) > 0 and len(p2) > 0:
                            #l_phrase_pairs.append([p1, p2, p_sim])
                            l_phrase_pairs.append([[p1, "%s-%s-%s" % (s1_doc_idx,s1_sent_idx, '-'.join(str(loc) for loc in p1_sent_locs)), p1_arc],
                                                   [p2, "%s-%s-%s" % (s2_doc_idx,s2_sent_idx, '-'.join(str(loc) for loc in p2_sent_locs)), p2_arc]])
                        else:
                            print '[ERR]: Empty phrase exists: p1 = %s, p2 = %s' % ('#'.join(p1), '#'.join(p2))
                            continue
    db_conn.close()
    return l_phrase_pairs


def get_phrase_in_graph(G, p):
    nodes = G.nodes
    #TODO
    #only for test
    # if 'ago' in p and 'back' in p:
    #     print 'here!'

    # we convert single-word phrase to a double-word phrase consisting of two idential words
    if len(p) < 2:
        node_str = p[0]+'#'+p[0]
        if node_str in nodes:
            return node_str
        else:
            return None
    else:
        if p[0]+'#'+p[1] in nodes:
            return p[0]+'#'+p[1]
        elif p[1]+'#'+p[0] in nodes:
            return p[1]+'#'+p[0]
        else:
            return None

# TODO
# fix node's data structure
# we lowercases all words before add their corresponding phrases into graph
# def create_phrase_graph(l_phrase_pairs):
#     p_graph = nx.Graph()
#     for p1, p2 in l_phrase_pairs:
#         # if the two phrases are already in the phrase graph, then ignore.
#         # otherwise, at least one new node is added to the graph, and also,
#         # a new edge is added to the graph as well. the weight of this edge
#         # is the sim between the two phrases.
#         p1_in_graph = get_phrase_in_graph(p_graph, p1[0])
#         if p1_in_graph is None:
#             if len(p1[0]) == 1:
#                 p1_in_graph = p1[0][0] + '#' + p1[0][0]
#             elif len(p1[0]) == 2:
#                 p1_in_graph = '#'.join(p1[0])
#             else:
#                 raise Exception('[ERR]: Invalid phrase length: %s' % p1[0])
#             p_graph.add_node(p1_in_graph, cnt=1, loc=[p1[1]], arc=[p1[2]])
#         else:
#             if p1[1] not in p_graph.node[p1_in_graph]['loc']:
#                 p_graph.node[p1_in_graph]['cnt'] += 1
#                 p_graph.node[p1_in_graph]['loc'].append(p1[1])
#                 p_graph.node[p1_in_graph]['arc'].append(p1[2])
#
#         p2_in_graph = get_phrase_in_graph(p_graph, p2[0])
#         if p2_in_graph is None:
#             if len(p2[0]) == 1:
#                 p2_in_graph = p2[0][0] + '#' + p2[0][0]
#             elif len(p2[0]) == 2:
#                 p2_in_graph = '#'.join(p2[0])
#             else:
#                 raise Exception('[ERR]: Invalid phrase length: %s' % p2[0])
#             p_graph.add_node(p2_in_graph, cnt=1, loc=[p2[1]], arc=[p2[2]])
#         else:
#             if p2[1] not in p_graph.node[p2_in_graph]['loc']:
#                 p_graph.node[p2_in_graph]['cnt'] += 1
#                 p_graph.node[p2_in_graph]['loc'].append(p2[1])
#                 p_graph.node[p2_in_graph]['arc'].append(p2[2])
#
#         # we do not add self-loop into the graph
#         if not p_graph.has_edge(p1_in_graph, p2_in_graph) and p1_in_graph != p2_in_graph:
#             p_sim = get_phrase_sim(p1[0], p2[0])
#             p_graph.add_edge(p1_in_graph, p2_in_graph, weight=p_sim)
#
#     for n_str, n_attrs in list(p_graph.nodes(data=True)):
#         if len(n_attrs['loc']) != len(n_attrs['arc']):
#             print "[ERR] Loc Arc not match", n_str, n_attrs
#
#     return p_graph

def create_phrase_graph(l_phrase_pairs):
    p_graph = nx.Graph()
    for p1, p2 in l_phrase_pairs:
        # if the two phrases are already in the phrase graph, then ignore.
        # otherwise, at least one new node is added to the graph, and also,
        # a new edge is added to the graph as well. the weight of this edge
        # is the sim between the two phrases.
        p1_in_graph = get_phrase_in_graph(p_graph, p1[0])
        if p1_in_graph is None:
            if len(p1[0]) == 1:
                p1_in_graph = p1[0][0] + '#' + p1[0][0]
            elif len(p1[0]) == 2:
                p1_in_graph = '#'.join(p1[0])
            else:
                raise Exception('[ERR]: Invalid phrase length: %s' % p1[0])
            p_graph.add_node(p1_in_graph, cnt=1, loc={p1[1]: [p1[2]]})
        else:
            if p1[1] not in p_graph.node[p1_in_graph]['loc'].keys():
                p_graph.node[p1_in_graph]['cnt'] += 1
                p_graph.node[p1_in_graph]['loc'][p1[1]] = [p1[2]]
            else:
                p_graph.node[p1_in_graph]['cnt'] += 1
                p_graph.node[p1_in_graph]['loc'][p1[1]].append(p1[2])

        p2_in_graph = get_phrase_in_graph(p_graph, p2[0])
        if p2_in_graph is None:
            if len(p2[0]) == 1:
                p2_in_graph = p2[0][0] + '#' + p2[0][0]
            elif len(p2[0]) == 2:
                p2_in_graph = '#'.join(p2[0])
            else:
                raise Exception('[ERR]: Invalid phrase length: %s' % p2[0])
            p_graph.add_node(p2_in_graph, cnt=1, loc={p2[1]: [p2[2]]})
        else:
            if p2[1] not in p_graph.node[p2_in_graph]['loc'].keys():
                p_graph.node[p2_in_graph]['cnt'] += 1
                p_graph.node[p2_in_graph]['loc'][p2[1]] = [p2[2]]
            else:
                p_graph.node[p2_in_graph]['cnt'] += 1
                p_graph.node[p2_in_graph]['loc'][p2[1]].append(p2[2])

        if not p_graph.has_edge(p1_in_graph, p2_in_graph) and p1_in_graph != p2_in_graph:
            p_sim = get_phrase_sim(p1[0], p2[0])
            p_graph.add_edge(p1_in_graph, p2_in_graph, weight=p_sim)

    # for n_str, n_attrs in list(p_graph.nodes(data=True)):
    #     sum_arcs = 0
    #     for arcs in n_attrs['loc'].values():
    #         sum_arcs += len(arcs)
    #     if n_attrs['cnt'] != sum_arcs:
    #         raise Exception("[ERR] Loc Arc not match: %s, %s." % (n_str, n_attrs))

    return p_graph
