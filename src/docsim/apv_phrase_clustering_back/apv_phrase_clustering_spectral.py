import networkx as nx
import os
import json
#import numpy as np
from sklearn.cluster import SpectralClustering
import math
import time
import idx_bit_translate
import sqlite3
import copy
# from scipy.sparse.csgraph import connected_components

data_path = '%s/workspace/data/' % os.environ['HOME']
dataset = ''
run_name = ''
uf_dump_suffix = '_ufclusters'
rbsc_dump_suffix = '_rbsc'
PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']
WORD_SIM_MODE = 'nasari'
MAX_DIAMETER = 2

##########################################################################################
# Phrase Extraction for Training Set
##########################################################################################
#def get_phrase_sim(w_sim1, w_sim2, p1_tag_count, p2_tag_count):
#    #sim = math.exp(3.0 / (math.pow(p1_tag_count+1, 3.0) + math.pow(p2_tag_count+1, 3.0))) * math.exp(min(w_sim1, w_sim2))
#    sim = math.exp(2.0/(1/w_sim1 + 1/w_sim2))
#    return sim


def get_word_sim_for_phrase(db_cur, w1, w2):
    if w1 is None or w2 is None:
        raise Exception('[ERR]: iIvalid word input!')
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
        return w_sim
    elif w1_idx < w2_idx:
        word_pair_id = idx_bit_translate.keys_to_key(w1_idx, w2_idx)
    else:
        word_pair_id = idx_bit_translate.keys_to_key(w2_idx, w1_idx)
    db_cur.execute('SELECT sim from words_sim where word_pair_idx = ?', (word_pair_id,))
    w_sim = db_cur.fetchone()[0]
    if w_sim is None:
        raise Exception('[ERR]: Cannot find sim for (%s, %s)' % (w1, w2))
    return w_sim


def get_phrase_sim(p1, p2):
    # TODO
    # test only
    #return 1.0
    global dataset
    db_path = data_path + dataset + '.db'
    db_conn = sqlite3.connect(db_path)
    db_cur = db_conn.cursor()
    ret_sim = 0.0

    if len(p1) == 0 or len(p2) == 0 or len(p1) + len(p2) > 4 or len(p1) + len(p2) < 3:
        print p1
        print p2
        raise Exception('[ERR]: p1, p2 are not valid:')
    if len(p1) == 2 and len(p2) == 2:
        # case 1: p1[0]-p2[0], p1[1]-p2[1]
        p_sim1 = 0
        w_sim1 = get_word_sim_for_phrase(db_cur, p1[0], p2[0])
        #p_sim1 += w_sim1
        w_sim2 = get_word_sim_for_phrase(db_cur, p1[1], p2[1])
        p_sim1 = min(w_sim1, w_sim2)

        # case 2: p1[0]-p2[1], p1[1]-p2[0]
        p_sim2 = 0
        w_sim1 = get_word_sim_for_phrase(db_cur, p1[0], p2[1])
        #p_sim2 += w_sim1
        w_sim2 = get_word_sim_for_phrase(db_cur, p1[1], p2[0])
        p_sim2 = min(w_sim1, w_sim2)

        ret_sim = max(p_sim1, p_sim2)
    else:
        if len(p1) == 1:
            tmp_p1 = copy.deepcopy(p1)
            tmp_p2 = copy.deepcopy(p2)
        else:
            tmp_p1 = copy.deepcopy(p2)
            tmp_p2 = copy.deepcopy(p1)

        w_sim1 = get_word_sim_for_phrase(db_cur, tmp_p1[0], tmp_p2[0])
        w_sim2 = get_word_sim_for_phrase(db_cur, tmp_p1[0], tmp_p2[1])

        ret_sim = min(w_sim1, w_sim2)

    db_conn.close()
    # we have to normalize word sims for nasari, because it ranges in [-1, 1],
    # which will lead to trouble for spectral clustering.
    if WORD_SIM_MODE == 'nasari':
        ret_sim = (ret_sim - (-1)) / (1 - (-1))
    return ret_sim


def get_word_sim(w1, w2):
    sim = 1.0
    return sim


# we keep the orig words without lowercasing them here
def extract_phrase_pairs(doc_comp_res_folder):

    db_conn = sqlite3.connect(data_path + dataset + '.db')
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
    return l_phrase_pairs


##########################################################################################
# Union Find Data Structure
##########################################################################################
class myNode(object):
    def __init__(self, p):
        self.parent = None
        self.rank = None
        self.cid = None
        self.pstr = p


class UnionFind(object):
    def __init__(self):
        # To hold the clusters
        self.clusters = []

    # create a new set(cluster) with a node
    def makeSet(self, node):
        # set the nodes parent to the node itself
        node.parent = node
        # set initial rank of node to 0
        node.rank = 0
        # add the node to cluster list
        self.clusters.append(node)

    # union the nodeA and nodeB clusters
    def union(self, nodeA, nodeB):

        self.link(self.findSet(nodeA), self.findSet(nodeB))

    # link the nodeA to nodeB or vice versa based upon the rank(number of nodes in the cluster) of the cluster
    def link(self, nodeA, nodeB):

        if nodeA.rank > nodeB.rank:
            nodeB.parent = nodeA
            # remove the nodeB from the cluster list, since it is merged with nodeA
            self.clusters.remove(nodeB)
        else:
            nodeA.parent = nodeB
            # remove the nodeA from the cluster list, since it is merged with nodeB
            self.clusters.remove(nodeA)
            # increase the rank of the cluster after merging the cluster
            if nodeA.rank == nodeB.rank:
                nodeB.rank = nodeB.rank + 1

    # find set will path compress(makes the nodes in cluster points to single leader/parent)
    # and returns the leader/parent of the cluster
    def findSet(self, node):

        if node != node.parent:
            node.parent = self.findSet(node.parent)
        return node.parent

    # get cluster size
    def clusterSize(self):
        return len(self.clusters)


##########################################################################################
# Union Find Clustering
##########################################################################################
def is_in_cluster_keys(phrase_list, cluster_keys):
    # Find if a key exist in the cluster with a phrase list, say ['A', 'B']
    # Return False if not found
    if len(phrase_list) == 2:
        if (phrase_list[0]+"#"+phrase_list[1]) in cluster_keys:
            return True
        elif (phrase_list[1]+"#"+phrase_list[0]) in cluster_keys:
            return True
    elif len(phrase_list) == 1:
        if phrase_list[0] in cluster_keys:
            return True
    return False


def get_node_by_phrase_list(phrase_list, cluster):
    # Find a node with a phrase list, say ['A', 'B']
    # Raise error if none is found,
    # because when calling the function, the key has already been checked in the previous step
    if len(phrase_list) == 2:
        try:
            return cluster[phrase_list[0]+"#"+phrase_list[1]][0]
        except:
            try:
                return cluster[phrase_list[1]+"#"+phrase_list[0]][0]
            except:
                raise Exception("%s not found in cluster keys." % phrase_list)
    elif len(phrase_list) == 1:
        try:
            return cluster[phrase_list[0]][0]
        except:
            raise Exception("%s not found in cluster keys." % phrase_list)


def union_find_clustering(l_phrase_pairs):
    uf = UnionFind()
    cluster = dict()
    max_cluster_id = 0

    for p_pair in l_phrase_pairs:
        ps1 = p_pair[0]
        ps2 = p_pair[1]
        if not is_in_cluster_keys(ps1, cluster.keys()):
            if not is_in_cluster_keys(ps2, cluster.keys()):
                ps1_node = myNode(ps1)
                ps2_node = myNode(ps2)
                uf.makeSet(ps1_node)
                uf.makeSet(ps2_node)
                uf.union(ps1_node, ps2_node)
                ps1_node.cid = max_cluster_id
                ps2_node.cid = max_cluster_id
                cluster['#'.join(ps1)] = (ps1_node.parent, ps1_node.cid)
                cluster['#'.join(ps2)] = (ps2_node.parent, ps2_node.cid)
                max_cluster_id += 1
            else:
                ps1_node = myNode(ps1)
                ps2_node = get_node_by_phrase_list(ps2, cluster)
                ps2_root = uf.findSet(ps2_node)
                uf.makeSet(ps1_node)
                uf.union(ps1_node, ps2_root)
                ps1_node.cid = ps2_root.cid
                cluster['#'.join(ps1)] = (uf.findSet(ps1_node), ps1_node.cid)
        else:
            if not is_in_cluster_keys(ps2, cluster.keys()):
                ps2_node = myNode(ps2)
                ps1_node = get_node_by_phrase_list(ps1, cluster)
                ps1_root = uf.findSet(ps1_node)
                uf.makeSet(ps2_node)
                uf.union(ps1_root, ps2_node)
                ps2_node.cid = ps1_root.cid
                cluster['#'.join(ps2)] = (uf.findSet(ps2_node), ps2_node.cid)
            else:
                ps1_node = get_node_by_phrase_list(ps1, cluster)
                ps2_node = get_node_by_phrase_list(ps2, cluster)
                ps1_root = uf.findSet(ps1_node)
                ps2_root = uf.findSet(ps2_node)
                if ps1_root == ps2_root:
                    continue
                uf.union(ps1_root, ps2_root)
    for p_i in cluster.keys():
        p_i_root = uf.findSet(cluster[p_i][0])
        new_cid = p_i_root.cid
        cluster[p_i] = new_cid
    return cluster


def dump_uf_clusters(d_clusters):
    with open(data_path + dataset + uf_dump_suffix, 'w+') as dump_fd:
        json.dump(d_clusters, dump_fd, indent=4)
    dump_fd.close()


##########################################################################################
# Recursive Spectrl Clustering
##########################################################################################
def load_uf_clusters():
    with open(data_path + dataset + uf_dump_suffix, 'r') as dump_fd:
        uf_p_cluster = json.load(dump_fd)

    d_ud_clusters = dict()
    for p_i in uf_p_cluster.items():
        if p_i[1] not in d_ud_clusters.keys():
            d_ud_clusters[p_i[1]] = [p_i[0].split('#')]
        else:
            d_ud_clusters[p_i[1]].append(p_i[0].split('#'))
    with open(data_path + dataset + uf_dump_suffix + '_by_cid', 'w+') as dump_fd:
        json.dump(d_ud_clusters, dump_fd, indent=4)
    return d_ud_clusters


def get_connected_components(l_phrase_pairs):
    full_p_graph = create_phrase_graph(l_phrase_pairs)
    l_comp_graphs = [full_p_graph.subgraph(comp) for comp in nx.connected_components(full_p_graph)]
    return l_comp_graphs


def get_phrase_in_graph(G, p):
    nodes = G.nodes
    if len(p) < 2:
        if p[0] in nodes:
            return p[0]
        else:
            return None
    else:
        if p[0]+'#'+p[1] in nodes:
            return p[0]+'#'+p[1]
        elif p[1]+'#'+p[0] in nodes:
            return p[1]+'#'+p[0]
        else:
            return None


# we lowercases all words before add their corresponding phrases into graph
def create_phrase_graph(l_phrase_pairs):
    p_graph = nx.Graph() 
    for p1, p2 in l_phrase_pairs:
        # if the two phrases are already in the phrase graph, then ignore.
        # otherwise, at least one new node is added to the graph, and also,
        # a new edge is added to the graph as well. the weight of this edge
        # is the sim between the two phrases.
        p1_in_graph = get_phrase_in_graph(p_graph, p1[0])
        if p1_in_graph is None:
            p1_in_graph = '#'.join(p1[0])
            # p_graph.add_node(p1_in_graph, cnt=1, loc=[p1[1]], arc=[p1[2]])
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
            p2_in_graph = '#'.join(p2[0])
            # p_graph.add_node(p2_in_graph, cnt=1, loc=[p2[1]], arc=[p2[2]])
            p_graph.add_node(p2_in_graph, cnt=1, loc={p2[1]: [p2[2]]})
        else:
            if p2[1] not in p_graph.node[p2_in_graph]['loc'].keys():
                p_graph.node[p2_in_graph]['cnt'] += 1
                p_graph.node[p2_in_graph]['loc'][p2[1]] = [p2[2]]
                # p_graph.node[p2_in_graph]['arc'].append(p2[2])
            else:
                p_graph.node[p2_in_graph]['cnt'] += 1
                p_graph.node[p2_in_graph]['loc'][p2[1]].append(p2[2])

        if not p_graph.has_edge(p1_in_graph, p2_in_graph):
            p_sim = get_phrase_sim(p1[0], p2[0])
            p_graph.add_edge(p1_in_graph, p2_in_graph, weight=p_sim)

    for n_str, n_attrs in list(p_graph.nodes(data=True)):
        # if len(n_attrs['loc']) != len(n_attrs['arc']):
        #     print "[ERR] Loc Arc not match", n_str, n_attrs
        sum_arcs = 0
        for loc, arcs in n_attrs.values().items():
            sum_arcs += len(arcs)
        if n_attrs['cnt'] != sum_arcs:
            print "[ERR] Loc Arc not match", n_str, n_attrs

    return p_graph


def build_aff_mat(p_graph):
    # the rows and cols are ordered according to the ordering produced by G.nodes()
    adj_mat = nx.adjacency_matrix(p_graph)
    #print '[DBG]: adj_mat = '
    #print adj_mat
    #second_adj_mat = np.dot(adj_mat, adj_mat)
    #print '[DBG]: 2nd_adj_mat = '
    #print second_adj_mat
    #third_adj_mat = np.dot(second_adj_mat, adj_mat)
    #print '[DBG]: 3rd_adj_mat = '
    #print third_adj_mat
    #adj_mat = adj_mat.todense()
    #second_adj_mat = second_adj_mat.todense()
    #third_adj_mat = third_adj_mat.todense()
    #np.fill_diagonal(second_adj_mat, 0.0)
    #np.fill_diagonal(third_adj_mat, 0.0)
    #return adj_mat + second_adj_mat + third_adj_mat
    return adj_mat


def phrase_clustering(p_graph, aff_mat, init_n_clusters, max_diameter):
    print '[DBG]: enter phrase_clustering: p_graph: '
    print nx.info(p_graph)
    #c_comps, comp_labels = connected_components(aff_mat)
    if not nx.is_connected(p_graph):
        raise Exception('[ERR]: p_graph is not connected!')
    d_clusters = dict()
    if p_graph.number_of_nodes() <= 200 and nx.diameter(p_graph) <= max_diameter:
        if list(p_graph.nodes)[0] in d_clusters.keys():
            raise Exception('[ERR]: Overlapping clusters emerge!')
        d_clusters[list(p_graph.nodes)[0]] = p_graph
        print '[DBG]: a good cluster is done!'
        print p_graph.nodes
        return d_clusters

    p_labels = SpectralClustering(n_clusters=init_n_clusters, affinity='precomputed', n_jobs=-1).fit_predict(aff_mat)
    cand_clusters = dict()
    nodes = list(p_graph.nodes)
    for id, label in enumerate(p_labels):
        if label not in cand_clusters.keys():
            cand_clusters[label] = [nodes[id]]
        else:
            cand_clusters[label].append(nodes[id])

    # keep doing clustering for each cluster if this cluster's diameter is greater than max_diameter
    for c in cand_clusters.keys():
        sub_graph_c = p_graph.subgraph(cand_clusters[c])
        if not nx.is_connected(sub_graph_c):
            print '[WRN]: Disconnected resulting clusters emerge!'
            #print nx.info(sub_graph_c)
            #raise Exception('[ERR]: sub_graph_c is not connected!')
        l_sub_graph_comp = [sub_graph_c.subgraph(comp) for comp in nx.connected_components(sub_graph_c)]
        for sub_graph_comp_id, sub_graph_comp in enumerate(l_sub_graph_comp):
            if len(sub_graph_comp.nodes) <= 0:
                continue
            sub_graph_aff_mat = build_aff_mat(sub_graph_comp)
            print '[DBG]: recursive phrase_clustering...'
            d_c_clusters = phrase_clustering(sub_graph_comp, sub_graph_aff_mat, 2, max_diameter)
            d_clusters = d_clusters.copy()
            d_clusters.update(d_c_clusters)

    return d_clusters


def verify_phrase_clusters(p_clusters, max_diameter):
    good_mark = True
    for c in p_clusters.keys():
        if not isinstance(p_clusters[c], nx.Graph):
            raise Exception('[ERR]: Wrong cluster emerges!')
        sub_graph_c = p_clusters[c]
        if nx.diameter(sub_graph_c) > max_diameter:
            print '[DBG]: cluster violates max_diameter condition!'
            print p_clusters[c]
            good_mark = False
    if good_mark:
        print '[DBG]: the phrase clusters are fine!'


def recursive_bi_spectral_clustering(l_phrase_pairs):
    d_final_clusters = dict()
    l_comp_graphs = get_connected_components(l_phrase_pairs)
    total_comp = len(l_comp_graphs)
    print '[DBG]: components size = %s' % total_comp
    print [len(c) for c in sorted(l_comp_graphs, key=len, reverse=True)]
    start = time.time()
    for comp_id, comp_graph in enumerate(l_comp_graphs):
        if len(comp_graph.nodes) <= 0:
            continue
        # we use the first phrase as the key of a cluster
        aff_mat = build_aff_mat(comp_graph)
        comp_graph_avg_deg = math.floor(sum(dict(comp_graph.degree()).values())/comp_graph.number_of_nodes())
        pred_c_size = 1
        for i in range(MAX_DIAMETER):
            pred_c_size += math.pow(comp_graph_avg_deg, i+1)
        #pred_c_size = 1 + comp_graph_avg_deg + math.pow(comp_graph_avg_deg, 2) + math.pow(comp_graph_avg_deg, 3)
        #n_init_clusters = int(math.ceil(comp_graph.number_of_nodes()/float(pred_c_size)))
        n_init_clusters = 2
        d_ret_clusters = phrase_clustering(comp_graph, aff_mat, n_init_clusters, MAX_DIAMETER)
        d_final_clusters = d_final_clusters.copy()
        d_final_clusters.update(d_ret_clusters)
        print '[DBG]: %s rbsc is done in %s secs.' % (float(comp_id+1)/total_comp, time.time()-start)
    if not verify_phrase_clusters(d_final_clusters, MAX_DIAMETER):
        print '[ERR]: Resulting clusters are not valid!'
    return d_final_clusters


# input is a set of subgraphs
def dump_rbsc_clusters(d_clusters):
    d_dump = dict()
    for i, c in enumerate(d_clusters):
        d_dump[i] = list(d_clusters[c].nodes(data=True))

    with open(data_path + dataset + preffix + '_diam2_phrase_clusters_by_clusterid.json', 'w+') as dump_fd:
        json.dump(d_dump, dump_fd, indent=4)
    dump_fd.close()

    #indata = json.load(open("/home/fcmeng/workspace/data/20news50short10_rbsc_cluster.json", 'r'))
    # cluster_by_phrase = dict()
    # cluster_id = 0
    # for k, v in d_dump.items():
    #     for each_phrase in v:
    #         if each_phrase not in cluster_by_phrase.keys():
    #             cluster_by_phrase[each_phrase] = cluster_id
    #         else:
    #             print "[ERR] Phrase %s:%s already in the cluster. Duplicated phrase!!" % \
    #                   (each_phrase, cluster_by_phrase[each_phrase])
    #     cluster_id += 1
    #
    # with open(data_path + dataset + preffix + "_phrase_clusters_by_phrase.json", "w") as outfile:
    #     json.dump(cluster_by_phrase, outfile, indent=4)


##########################################################################################
# Main
##########################################################################################
def main(folder):
    global dataset, preffix
    dataset = folder[:folder.find('_')]
    preffix = folder[folder.find('_')+1:]
    l_phrase_pairs = extract_phrase_pairs(data_path + folder + '/')
    print '[INF]: phrase extraction is done!'
    #print l_phrase_pairs
    #p_clusters = union_find_clustering(l_phrase_pairs)
    #print '[INF]: union find clustering is done!'
    #print d_clusters
    #dump_uf_clusters(p_clusters)
    #print '[INF]: union find clustering dump is done!'
    #load_uf_clusters()
    d_clusters = recursive_bi_spectral_clustering(l_phrase_pairs)
    print '[INF]: recursive bi-spectral-clustering is done!'
    print d_clusters
    dump_rbsc_clusters(d_clusters)
    print '[INF]: recursive bi-spectral-clustering dump is done!'


# We found that the same phrase in the same sentence and same location has different arcs
# This program saves phrases each arc from each cycle
main('20news50short10_nasari_40_rmswcbwexpws_w3-3')
