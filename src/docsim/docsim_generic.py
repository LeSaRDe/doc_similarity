import nltk
from nltk.tree import Tree, ParentedTree
# from nltk.tokenize import sent_tokenize
# from nltk.parse import corenlp
from gensim.models import KeyedVectors
import socket
import networkx as nx
# import matplotlib.pyplot as plt
import math
import multiprocessing
import threading
# import ctypes
import sqlite3
# import random
# import sys
import re
import os
import json
import time
import numpy
import logging
import sys
from itertools import combinations

sys.path.insert(1, '/home/mf3jh/workspace/lib/lexvec/lexvec/python/lexvec/')
import model as lexvec
import scipy.spatial.distance as scipyd
# from itertools import combinations
from scipy import special

LEXVEC_MODEL_PATH = '%s/workspace/lib/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W+C.pos.vectors'
g_lexvec_model = None

NODE_ID_COUNTER = 0
WORD_SIM_THRESHOLD_ADW = 0.5
WORD_SIM_THRESHOLD_NASARI = 0.5
WORD_SIM_THRESHOLD_NASARI_N = 0.5
WORD_SIM_THRESHOLD_LEXVEC = 0.5
WORD_SIM_THRESHOLD_LEXVEC_N = 0.5
SEND_PORT_ADW = 8606
SEND_PORT_NASARI = 8306
SEND_ADDR_ADW = 'localhost'
SEND_ADDR_NASARI = 'localhost'
MULTI_WS_SERV = False
MULTI_WS_SERV_MOD = 2
RAND_SEED = 0
# the other option is 'adw'
# WORD_SIM_MODE = 'nasari'
WORD_SIM_MODE = 'lexvec'
# WORD_SIM_MODE = 'adw'
# WORD_SIM_MODE = 'adw_tag'
# WORD_SIM_MODE = 'adw_offline'
# DB_CONN_STR = '/home/{0}/workspace/data/docsim/20news50short10.db'.format(os.environ['USER'])
DB_CONN_STR = '/home/{0}/workspace/data/docsim/bbc.db'.format(os.environ['USER'])
# DB_CONN_STR = '/home/{0}/workspace/data/docsim/leefixsw.db'.format(os.environ['USER'])
# used for lee vs leebg
BGDB_CONN_STR = '/home/{0}/workspace/data/leebgfixsw.db'.format(os.environ['USER'])
# used for Li65 sentence comparison
LI65_COL1_CONN_STR = '/home/{0}/workspace/data/Li65/col1_nosw.db'.format(os.environ['USER'])
LI65_COL2_CONN_STR = '/home/{0}/workspace/data/Li65/col2_nosw.db'.format(os.environ['USER'])
# used for STS2017 sentence comparison
STS_COL1_CONN_STR = '/home/{0}/workspace/data/sts2017/col1.db'.format(os.environ['USER'])
STS_COL2_CONN_STR = '/home/{0}/workspace/data/sts2017/col2.db'.format(os.environ['USER'])
# used for SICK sentence comparison
SICK_COL1_CONN_STR = '/home/{0}/workspace/data/docsim/sick/col1.db'.format(os.environ['USER'])
SICK_COL2_CONN_STR = '/home/{0}/workspace/data/docsim/sick/col2.db'.format(os.environ['USER'])
# rmsw = remove stopwords
# cbw = use cb_weight to set inter_edges' weights to very high when computing cycle basis
# expws = exponentiate word similarities
# cycdbg = output cycle dbg info to intermediate files
# cycrbf = use rbf to compute significance of cycles
# n[40] = use noun threshold 0.4 individually
# cycdist = use cycle distribution penalty
# expn = exponentiate noun similarities
# wo = weighted overlap simialrity algorithm
# scyc = use simple cycles instead of min cycle basis
# nosig = only use word sim
# noner = no ner filtering
# ssum = sum sim
# lem = use lemma instead of word for word sim
# pcomb = use phrase pairs to compute similarity weights
# cycw = use harmonic mean cycle weights
# test = only for test use
# sigm = use sigmoid to weights
OUT_CYCLE_FILE_PATH = '/home/{0}/workspace/data/docsim/bbc_lexvec_50_rmswcbwexpwspcomb_w3-3/'.format(os.environ['USER'])
# CYC_SIG_PARAM 1 and 2 are used by exp(param1/(w1^param2 + w2^param2))
CYC_SIG_PARAM_1 = 3.0
CYC_SIG_PARAM_2 = 3.0
# for cycle distribution penalty
CYC_DIST = False
# exponentiate noun similarities
EXP_NOUN = False
# use rbf like method to compute cycle significance
# CYC_SIG_PARAM_3 and 4 are used in this case
CYC_RBF = False
# CYC_SIG_PARAM 3 and 4 are used by exp(- (w1-param3)^2 / param4)
CYC_SIG_PARAM_3 = 1.0
CYC_SIG_PARAM_4 = 20.0
# sum two word sims when computing sim of a cycle
SUM_SIM = False
# get word sim by lemma
LEMMA_SIM = False
# MAX_PROC = multiprocessing.cpu_count()
# PROC_BATCH_SIZE = multiprocessing.cpu_count()
MAX_PROC = multiprocessing.cpu_count()
PROC_BATCH_SIZE = 10
# use simple cycles instead of min cycle basis
SIMPLE_CYCLES = False
# swtich for lee and 20news and reuters and bbc
LEE = True
# switch for lee vs leebg
LEE_VS_LEEBG = False
# switch for Li65 and STS and SICK
LI65 = False
# swthich for msr
MSR = False
MSR_SENT_PAIR_FILE = '/home/{0}/workspace/data/msr/msr_sim.txt'.format(os.environ['USER'])

SAVE_CYCLES = True

PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']
# PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC', 'PERSON']

OFFLINE_DICT = '/home/{0}/workspace/doc_similarity/res/ws_offline_dict'.format(os.environ['USER'])
doc_pair_dict = {}


# for ADW Lee
def fillDocPairDict(doc1, doc2):
    global doc_pair_dict
    babel_file = open(OFFLINE_DICT, 'r')
    babel_ADW = {}
    for line in babel_file:
        ba = line.split(' : ')
        babel_ADW[ba[0].strip()] = ba[1].split()
    doc1_int = int(doc1) + 1
    doc2_int = int(doc2) + 1
    if doc1_int < doc2_int:
        dpd_tmp = babel_ADW['(%s,%s)' % (doc1_int, doc2_int)]
    else:
        dpd_tmp = babel_ADW['(%s,%s)' % (doc2_int, doc1_int)]
    for wp in dpd_tmp:
        wp = wp.split(':')
        doc_pair_dict[wp[0]] = float(wp[1])


# this function takes a tree string and returns the graph of this tree
# the format of the input tree string needs follow the CoreNLP Tree def.
# this format is compatible with NLTK Tree.
# the graph is introduced from NetworkX.
# leaf format: [s_i]:L:[word]#[synset_1]+...+[synset_2]#[token_idx]#[ner]#[pos]#[lemma]:[uni_idx]
def treestr_to_graph(treestr, id):
    ret_graph = nx.Graph()
    ret_digraph = nx.DiGraph()
    tree = Tree.fromstring(treestr)
    # checkTree(tree, '0')
    global NODE_ID_COUNTER
    identifyNodes(tree, id)
    tree.set_label(id + ':' + tree.label() + ':' + str(NODE_ID_COUNTER))
    tree_prod = tree.productions()
    # print tree_prod
    for i, p in enumerate(tree_prod):
        p = str(p).split('->')
        p[1] = p[1].split()
        start = p[0]
        start = start.strip()
        ret_graph.add_node(start, type='node')
        for edge_e in p[1]:
            end = edge_e.replace("'", "")
            end = end.strip()
            if end[3:5] == "L:":
                word_n_tags = end[5:].split(":")[0].split('#')
                if word_n_tags[1] == "":
                    offset_tags = []
                else:
                    offset_tags = word_n_tags[1].split('+')
                token_index = word_n_tags[2].strip()
                ret_graph.add_node(end, type='leaf', tags=offset_tags, idx=token_index)
            else:
                ret_graph.add_node(end, type='node')
            ret_graph.add_edge(start, end.strip(), weight=1, type='intra', cb_weight=1)
            ret_digraph.add_edge(start, end.strip(), weight=1, type='intra', cb_weight=1)
    # print "tree_str_to_graph:"
    # print ret_graph.nodes
    return ret_graph, ret_digraph


def checkTree(tree, id):
    for index, subtree in enumerate(tree):
        subtree_id = id + ":" + str(index)
        # print "subtree:" + subtree_id
        # print subtree
        if isinstance(subtree, ParentedTree):
            checkTree(subtree, subtree_id)


def identifyNodes(t, idx):
    global NODE_ID_COUNTER
    # print "identifyNodes: " + idx
    for index, subtree in enumerate(t):
        # print "find a type: " + str(type(subtree))
        if isinstance(subtree, Tree):
            NODE_ID_COUNTER += 1
            subtree.set_label(idx + ':' + subtree.label() + ':' + str(NODE_ID_COUNTER))
            identifyNodes(subtree, idx)
        # elif isinstance(subtree, str):
        else:
            newVal = idx + ':' + subtree + ':' + str(NODE_ID_COUNTER)
            t[index] = newVal
        NODE_ID_COUNTER += 1


# given a leaf in a parse tree, this function returns True
# if this leaf has a significant NER tag, such as ORGANIZATION,
# LOCATION and MISC; otherwise returns False.
# def has_preserved_ner(leaf_str):
#    tags = leaf_str.split(':')[2]
#    ner = tags.split('#')[3].strip()
#    if ner in PRESERVED_NER_LIST:
#        return True
#    return False

# given a leaf in a parse tree, this function returns
# the lowercased word (word itselft without any tags)
# if the word is not of any preserved NER; otherwise
# returns the original word without the lowercase convertion.
def lowercase_word_by_ner(leaf_str):
    tags = leaf_str.split(':')[2]
    ner = tags.split('#')[3].strip()
    word = tags.split('#')[0].strip()
    if ner in PRESERVED_NER_LIST:
        return word
    else:
        return word.lower()


def send_wordsim_request(mode, input_1, input_2):
    global SEND_PORT_ADW
    global SEND_PORT_NASARI
    global SEND_ADDR_ADW
    global SEND_ADDR_NASARI
    global RAND_SEED
    global MULTI_WS_SERV_MOD
    global MULTI_WS_SERV

    c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    attemp = 0
    if mode == 'oo':
        synset_1_str = '+'.join(input_1)
        synset_2_str = '+'.join(input_2)
        send_str = mode + '#' + synset_1_str + '#' + synset_2_str
        send_port = SEND_PORT_ADW
        send_addr = SEND_ADDR_ADW
    elif mode == 'ww':
        # input_1 and input_2 are the two words here
        send_str = input_1 + '#' + input_2
        send_port = SEND_PORT_NASARI
        send_addr = SEND_ADDR_NASARI
        if MULTI_WS_SERV:
            millis = int(round(time.time() * 1000))
            millis &= 0xffffffffL
            # RAND_SEED += 1
            # RAND_SEED = int(RAND_SEED)
            # RAND_SEED &= 0xffffffffL
            numpy.random.seed(millis)
            send_port += numpy.random.randint(MULTI_WS_SERV_MOD)
            # print "[DBG]: connecting port %s" % send_port
    elif mode == 'tt':
        send_str = mode + '#' + input_1 + '#' + input_2
        send_port = SEND_PORT_ADW
        if MULTI_WS_SERV:
            millis = int(round(time.time() * 1000))
            millis &= 0xffffffffL
            numpy.random.seed(millis)
            send_port += numpy.random.randint(MULTI_WS_SERV_MOD)
        send_addr = SEND_ADDR_ADW
    else:
        raise Exception('[ERR]: Unsupported word comparison mode!')

    # while attemp < 10:
    #    try:
    #        c_sock.bind((socket.gethostname(), recv_port))
    #    except socket.error, msg:
    #        print "[ERR]: bind error. " + "port:" + str(recv_port) + " is in use."
    #        print msg
    #        recv_port += 33
    #        recv_port = recv_port % 50000
    #        if recv_port < 2001:
    #            recv_port += 2001
    #        attemp += 1
    c_sock.sendto(send_str, (send_addr, send_port))
    # attemp = 0
    while True:
        try:
            c_sock.settimeout(1.0)
            ret_str, serv_addr = c_sock.recvfrom(4096)
            ret = float(ret_str)
            # print "[DBG]: send_word_sim_request:" + str(ret)
            c_sock.close()
            return ret
        # except socket.error, msg:
        except Exception as e:
            logging.error('Cannot get word similarity! Resend!')
            logging.error(e.message)
            # print "[ERR]: Cannot get word similarity! Resend!"
            # print e.message
            c_sock.close()
            c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            c_sock.sendto(send_str, (send_addr, send_port))
            # time.sleep(random.randint(1, 6))
            attemp += 1
    # c_sock.close()
    # return ret


def lexvec_word_sim(word_1, word_2):
    if g_lexvec_model is None:
        raise Exception('LexVec model is None!')
    try:
        vect_1 = g_lexvec_model.wv[word_1.lower()]
        vect_2 = g_lexvec_model.wv[word_2.lower()]
        sim = 1.0 - scipyd.cosine(vect_1, vect_2)
    except:
        sim = 0.0
    return sim


# this function finds all edges between two parsing trees w.r.t. two sentenses.
# an edge will be created only when its weight is greater than a threshold.
# 'tree_1' and 'tree_2' are two parsing trees.
# what is returned is a collection of edges.
def find_inter_edges(tree_1, tree_2):
    edges = []
    leaves_1 = filter(lambda (f, d): d['type'] == 'leaf', tree_1.nodes(data=True))
    leaves_2 = filter(lambda (f, d): d['type'] == 'leaf', tree_2.nodes(data=True))
    for leaf_1 in leaves_1:
        # print "[DBG]: leaf_1 str = " + leaf_1[0]
        synset_1 = leaf_1[1]['tags']
        word_1 = lowercase_word_by_ner(leaf_1[0])
        # word_1 = leaf_1[0].split(':')[2].split('#')[0].strip()
        for leaf_2 in leaves_2:
            sim = float(0)
            synset_2 = leaf_2[1]['tags']
            word_2 = lowercase_word_by_ner(leaf_2[0])
            # word_2 = leaf_2[0].split(':')[2].split('#')[0].strip()
            if WORD_SIM_MODE == 'adw':
                if len(synset_1) > 0 and len(synset_2) > 0:
                    sim = send_wordsim_request('oo', synset_1, synset_2)
                if sim == float(0):
                    if word_1 == word_2:
                        sim = 1
                if sim >= WORD_SIM_THRESHOLD_ADW:
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
            elif WORD_SIM_MODE == 'adw_tag':
                # we use Java ADW word similarity server in this case
                # it can compare any two words with one of the following tags
                # the two words need not be of the same pos tag
                pos_set = ['n', 'v', 'r', 'a']
                tags_1 = leaf_1[0].split(':')[2]
                pos_1 = tags_1.split('#')[4].strip()
                lemma_1 = tags_1.split('#')[5].strip()
                if pos_1[0].lower() in pos_set:
                    pos_1 = pos_1[0].lower()
                    word_1 = lemma_1
                else:
                    pos_1 = None
                tags_2 = leaf_2[0].split(':')[2]
                pos_2 = tags_2.split('#')[4].strip()
                lemma_2 = tags_2.split('#')[5].strip()
                if pos_2[0].lower() in pos_set:
                    pos_2 = pos_2[0].lower()
                    word_2 = lemma_2
                else:
                    pos_2 = None
                if word_1 == word_2:
                    sim = 1.0
                elif pos_1 is not None and pos_2 is not None:
                    sim = send_wordsim_request('tt', word_1 + ':' + pos_1, word_2 + ':' + pos_2)
                else:
                    sim = 0.0
                if sim >= WORD_SIM_THRESHOLD_ADW:
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
            elif WORD_SIM_MODE == 'adw_offline':
                tags_1 = leaf_1[0].split(':')[2]
                pos_1 = tags_1.split('#')[4].strip()
                lemma_1 = tags_1.split('#')[5].strip()
                index_1 = tags_1.split('#')[2].strip()
                tags_2 = leaf_2[0].split(':')[2]
                pos_2 = tags_2.split('#')[4].strip()
                lemma_2 = tags_2.split('#')[5].strip()
                index_2 = tags_2.split('#')[2].strip()
                if word_1 == word_2:
                    sim = 1.0
                else:
                    ws_key = word_1 + '_' + index_1 + ',' + word_2 + '_' + index_2
                    try:
                        sim = doc_pair_dict[ws_key]
                    except:
                        sim = float(0)
                if EXP_NOUN and pos_1[0].lower() == 'n' \
                        and pos_2[0].lower() == 'n' \
                        and sim >= WORD_SIM_THRESHOLD_NASARI_N:
                    logging.debug('Both nouns: ' + leaf_1[0] + ' : ' + leaf_2[0])
                    # edges.append((leaf_1[0], leaf_2[0], {'weight': math.exp(sim), 'type': 'inter', 'cb_weight' :  sim*100}))
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                elif sim >= WORD_SIM_THRESHOLD_NASARI:
                    # print "[DBG]: nasari sim = " + str(sim)
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                else:
                    pass
            elif WORD_SIM_MODE == 'nasari':
                tags_1 = leaf_1[0].split(':')[2]
                pos_1 = tags_1.split('#')[4].strip()
                lemma_1 = tags_1.split('#')[5].strip()
                tags_2 = leaf_2[0].split(':')[2]
                pos_2 = tags_2.split('#')[4].strip()
                lemma_2 = tags_2.split('#')[5].strip()
                if word_1 == word_2:
                    sim = 1.0
                else:
                    if LEMMA_SIM:
                        logging.debug('[DBG]: lemma %s, %s' % (lemma_1, lemma_2))
                        # print '[DBG]: lemma %s, %s' % (lemma_1, lemma_2)
                        sim = send_wordsim_request('ww', lemma_1, lemma_2)
                    else:
                        sim = send_wordsim_request('ww', word_1, word_2)
                if EXP_NOUN and pos_1[0].lower() == 'n' \
                        and pos_2[0].lower() == 'n' \
                        and sim >= WORD_SIM_THRESHOLD_NASARI_N:
                    logging.debug("[DBG]: both nouns: " + leaf_1[0] + ' : ' + leaf_2[0])
                    # print "[DBG]: both nouns: " + leaf_1[0] + ' : ' + leaf_2[0]
                    # edges.append((leaf_1[0], leaf_2[0], {'weight': math.exp(sim), 'type': 'inter', 'cb_weight' :  sim*100}))
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                elif sim >= WORD_SIM_THRESHOLD_NASARI:
                    # print "[DBG]: nasari sim = " + str(sim)
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                else:
                    pass
            elif WORD_SIM_MODE == 'lexvec':
                tags_1 = leaf_1[0].split(':')[2]
                pos_1 = tags_1.split('#')[4].strip()
                lemma_1 = tags_1.split('#')[5].strip()
                tags_2 = leaf_2[0].split(':')[2]
                pos_2 = tags_2.split('#')[4].strip()
                lemma_2 = tags_2.split('#')[5].strip()
                if word_1 == word_2:
                    sim = 1.0
                else:
                    if LEMMA_SIM:
                        logging.debug('[DBG]: lemma %s, %s' % (lemma_1, lemma_2))
                        # print '[DBG]: lemma %s, %s' % (lemma_1, lemma_2)
                        sim = lexvec_word_sim(lemma_1, lemma_2)
                        # sim = send_wordsim_request('ww', lemma_1, lemma_2)
                    else:
                        sim = lexvec_word_sim(word_1, word_2)
                        # sim = send_wordsim_request('ww', word_1, word_2)
                if EXP_NOUN and pos_1[0].lower() == 'n' \
                        and pos_2[0].lower() == 'n' \
                        and sim >= WORD_SIM_THRESHOLD_LEXVEC_N:
                    logging.debug("[DBG]: both nouns: " + leaf_1[0] + ' : ' + leaf_2[0])
                    # print "[DBG]: both nouns: " + leaf_1[0] + ' : ' + leaf_2[0]
                    # edges.append((leaf_1[0], leaf_2[0], {'weight': math.exp(sim), 'type': 'inter', 'cb_weight' :  sim*100}))
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                elif sim >= WORD_SIM_THRESHOLD_LEXVEC:
                    # print "[DBG]: nasari sim = " + str(sim)
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight': 100}))
                else:
                    pass
    return edges


def treestr_pair_to_graph(treestr_1, treestr_2, id_1, id_2):
    graph_1, digraph_1 = treestr_to_graph(treestr_1, id_1)
    graph_2, digraph_2 = treestr_to_graph(treestr_2, id_2)
    inter_edges = find_inter_edges(graph_1, graph_2)
    ret_graph = nx.compose(graph_1, graph_2)
    ret_graph.add_edges_from(inter_edges)
    return ret_graph, inter_edges, graph_1, graph_2, digraph_1, digraph_2


def get_tags_n_leaves(cycle):
    s1_nodes = {"tags": [], "leaves": []}
    s2_nodes = {"tags": [], "leaves": []}
    for node in cycle:
        if node[:2] == 's1':
            if node[3:4] == 'L':
                s1_nodes["leaves"].append(node)
            else:
                s1_nodes["tags"].append(node)
        elif node[:2] == 's2':
            if node[3:4] == 'L':
                s2_nodes["leaves"].append(node)
            else:
                s2_nodes["tags"].append(node)
    return s1_nodes, s2_nodes


def validate_cycle(cycle):
    ret = True
    s1_nodes, s2_nodes = get_tags_n_leaves(cycle)
    if len(s1_nodes["leaves"]) > 2 or len(s2_nodes["leaves"]) > 2 or len(s1_nodes["leaves"]) == 0 or len(
            s2_nodes["leaves"]) == 0 or len(s1_nodes["leaves"]) + len(s2_nodes["leaves"]) < 3 or (
            len(s1_nodes["tags"]) == 0 and len(s2_nodes["tags"]) == 0):
        ret = False
    return ret, s1_nodes["leaves"], s2_nodes["leaves"]


def find_shortest_path(g1, g2, sub_nodes1, sub_nodes2):
    if len(sub_nodes1) <= 1:
        p1 = sub_nodes1
    else:
        p1 = set()
        for m in sub_nodes1:
            for n in sub_nodes1:
                if sub_nodes1.index(m) < sub_nodes1.index(n):
                    p1.update(nx.shortest_path(g1, source=m, target=n))
        p1 = list(p1)

    if len(sub_nodes2) <= 1:
        p2 = sub_nodes2
    else:
        p2 = set()
        for m in sub_nodes2:
            for n in sub_nodes2:
                if sub_nodes2.index(m) < sub_nodes2.index(n):
                    p2.update(nx.shortest_path(g2, source=m, target=n))
        p2 = list(p2)
    return p1 + p2


def write_intermedia_to_file(doc1_id, doc2_id, json_data):
    fname = OUT_CYCLE_FILE_PATH + "%s#%s.json" % (doc1_id.replace('/', '_'), doc2_id.replace('/', '_'))
    with open(fname, 'w+') as outfile:
        try:
            json.dump(json_data, outfile, indent=4)
        except Exception as e:
            print "[DBG]: json exception: "
            print e
            outfile.write(json_data)
    outfile.close()


# when finding the min weight cycle basis, we need to be clear that
# a graph may have multiple min weight cycle bases.
# here we emphasize more on the edges of the two parse trees instead of
# the word similarities. so we use 'cb_weight' to compute the min weight
# cycle basis. the edges between words are of really high weights so that
# they are not preferred to be selected. in in this way, we expect that
# the resulting cycle basis can reflect the best choice of phrase-edges in
# each parse tree.
def find_min_cycle_basis(graph, tree_1, tree_2):
    # print "[DBG]: ----------------------------------------"
    # pre_cycle_basis = nx.minimum_cycle_basis(graph)
    pre_cycle_basis = nx.minimum_cycle_basis(graph, weight='cb_weight')
    min_cycle_basis = []
    while len(pre_cycle_basis) > 0:
        b = pre_cycle_basis.pop()
        v, sub_s1, sub_s2 = validate_cycle(b)
        if not v:
            p12 = find_shortest_path(tree_1, tree_2, sub_s1, sub_s2)
            H = graph.subgraph(p12)
            # sub_cycle_basis = nx.minimum_cycle_basis(H)
            sub_cycle_basis = nx.minimum_cycle_basis(H, weight='cb_weight')
            # print "[DBG]: sub_cycle_basis = "
            # print sub_cycle_basis
            for cc in sub_cycle_basis:
                # print "[DBG]: cc = "
                # print cc
                if cc not in pre_cycle_basis and cc not in min_cycle_basis and cc != b and set(cc) != set(b):
                    pre_cycle_basis.append(cc)
                # else:
                # print "[ERR]: Already has this cycle:"
                # print cc
                # print "[ERR]: Invalid cycle = "
                # print b
                # print "[ERR]: Leaves 1 = "
                # print sub_s1
                # print "[ERR]: Leaves 2 = "
                # print sub_s2
                # print "[ERR]: Current min_cycle_basis = "
                # print min_cycle_basis
                # print "[ERR]: Current pre_cycle_basis = "
                # print pre_cycle_basis
        else:
            min_cycle_basis.append(b)
            # print "[DBG]: add b to min_cycle_basis: "
            # print min_cycle_basis
        # print "===================="
    # print "[DBG]: ----------------------------------------"
    return min_cycle_basis


# this function is an alternative to find_min_cycle_basis
# instead of finding a min cycle basis, we find all simple cycles
# the weights are not necessary in finding simple cycles
# NetworkX only support di-graphs for finding simple cycles, so
# we convert our undi-graph to a di-graph.
def find_simple_cycles(graph, tree_1, tree_2):
    DiG = nx.DiGraph(graph)
    DiG = graph.to_directed()
    l_simple_cycles = list(nx.simple_cycles(DiG))
    # print "[DBG]: orig simple_cycles = "
    # print l_simple_cycles
    l_ret_cycles = []
    for cyc in l_simple_cycles:
        v, sub_s1, sub_s2 = validate_cycle(cyc)
        if v:
            cand_mark = True
            for cyc_cand in l_ret_cycles:
                if set(cyc) == set(cyc_cand):
                    cand_mark = False
                    break
            if cand_mark:
                l_ret_cycles.append(cyc)
    logging.debug("[DBG]: find %s simple cycles." % len(l_ret_cycles))
    # print "[DBG]: find %s simple cycles." % len(l_ret_cycles)
    return l_ret_cycles


def my_sigmoid(x):
    return 1 / (1 + math.exp(-x))


def cal_cycle_weight(cycle, inter_edges):
    s1_nodes, s2_nodes = get_tags_n_leaves(cycle)
    if len(s1_nodes["leaves"]) > 2 or len(s2_nodes["leaves"]) > 2:
        logging.error('Sentence has more than 2 words in one cycle!')
        # print "[ERR]: Sentence has more than 2 words in one cycle!"
    w1 = len(s1_nodes["tags"]) + 1
    w2 = len(s2_nodes["tags"]) + 1
    # only for arc stat
    # arc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # arc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # arc_sock.sendto(str(w1) + '#' + str(w2), ('localhost', 9103))
    # arc_sock.close()

    if CYC_RBF:
        arch_weight = math.exp(- (math.pow(max(w1, w2) - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)
    else:
        arch_weight = math.exp(CYC_SIG_PARAM_1 / (math.pow(w1, CYC_SIG_PARAM_2) + math.pow(w2, CYC_SIG_PARAM_2)))
        # arch_weight = my_sigmoid(CYC_SIG_PARAM_1 / (math.pow(w1, CYC_SIG_PARAM_2) + math.pow(w2, CYC_SIG_PARAM_2)))
    # arch_weight_1  = math.exp(- (math.pow(w1 - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)
    # arch_weight_2  = math.exp(- (math.pow(w2 - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)

    if SUM_SIM:
        # inter_weight = 1
        inter_weight = 0
        for link in inter_edges:
            if link[0] in s1_nodes["leaves"]:
                if link[1] in s2_nodes["leaves"]:
                    inter_weight += link[2]["weight"]
        print '[DBG]: sum sim: %s' % inter_weight
        # if link[2]["weight"] < inter_weight:
        # if link[2]["weight"] > inter_weight:
        # inter_weight = link[2]["weight"]
    else:
        inter_weight = 1
        # inter_weight = 0
        for link in inter_edges:
            if link[0] in s1_nodes["leaves"]:
                if link[1] in s2_nodes["leaves"]:
                    if link[2]["weight"] < inter_weight:
                        # if link[2]["weight"] > inter_weight:
                        inter_weight = link[2]["weight"]

    # ret = arch_weight * inter_weight
    ret = arch_weight * math.exp(inter_weight * 2)
    # ret = math.exp(inter_weight * 2)
    # print "[DBG]: arc = " + str(arch_weight) + " ws = " + str(math.exp(inter_weight*2))
    # print "[DBG]: arc = " + str(arch_weight) + " ws = " + str(inter_weight)
    if arch_weight == 1.0:
        print "[DBG]: w1 = %d" % w1
        print "[DBG]: w2 = %d" % w2
    return ret, s1_nodes["leaves"], s2_nodes["leaves"]


def find_path_len_between_leaves(w1, w2, ditree):
    if w1 == w2:
        return 0
    parent_1 = ditree.predecessors(w1)
    parent_2 = ditree.predecessors(w1)
    if len(parent_1) != 1 or len(parent_2) != 1:
    #     # logging.error('Tree is incorrect, %s, %s.' % (w1, w2))
        raise Exception('Tree is incorrect, %s, %s.' % (w1, w2))
    path_len = 0
    while True:
        if parent_1[0] == parent_2[0]:
co              return path_len
        parent_1_tmp = ditree.predecessors(parent_1[0])
        if len(parent_1_tmp) != 0:
            parent_1 = parent_1_tmp
        parent_2_tmp = ditree.predecessors(parent_2[0])
        if len(parent_2_tmp) != 0:
            parent_2 = parent_2_tmp


def compute_cycle_sim(sim_1, sim_2, arc_1, arc_2):
    cycle_sim = 0.0
    w_sim = min([sim_1, sim_2])
    arc_sig = CYC_SIG_PARAM_1 / (math.pow(arc_1, CYC_SIG_PARAM_2) + math.pow(arc_2, CYC_SIG_PARAM_2))
    

def sim_from_tree_pair_graph_all_basic_cycles(inter_edges, graph, ditree_1, ditree_2):
    l_inter_edge_pairs = list(combinations(inter_edges, 2))
    logging.debug('%s inter edge pairs.' % len(l_inter_edge_pairs))
    l_cycle_sim = []
    for inter_edge_pair in l_inter_edge_pairs:
        inter_edge_1 = inter_edge_pair[0]
        s1_w1 = None
        s2_w1 = None
        if inter_edge_1[0][:2] == 's1':
            s1_w1 = inter_edge_1[0]
            s2_w1 = inter_edge_1[1]
        else:
            s2_w1 = inter_edge_1[0]
            s1_w1 = inter_edge_1[1]
        s1_w2 = None
        s2_w2 = None
        inter_edge_2 = inter_edge_pair[1]
        if inter_edge_2[0][:2] == 's1':
            s1_w2 = inter_edge_2[0]
            s2_w2 = inter_edge_2[1]
        else:
            s1_w2 = inter_edge_2[0]
            s2_w2 = inter_edge_2[1]
        arc_1 = find_path_len_between_leaves(s1_w1, s1_w2, ditree_1)
        arc_2 = find_path_len_between_leaves(s2_w1, s2_w2, ditree_2)
        sim_1 = inter_edge_1[2]['weight']
        sim_2 = inter_edge_2[2]['weight']
        cycle_sim = compute_cycle_sim(sim_1, sim_2, arc_1, arc_2)



def sim_from_tree_pair_graph(inter_edges, graph, tree_1, tree_2):
    cycle_weights = []
    words_with_tags = []
    if len(inter_edges) < 2:
        return (0, [], [], 0)
    if SIMPLE_CYCLES:
        min_cycle_basis = find_simple_cycles(graph, tree_1, tree_2)
    else:
        min_cycle_basis = find_min_cycle_basis(graph, tree_1, tree_2)
    s_s1_leaves = set()
    s_s2_leaves = set()
    l_s1_phrases = []
    l_s2_phrases = []
    for cycle in min_cycle_basis:
        if len(cycle) < 3:
            logging.error("[ERR]: Invalid cycle in the basis: ")
            logging.error(cycle)
            continue
        cw, s1_leaves, s2_leaves = cal_cycle_weight(cycle, inter_edges)
        cycle_weights.append(cw)
        words_with_tags = words_with_tags + s1_leaves + s2_leaves
        s_s1_leaves = s_s1_leaves.union(set(s1_leaves))
        s_s2_leaves = s_s2_leaves.union(set(s2_leaves))
        if set(s1_leaves) not in l_s1_phrases:
            l_s1_phrases.append(set(s1_leaves))
        if set(s2_leaves) not in l_s2_phrases:
            l_s2_phrases.append(set(s2_leaves))
    cnt_tree_1_leaves = len([node[0] for node in tree_1.nodes(data='type') if node[1] == 'leaf'])
    cnt_tree_2_leaves = len([node[0] for node in tree_2.nodes(data='type') if node[1] == 'leaf'])
    # pair_len_weight = 2.0 / (cnt_tree_1_leaves / len(s_s1_leaves) + cnt_tree_2_leaves / len(s_s2_leaves))
    pair_len_weight = 2.0 / \
    (
            1 / (len(l_s1_phrases) / (special.comb(cnt_tree_1_leaves, 2) + cnt_tree_1_leaves)) +
            1 / (len(l_s2_phrases) / (special.comb(cnt_tree_2_leaves, 2) + cnt_tree_2_leaves))
    )
    logging.debug('pair_len_weight = %s' % pair_len_weight)
    return (sum(cycle_weights), min_cycle_basis, words_with_tags, pair_len_weight)


def sent_pair_sim(sent_treestr_1, sent_treestr_2):
    tp_graph, inter_edges, tree_1, tree_2, ditree_1, ditree_2 = treestr_pair_to_graph(sent_treestr_1, sent_treestr_2, 's1', 's2')
    (sim, min_cycle_basis, words_with_tags, pair_len_weight) = sim_from_tree_pair_graph(inter_edges, tp_graph, tree_1,
                                                                                        tree_2)
    words = []
    for word in words_with_tags:
        words.append(lowercase_word_by_ner(word))
        # words.append(re.split('[:#]', word)[2])
    # pid = os.getpid()
    # proc = psutil.Process(pid)
    # print "[DBG]: sent_pair_sim before term"
    # proc.terminate()
    # print "[DBG]: sent_pair_sim after term"
    if sim != 0:
        # print "----------------------------------------"
        # print "[DBG]: sent 1 = " + sent_treestr_1
        # print "[DBG]: sent 2 = " + sent_treestr_2
        # print "[DBG]: sent sim = " + str(sim)
        pass
    else:
        return 0, [], words
    return sim * pair_len_weight, min_cycle_basis, words


def count_cycles_and_record(doc_1, doc_2, cycle_count):
    try:
        outfile = open('/home/fcmeng/workspace/data/cycle_count.txt', 'a+')
        outfile.write("{0}#{1}:{2}\n".format(doc_1, doc_2, cycle_count))
        outfile.close()
    except Exception as e:
        print e


def doc_pair_sim(doc1, doc2):
    start = time.time()
    # global RECV_PORT
    # sim_arr = multiprocessing.Array(ctypes.c_double, num_sent_pairs)
    # sim_arr_i = 0
    # sim_procs = []
    # proc_id = 0
    # proc_batch = 0
    l_sent_treestr_1 = doc1[1]
    l_sent_treestr_2 = doc2[1]
    if l_sent_treestr_1 is None or l_sent_treestr_2 is None or len(l_sent_treestr_1) == 0 or len(l_sent_treestr_2) == 0:
        logging.error('Invalid input doc!')
        logging.error("l_sent_treestr_1 = ")
        logging.error(l_sent_treestr_1)
        logging.error("l_sent_treestr_2 = ")
        logging.error(l_sent_treestr_2)
        # print "create process: " + str(p.pid)
        if SAVE_CYCLES:
            out_json = {'sim': 0, 'sentence_pair': {}, 'word_list': []}
        else:
            out_json = [0, ""]
        write_intermedia_to_file(doc1[0], doc2[0], out_json)
        end = time.time()
        return 0
    # print "[DBG]: parent pid = " + str(os.getpid())
    # if SAVE_CYCLES:
    #     out_json = {'sim':0.0, 'sentence_pair':{}}
    # else:
    #     out_json = []
    doc_sim = 0.0
    sentence_pair = dict()
    doc_word_list = []
    cycle_count = 0
    l_cyc_count_per_sent_pair = []
    for doc1_s_id, sent_treestr_1 in enumerate(l_sent_treestr_1):
        for doc2_s_id, sent_treestr_2 in enumerate(l_sent_treestr_2):
            # TODO:
            sim_res, min_cycle_basis, word_list = sent_pair_sim(sent_treestr_1, sent_treestr_2)
            # cycle_count += len(min_cycle_basis)
            # print "[DBG]: len of min_cycle_basis is %s" % len(min_cycle_basis)
            if CYC_DIST:
                if len(min_cycle_basis) > 0:
                    l_cyc_count_per_sent_pair.append(1)
                else:
                    l_cyc_count_per_sent_pair.append(0)
            # print "[DBG]: l_cyc_count_per_sent_pair = "
            # print l_cyc_count_per_sent_pair

            if sim_res != 0:
                doc_sim += sim_res
                # cycle count
                if SAVE_CYCLES:
                    sent_pair_sim_cycles = {'cycles': min_cycle_basis, 'sim': sim_res}
                    sentence_pair["%s-%s" % (doc1_s_id, doc2_s_id)] = sent_pair_sim_cycles
                doc_word_list = doc_word_list + word_list
                # if SAVE_CYCLES:
                #     sent_pair_sim_cycles = {'cycles':min_cycle_basis, 'sim':sim_res}
                #     out_json['sim'] += sim_res
                #     out_json['sentence_pair']["%s-%s" % (doc1_s_id, doc2_s_id)] = sent_pair_sim_cycles
                # else:
                #     out_json[0] += sim_res
                #     out_json[1] = out_json[1].append(word_list)
    # we may want to reward doc pairs with more cycles
    # count_cycles_and_record(doc1[0], doc2[0], cycle_count)
    # norm_cycle_count = (cycle_count / 150.0) * 5
    # doc_sim = doc_sim * math.exp(norm_cycle_count)
    # doc_sim += cycle_count
    # calculate the doc sim weight w.r.t. the distribution of cycles
    if CYC_DIST:
        divgrad_cyc_count = []
        for i in range(len(l_cyc_count_per_sent_pair)):
            for j in range(i + 1, len(l_cyc_count_per_sent_pair)):
                divgrad_cyc_count.append(math.pow(l_cyc_count_per_sent_pair[i] - l_cyc_count_per_sent_pair[j], 2))
        cyc_dist_weight = math.exp(- math.sqrt(sum(divgrad_cyc_count) / len(l_cyc_count_per_sent_pair)))
        old_doc_sim = doc_sim
        doc_sim = old_doc_sim * cyc_dist_weight
        print "[DBG]: divgrad_cyc_count = "
        print divgrad_cyc_count
        print "[DBG]: cyc_dist_weight = %s, old_doc_sim = %s, new_doc_sim = %s" % (
            cyc_dist_weight, old_doc_sim, doc_sim)
        print "===================="
    if SAVE_CYCLES:
        out_json = {'sim': doc_sim, 'sentence_pair': sentence_pair, 'word_list': list(set(doc_word_list))}
    else:
        out_json = [doc_sim, ','.join(set(doc_word_list))]
    write_intermedia_to_file(doc1[0], doc2[0], out_json)
    end = time.time()
    print("[DBG]: Doc pair costs %s" % (end - start))


def isValidTree(tree_str):
    if tree_str == "" or tree_str == "ROOT":
        return False
    return True


def validate_doc_trees(doc1_tree, doc2_tree):
    valid_doc1_tree = list(filter(lambda s: isValidTree(s), doc1_tree.split('|')))
    valid_doc2_tree = list(filter(lambda s: isValidTree(s), doc2_tree.split('|')))
    return valid_doc1_tree, valid_doc2_tree


def sim_procs_cool_down(l_sim_proc, max_proc):
    # print "[DBG]: start a cool-down."
    while (len(l_sim_proc) > max_proc):
        # print "[DBG]: sim_procs_cool_down:" + str(len(l_sim_proc))
        for proc in l_sim_proc:
            proc.join(1)
            if not proc.is_alive():
                # print "[DBG]: " + str(proc.pid) + " is removed."
                l_sim_proc.remove(proc)
    return len(l_sim_proc)
    # print "[DBG]: done a cool-down."


# this function compute the text similarity between two users given a text data within a specified period for each user.
def text_sim(db_cur):
    global g_lexvec_model
    g_lexvec_model = load_lexvec_model()
    db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    # db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = 'talk.politics.mideast/77290' OR doc_id = 'talk.politics.mideast/77256'")
    rows = db_cur.fetchall()
    total_doc_pair_count = (len(rows) * (len(rows) - 1)) / 2
    print("[INF]: Total doc-pairs = %s" % total_doc_pair_count)
    sim_procs = []
    proc_id = 0
    proc_batch = 0
    done_proc_count = 0
    for i, doc1 in enumerate(rows):
        for j, doc2 in enumerate(rows):
            if os.path.exists(
                    OUT_CYCLE_FILE_PATH + "%s#%s.json" % (doc1[0].replace('/', '_'), doc2[0].replace('/', '_'))):
                logging.error(
                    "[%s-%s]%s#%s.json already exists." % (i, j, doc1[0].replace('/', '_'), doc2[0].replace('/', '_')))
                continue
            if i < j:
                if WORD_SIM_MODE == 'adw_offline':
                    fillDocPairDict(doc1[0], doc2[0])
                valid_doc1, valid_doc2 = validate_doc_trees(doc1[1], doc2[1])
                print("[DBG]: doc pairs = %s : %s" % (doc1[0], doc2[0]))
                p = threading.Thread(target=doc_pair_sim, args=((doc1[0], valid_doc1), (doc2[0], valid_doc2)))
                # p = multiprocessing.Process(target=doc_pair_sim, args=((doc1[0], valid_doc1), (doc2[0], valid_doc2)))
                # proc_id += 1
                sim_procs.append(p)
                p.start()
                # if proc_id >= PROC_BATCH_SIZE:
                #     proc_batch += 1
                #     proc_id = 0
                # print "[DBG]: task count = " + str(proc_batch * PROC_BATCH_SIZE + proc_id)
                # print "[DBG]: sim array ="
                # if len(sim_procs) >= MAX_PROC:
                # print "[DBG]: cool down 1"
                current_proc_count = len(sim_procs)
                after_cool_down_proc_count = sim_procs_cool_down(sim_procs, MAX_PROC)
                done_proc_count += (current_proc_count - after_cool_down_proc_count)
                print("[INF]: {0:.0%} done!".format(float(done_proc_count) / float(total_doc_pair_count)))
                # print "[DBG]: sim procs:" + str(len(sim_procs))
            # p.join()
        # print "[DBG]: cool down 2"
    current_proc_count = len(sim_procs)
    after_cool_down_proc_count = sim_procs_cool_down(sim_procs, 0)
    done_proc_count += (current_proc_count - after_cool_down_proc_count)
    print("[INF]: {0:.0%} done!".format(float(done_proc_count) / float(total_doc_pair_count)))

    # print "[DBG]: total task: " + str(len(l_sent_treestr_1)*len(l_sent_treestr_2))
    # sim = doc_pair_sim(l_sent_treestr_1, l_sent_treestr_2, len(l_sent_treestr_1)*len(l_sent_treestr_2))
    # return sim


# this function is used to compare each lee doc with all leebg docs
# we use this function is to obtain GPs for lee docs
def text_sim_lee_vs_leebg(lee_db_cur, leebg_db_cur):
    print "[INF]: Lee vs LeeBG ..."
    lee_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    # db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '13' OR doc_id = '31'")
    lee_rows = lee_db_cur.fetchall()
    print "[DBG]: lee_rows = %s" % len(lee_rows)

    leebg_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    leebg_rows = leebg_db_cur.fetchall()
    print "[DBG]: leebg_rows = %s" % len(leebg_rows)

    total_doc_pair_count = len(lee_rows) * len(leebg_rows)
    print "[INF]: Total doc-pairs = %s" % total_doc_pair_count

    sim_procs = []
    proc_id = 0
    proc_batch = 0
    for i, doc1 in enumerate(lee_rows):
        for j, doc2 in enumerate(leebg_rows):
            valid_doc1, valid_doc2 = validate_doc_trees(doc1[1], doc2[1])
            p = multiprocessing.Process(target=doc_pair_sim, args=((doc1[0], valid_doc1), (doc2[0], valid_doc2)))
            proc_id += 1
            sim_procs.append(p)
            p.start()
            if proc_id >= PROC_BATCH_SIZE:
                proc_batch += 1
                proc_id = 0
            if len(sim_procs) >= MAX_PROC:
                # print "[DBG]: cool down 1"
                sim_procs_cool_down(sim_procs)
                print "[INF]: {0:.0%} done!".format(float(proc_batch * PROC_BATCH_SIZE) / float(total_doc_pair_count))
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count) / float(total_doc_pair_count))


# this function is used for Li65 sentence comparison
def text_sim_li65(col1_db_cur, col2_db_cur):
    if WORD_SIM_MODE == 'lexvec':
        global g_lexvec_model
        g_lexvec_model = load_lexvec_model()
    print "[INF]: Li65 sentence comparison ..."
    col1_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    # db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '13' OR doc_id = '31'")
    col1_rows = col1_db_cur.fetchall()
    print "[DBG]: col1_rows = %s" % len(col1_rows)

    col2_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    col2_rows = col2_db_cur.fetchall()
    print "[DBG]: col2_rows = %s" % len(col2_rows)

    total_doc_pair_count = len(col1_rows)
    print "[INF]: Total doc-pairs = %s" % total_doc_pair_count

    sim_procs = []
    proc_id = 0
    proc_batch = 0
    done_proc_count = 0
    for i in range(len(col1_rows)):
        valid_doc1, valid_doc2 = validate_doc_trees(col1_rows[i][1], col2_rows[i][1])
        # p = multiprocessing.Process(target=doc_pair_sim,
        #                             args=((col1_rows[i][0], valid_doc1), (col2_rows[i][0], valid_doc2)))
        p = threading.Thread(target=doc_pair_sim, args=((col1_rows[i][0], valid_doc1), (col2_rows[i][0], valid_doc2)))
        proc_id += 1
        sim_procs.append(p)
        p.start()
        current_proc_count = len(sim_procs)
        after_cool_down_proc_count = sim_procs_cool_down(sim_procs, MAX_PROC)
        done_proc_count += (current_proc_count - after_cool_down_proc_count)
        print("[INF]: {0:.0%} done!".format(float(done_proc_count) / float(total_doc_pair_count)))
    after_cool_down_proc_count = sim_procs_cool_down(sim_procs, MAX_PROC)
    done_proc_count += (current_proc_count - after_cool_down_proc_count)
    print("[INF]: {0:.0%} done!".format(float(done_proc_count) / float(total_doc_pair_count)))


# this function is used for MSR sentence comparison
def text_sim_msr(msr_db_cur):
    print "[INF]: MSR sentence comparison ..."
    # read in sent pairs
    sent_pair_fd = open(MSR_SENT_PAIR_FILE, 'r')
    sent_pair_fd.seek(0, 0)
    l_sent_pairs = []
    sent_pair_lines = sent_pair_fd.readlines()
    for sp_line in sent_pair_lines:
        sent_pair = []
        sp = sp_line.split(':')[0].split('#')
        sent_1_idx = sp[0].strip()
        sent_2_idx = sp[1].strip()
        msr_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE doc_id = ?', (sent_1_idx,))
        # msr_db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '1984954'")
        msr_row = msr_db_cur.fetchone()
        if msr_row[1] is None or len(msr_row[1]) == 0:
            print "[ERR]: An incorrect parse tree for %s" % sent_1_idx
            continue
        else:
            sent_pair.append(sent_1_idx)
            sent_pair.append(msr_row[1])
        # msr_db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '1984531'")
        msr_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE doc_id = ?', (sent_2_idx,))
        msr_row = msr_db_cur.fetchone()
        if msr_row[1] is None or len(msr_row[1]) == 0:
            print "[ERR]: An incorrect parse tree for %s" % sent_2_idx
            continue
        else:
            sent_pair.append(sent_2_idx)
            sent_pair.append(msr_row[1])
        l_sent_pairs.append(sent_pair)
    total_doc_pair_count = len(l_sent_pairs)
    print "[DBG]: Total %s sent pairs to go..." % total_doc_pair_count
    sent_pair_fd.close()

    sim_procs = []
    proc_id = 0
    proc_batch = 0
    for sent_pair in l_sent_pairs:
        valid_doc1, valid_doc2 = validate_doc_trees(sent_pair[1], sent_pair[3])
        p = multiprocessing.Process(target=doc_pair_sim, args=((sent_pair[0], valid_doc1), (sent_pair[2], valid_doc2)))
        proc_id += 1
        sim_procs.append(p)
        p.start()
        if proc_id >= PROC_BATCH_SIZE:
            proc_batch += 1
            proc_id = 0
        if len(sim_procs) >= MAX_PROC:
            # print "[DBG]: cool down 1"
            sim_procs_cool_down(sim_procs)
            print "[INF]: {0:.0%} done!".format(float(proc_batch * PROC_BATCH_SIZE) / float(total_doc_pair_count))
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count) / float(total_doc_pair_count))


def load_lexvec_model():
    lexvec_model = KeyedVectors.load_word2vec_format(LEXVEC_MODEL_PATH, binary=False)
    return lexvec_model


def main():
    # =========================================================
    # output_file = str(sys.argv[1]).strip()
    start = time.time()
    if LEE:
        db_conn = sqlite3.connect(DB_CONN_STR)
        cur = db_conn.cursor()
        text_sim(cur)
        db_conn.close()
    elif LEE_VS_LEEBG:
        db_conn = sqlite3.connect(DB_CONN_STR)
        cur = db_conn.cursor()
        leebg_db_conn = sqlite3.connect(BGDB_CONN_STR)
        leebg_cur = leebg_db_conn.cursor()
        text_sim_lee_vs_leebg(cur, leebg_cur)
        leebg_db_conn.close()
        db_conn.close()
    elif LI65:
        # li65_col1_db_conn = sqlite3.connect(LI65_COL1_CONN_STR)
        # li65_col2_db_conn = sqlite3.connect(LI65_COL2_CONN_STR)
        li65_col1_db_conn = sqlite3.connect(SICK_COL1_CONN_STR)
        li65_col2_db_conn = sqlite3.connect(SICK_COL2_CONN_STR)

        li65_col1_db_cur = li65_col1_db_conn.cursor()
        li65_col2_db_cur = li65_col2_db_conn.cursor()
        text_sim_li65(li65_col1_db_cur, li65_col2_db_cur)
        li65_col1_db_conn.close()
        li65_col2_db_conn.close()
    elif MSR:
        msr_db_conn = sqlite3.connect(DB_CONN_STR)
        msr_db_cur = msr_db_conn.cursor()
        text_sim_msr(msr_db_cur)
        msr_db_conn.close()

    # ret = '|'.join([str(sim)]) + '\n'
    # with open(output_file, 'a+') as f_out:
    #     fcntl.flock(f_out, fcntl.LOCK_EX)
    #     f_out.write(ret)
    #     fcntl.flock(f_out, fcntl.LOCK_UN)
    #     f_out.close()
    print "[DBG]: Total time elapse = %s" % (time.time() - start)
    print "ALL DONE!"
    # only for arc stat
    # arc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # arc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # arc_sock.sendto('done', ('localhost', 9103))
    # arc_sock.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
