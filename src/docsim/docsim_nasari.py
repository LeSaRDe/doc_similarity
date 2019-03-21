import nltk
from nltk.tree import Tree, ParentedTree
# from nltk.tokenize import sent_tokenize
# from nltk.parse import corenlp
import socket
import networkx as nx
# import matplotlib.pyplot as plt
import math
import multiprocessing
# import ctypes
import sqlite3
# import random
# import sys
import re
import os
import json
import time
import numpy

#(ROOT (S (NP (NP (NNP Align#[00464321v]) (, ,) (NNP Disambiguate#[00957178v]) (, ,) ) (CC and) (NP (NP (VB Walk#[01904930v])) (PRN (-LRB- -LRB-) (NP (NN ADW)) (-RRB- -RRB-)))) (VP (VBZ is) (NP (NP (DT a) (JJ WordNet-based) (NN approach#[00941140n])) (PP (IN for) (S (VP (VBG measuring#[00647094v]) (NP (NP (JJ semantic#[02842042a]) (NN similarity#[04743605n])) (PP (IN of) (NP (NP (JJ arbitrary#[00718924a]) (NNS pairs#[13743605n])) (PP (IN of) (NP (JJ lexical#[02886629a]) (NNS items#[03588414n]))) (, ,) (PP (IN from) (NP (NN word#[06286395n]) (NNS senses#[03990834n])))))) (PP (TO to) (NP (JJ full#[01083157a]) (NNS texts#[06387980n, 06388579n])))))))) (. .)))

#con_sent_tree_str ='(ROOT (S (NP (NP L:Align#00464321v L:Disambiguate#00957178v) L:Walk#01904930v) (NP (NP L:WordNet-based L:approach#04746134n) (S (VP L:measuring#00647094v (NP (NP L:semantic#02842042a L:similarity#06251033n) (NP (NP L:arbitrary#00718924a L:pairs#13743605n) (NP L:lexical#02886869a L:items#03588414n) (NP L:word#06286395n L:senses#03990834n))) (NP L:full#01083157a L:texts#06414372n))))))'

#test_sent_tree_str_1 = '(ROOT (S (SBAR (S (VP L:mentioned#01024190v (NP L:UI L:comments#06762711n)))) (VP L:think#00689344v (SBAR (S (VP L:like#00691665v (S (VP L:avoid#00811375v (S (VP L:Getting (NP (NP L:Started#00345761v L:tab#04379096n) (NP L:package#03871083n (S (VP L:avoid#00811375v (NP L:customer#09984659n L:confusion#00379754n)))))))))))))))'

#test_sent_tree_str_2 = '(ROOT (S (S L:probably#00138611r+00138611r L:capability#05202497n (SBAR (S L:driver#06574473n (VP L:set#01062395v L:capability#05202497n (SBAR (S (VP L:notice#01058574v (SBAR (S L:Chrome#14810704n))))))))) (S L:run#02730326v)))'

#sent = 'Align, Disambiguate, and Walk (ADW) is a WordNet-based approach for measuring semantic similarity of arbitrary pairs of lexical items, from word senses to full texts.'

NODE_ID_COUNTER = 0
WORD_SIM_THRESHOLD_ADW = 0.75
WORD_SIM_THRESHOLD_NASARI = 0.50
WORD_SIM_THRESHOLD_NASARI_N = 0.50
SEND_PORT_ADW = 8607
SEND_PORT_NASARI = 8306
SEND_ADDR_ADW = 'localhost'
SEND_ADDR_NASARI = 'localhost'
MULTI_WS_SERV = False
MULTI_WS_SERV_MOD = 4
RAND_SEED = 0
# the other option is 'adw'
WORD_SIM_MODE = 'nasari'
#WORD_SIM_MODE = 'adw'
#WORD_SIM_MODE = 'adw_tag'
DB_CONN_STR = '/home/{0}/workspace/data/20news50short10.db'.format(os.environ['USER'])
# used for lee vs leebg
BGDB_CONN_STR = '/home/{0}/workspace/data/leebgfixsw.db'.format(os.environ['USER'])
# used for Li65 sentence comparison
LI65_COL1_CONN_STR = '/home/{0}/workspace/data/Li65/col1_sw.db'.format(os.environ['USER'])
LI65_COL2_CONN_STR = '/home/{0}/workspace/data/Li65/col2_sw.db'.format(os.environ['USER'])
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
# test = only for test use
OUT_CYCLE_FILE_PATH = '/home/{0}/workspace/data/20news50short10_nasari_50_rmswcbwexpwsscyc_makeup_w3-3/'.format(os.environ['USER'])
#CYC_SIG_PARAM 1 and 2 are used by exp(param1/(w1^param2 + w2^param2))
CYC_SIG_PARAM_1 = 3.0
CYC_SIG_PARAM_2 = 3.0
# for cycle distribution penalty
CYC_DIST = False
# exponentiate noun similarities
EXP_NOUN = False
# use rbf like method to compute cycle significance
# CYC_SIG_PARAM_3 and 4 are used in this case
CYC_RBF = False
#CYC_SIG_PARAM 3 and 4 are used by exp(- (w1-param3)^2 / param4)
CYC_SIG_PARAM_3 = 1.0
CYC_SIG_PARAM_4 = 20.0
MAX_PROC = multiprocessing.cpu_count()
PROC_BATCH_SIZE = multiprocessing.cpu_count()
# use simple cycles instead of min cycle basis
SIMPLE_CYCLES = True
# swtich for lee and 20news
LEE = True
# switch for lee vs leebg
LEE_VS_LEEBG = False
# switch for Li65
LI65 = False
# swthich for msr
MSR = False
MSR_SENT_PAIR_FILE = '/home/{0}/workspace/data/msr/msr_sim.txt'.format(os.environ['USER'])

SAVE_CYCLES = True

PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']

# this function takes a tree string and returns the graph of this tree
# the format of the input tree string needs follow the CoreNLP Tree def.
# this format is compatible with NLTK Tree.
# the graph is introduced from NetworkX.
# leaf format: [s_i]:L:[word]#[synset_1]+...+[synset_2]#[token_idx]#[ner]#[pos]#[lemma]:[uni_idx]
def treestr_to_graph(treestr, id):
    ret_graph = nx.Graph()
    tree = Tree.fromstring(treestr)
    #checkTree(tree, '0')
    global NODE_ID_COUNTER
    identifyNodes(tree, id)
    tree.set_label(id + ':' + tree.label() + ':' + str(NODE_ID_COUNTER))
    tree_prod = tree.productions()
    #print tree_prod
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
                ret_graph.add_node(end, type='leaf', tags = offset_tags, idx = token_index)
            else:
                ret_graph.add_node(end, type='node')
            ret_graph.add_edge(start, end.strip(), weight = 1, type = 'intra', cb_weight = 1)
    #print "tree_str_to_graph:"
    #print ret_graph.nodes
    return ret_graph


def checkTree(tree, id):
    for index, subtree in enumerate(tree):
        subtree_id = id + ":" + str(index)
        #print "subtree:" + subtree_id
        #print subtree
        if isinstance(subtree, ParentedTree):
            checkTree(subtree, subtree_id)


def identifyNodes(t, idx):
    global NODE_ID_COUNTER
    #print "identifyNodes: " + idx
    for index, subtree in enumerate(t):
        #print "find a type: " + str(type(subtree))
        if isinstance(subtree, Tree):
            NODE_ID_COUNTER += 1
            subtree.set_label(idx + ':' + subtree.label() + ':' + str(NODE_ID_COUNTER))
            identifyNodes(subtree, idx)
        #elif isinstance(subtree, str):
        else:
            newVal = idx + ':' + subtree + ':' + str(NODE_ID_COUNTER)
            t[index] = newVal
        NODE_ID_COUNTER += 1

# given a leaf in a parse tree, this function returns True
# if this leaf has a significant NER tag, such as ORGANIZATION,
# LOCATION and MISC; otherwise returns False.
#def has_preserved_ner(leaf_str):
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
        #input_1 and input_2 are the two words here
        send_str = input_1 + '#' + input_2
        send_port = SEND_PORT_NASARI
        send_addr = SEND_ADDR_NASARI
        if MULTI_WS_SERV:
            millis = int(round(time.time() * 1000))
            millis &= 0xffffffffL
            #RAND_SEED += 1
            #RAND_SEED = int(RAND_SEED)
            #RAND_SEED &= 0xffffffffL
            numpy.random.seed(millis)
            send_port += numpy.random.randint(MULTI_WS_SERV_MOD)
            #print "[DBG]: connecting port %s" % send_port
    elif mode == 'tt':
        send_str = mode + '#' + input_1 + '#' + input_2
        send_port = SEND_PORT_ADW
        send_addr = SEND_ADDR_ADW
    else:
        raise Exception('[ERR]: Unsupported word comparison mode!')

    #while attemp < 10:
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
    #attemp = 0
    while True:
        try:
            c_sock.settimeout(1.0)
            ret_str, serv_addr = c_sock.recvfrom(4096)
            ret = float(ret_str)
            #print "[DBG]: send_word_sim_request:" + str(ret)
            c_sock.close()
            return ret
        #except socket.error, msg:
        except Exception as e:
            print "[ERR]: Cannot get word similarity! Resend!"
            print e.message
            c_sock.close()
            c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            c_sock.sendto(send_str, (send_addr, send_port))
            #time.sleep(random.randint(1, 6))
            attemp += 1
    # c_sock.close()
    # return ret

# this function finds all edges between two parsing trees w.r.t. two sentenses.
# an edge will be created only when its weight is greater than a threshold.
# 'tree_1' and 'tree_2' are two parsing trees.
# what is returned is a collection of edges.
def find_inter_edges(tree_1, tree_2):
    edges = []
    leaves_1 = filter(lambda(f, d): d['type'] == 'leaf', tree_1.nodes(data=True))
    leaves_2 = filter(lambda(f, d): d['type'] == 'leaf', tree_2.nodes(data=True))
    #print "find_inter_edges:"
    #print tree_1.edges
    #print tree_2.edges
    #print tree_1.nodes
    #print tree_2.nodes
    #print "leaves:"
    #print leaves_1
    #print leaves_2
    for leaf_1 in leaves_1:
        #print "[DBG]: leaf_1 str = " + leaf_1[0]
        synset_1 = leaf_1[1]['tags']
        word_1 = lowercase_word_by_ner(leaf_1[0])
        #word_1 = leaf_1[0].split(':')[2].split('#')[0].strip()
        for leaf_2 in leaves_2:
            sim = float(0)
            synset_2 = leaf_2[1]['tags']
            word_2 = lowercase_word_by_ner(leaf_2[0])
            #word_2 = leaf_2[0].split(':')[2].split('#')[0].strip()
            if WORD_SIM_MODE == 'adw':
                if len(synset_1) > 0 and len(synset_2) > 0:
                    sim = send_wordsim_request('oo', synset_1, synset_2)
                if sim == float(0):
                    if word_1 == word_2:
                        sim = 1
                if sim > WORD_SIM_THRESHOLD_ADW:
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight' :  100}))
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
                    sim = send_wordsim_request('tt', word_1+':'+pos_1, word_2+':'+pos_2)
                else:
                    sim = 0.0
                if sim > WORD_SIM_THRESHOLD_ADW:
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight' :  100}))
            elif WORD_SIM_MODE == 'nasari':
                if word_1 == word_2:
                    sim = 1.0
                else:
                    sim = send_wordsim_request('ww', word_1, word_2)
                tags_1 = leaf_1[0].split(':')[2]
                pos_1 = tags_1.split('#')[4].strip()
                tags_2 = leaf_2[0].split(':')[2]
                pos_2 = tags_2.split('#')[4].strip()
                if EXP_NOUN and pos_1[0].lower() == 'n' and pos_2[0].lower() == 'n' and sim >= WORD_SIM_THRESHOLD_NASARI_N:
                    #print "[DBG]: both nouns: " + leaf_1[0] + ' : ' + leaf_2[0]
                    edges.append((leaf_1[0], leaf_2[0], {'weight': math.exp(sim), 'type': 'inter', 'cb_weight' :  sim*100}))
                elif sim >= WORD_SIM_THRESHOLD_NASARI:
                    #print "[DBG]: nasari sim = " + str(sim)
                    edges.append((leaf_1[0], leaf_2[0], {'weight': sim, 'type': 'inter', 'cb_weight' :  100}))
                else:
                    pass
    return edges


def treestr_pair_to_graph(treestr_1, treestr_2, id_1, id_2):
    graph_1 = treestr_to_graph(treestr_1, id_1)
    graph_2 = treestr_to_graph(treestr_2, id_2)
    inter_edges = find_inter_edges(graph_1, graph_2)
    ret_graph = nx.compose(graph_1, graph_2)
    ret_graph.add_edges_from(inter_edges)
    return ret_graph, inter_edges, graph_1, graph_2


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
    if len(s1_nodes["leaves"]) > 2 or len(s2_nodes["leaves"]) > 2 or len(s1_nodes["leaves"]) == 0 or len(s2_nodes["leaves"]) ==0 or len(s1_nodes["leaves"]) + len(s2_nodes["leaves"]) < 3 or (len(s1_nodes["tags"]) == 0 and len(s2_nodes["tags"]) ==0):
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
    fname = OUT_CYCLE_FILE_PATH + "%s#%s.json" % (doc1_id.replace('/','_'), doc2_id.replace('/','_'))
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
    #print "[DBG]: ----------------------------------------"
    #pre_cycle_basis = nx.minimum_cycle_basis(graph)
    pre_cycle_basis = nx.minimum_cycle_basis(graph, weight='cb_weight')
    #print "[DBG]: pre_cycle_basis init = "
    #print pre_cycle_basis
    min_cycle_basis = []
    while len(pre_cycle_basis) > 0:
        #print "===================="
        #print "[DBG]: graph nodes = "
        #print graph.nodes()
        #print "[DBG]: graph edges = "
        #print graph.edges()
        #print "[DBG]: tree_1 nodes = "
        #print tree_1.nodes()
        #print "[DBG]: tree_1 edges = "
        #print tree_1.edges()
        #print "[DBG]: tree_2 nodes = "
        #print tree_2.nodes()
        #print "[DBG]: tree_2 edges = "
        #print tree_2.edges()
        b = pre_cycle_basis.pop()
        #print "[DBG]: pop basic cycle: "
        #print b
        v, sub_s1, sub_s2 = validate_cycle(b)
        #print "[DBG]: b is " + str(v)
        if not v:
            p12 = find_shortest_path(tree_1, tree_2, sub_s1, sub_s2)
            #print "[DBG]: p12 = "
            #print p12
            H = graph.subgraph(p12)
            #print "[DBG]: H nodes = "
            #print H.nodes()
            #print "[DBG]: H edges = "
            #print H.edges()
            #sub_cycle_basis = nx.minimum_cycle_basis(H)
            sub_cycle_basis = nx.minimum_cycle_basis(H, weight='cb_weight')
            #print "[DBG]: sub_cycle_basis = "
            #print sub_cycle_basis
            for cc in sub_cycle_basis:
                #print "[DBG]: cc = "
                #print cc
                if cc not in pre_cycle_basis and cc not in min_cycle_basis and cc != b and set(cc) != set(b):
                    pre_cycle_basis.append(cc)
                    #print "[DBG]: add cc to pre_cycle_basis: "
                    #print pre_cycle_basis
                #else:
                    #print "[ERR]: Already has this cycle:"
                    #print cc
                    #print "[ERR]: Invalid cycle = "
                    #print b
                    #print "[ERR]: Leaves 1 = "
                    #print sub_s1
                    #print "[ERR]: Leaves 2 = "
                    #print sub_s2
                    #print "[ERR]: Current min_cycle_basis = "
                    #print min_cycle_basis
                    #print "[ERR]: Current pre_cycle_basis = "
                    #print pre_cycle_basis
        else:
            min_cycle_basis.append(b)
            #print "[DBG]: add b to min_cycle_basis: "
            #print min_cycle_basis
        #print "===================="
    #print "[DBG]: ----------------------------------------"
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
    #print "[DBG]: orig simple_cycles = "
    #print l_simple_cycles
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
    print "[DBG]: find %s simple cycles." % len(l_ret_cycles)
    return l_ret_cycles



def cal_cycle_weight(cycle, inter_edges):
    s1_nodes, s2_nodes = get_tags_n_leaves(cycle)
    if len(s1_nodes["leaves"]) > 2 or len(s2_nodes["leaves"]) > 2:
        print "[ERR]: Sentence has more than 2 words in one cycle!"
    w1 = len(s1_nodes["tags"]) + 1
    w2 = len(s2_nodes["tags"]) + 1
    # only for arc stat
    #arc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #arc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #arc_sock.sendto(str(w1) + '#' + str(w2), ('localhost', 9103))
    #arc_sock.close()

    if CYC_RBF:
        arch_weight = math.exp(- (math.pow(max(w1, w2) - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)
    else:
        arch_weight = math.exp(CYC_SIG_PARAM_1 / (math.pow(w1, CYC_SIG_PARAM_2) + math.pow(w2, CYC_SIG_PARAM_2)))
    #arch_weight_1  = math.exp(- (math.pow(w1 - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)
    #arch_weight_2  = math.exp(- (math.pow(w2 - CYC_SIG_PARAM_3, 2)) / CYC_SIG_PARAM_4)

    inter_weight = 1
    #inter_weight = 0
    for link in inter_edges:
        if link[0] in s1_nodes["leaves"]:
            if link[1] in s2_nodes["leaves"]:
                if link[2]["weight"] < inter_weight:
                #if link[2]["weight"] > inter_weight:
                    inter_weight = link[2]["weight"]

    #ret = arch_weight * inter_weight
    ret = arch_weight * math.exp(inter_weight*2)
    #print "[DBG]: arc = " + str(arch_weight) + " ws = " + str(math.exp(inter_weight*2))
    #print "[DBG]: arc = " + str(arch_weight) + " ws = " + str(inter_weight)
    if arch_weight == 1.0:
        print "[DBG]: w1 = %d" % w1
        print "[DBG]: w2 = %d" % w2
    return ret, s1_nodes["leaves"]+s2_nodes["leaves"]


def sim_from_tree_pair_graph(inter_edges, graph, tree_1, tree_2):
    cycle_weights = []
    words_with_tags = []
    if len(inter_edges) < 2:
        return 0, [], []
    if SIMPLE_CYCLES:
        min_cycle_basis = find_simple_cycles(graph, tree_1, tree_2)
    else:
        min_cycle_basis = find_min_cycle_basis(graph, tree_1, tree_2)
    for cycle in min_cycle_basis:
        if len(cycle) < 3:
            print "[ERR]: Invalid cycle in the basis: "
            print cycle
            continue
        cw, leaves = cal_cycle_weight(cycle, inter_edges)
        cycle_weights.append(cw)
        words_with_tags = words_with_tags + leaves
    return sum(cycle_weights), min_cycle_basis, words_with_tags


def sent_pair_sim(sent_treestr_1, sent_treestr_2):
    tp_graph, inter_edges, tree_1, tree_2 = treestr_pair_to_graph(sent_treestr_1, sent_treestr_2, 's1', 's2')
    sim, min_cycle_basis, words_with_tags = sim_from_tree_pair_graph(inter_edges, tp_graph, tree_1, tree_2)
    words = []
    for word in words_with_tags:
        words.append(lowercase_word_by_ner(word))
        #words.append(re.split('[:#]', word)[2])
    # pid = os.getpid()
    # proc = psutil.Process(pid)
    #print "[DBG]: sent_pair_sim before term"
    # proc.terminate()
    #print "[DBG]: sent_pair_sim after term"
    if sim != 0:
        #print "----------------------------------------"
        #print "[DBG]: sent 1 = " + sent_treestr_1
        #print "[DBG]: sent 2 = " + sent_treestr_2
        #print "[DBG]: sent sim = " + str(sim)
        pass
    else:
        return 0, [], words
    return sim, min_cycle_basis, words


def sim_procs_cool_down(l_sim_proc):
    #print "[DBG]: start a cool-down."
    while(len(l_sim_proc) != 0):
        #print "[DBG]: sim_procs_cool_down:" + str(len(l_sim_proc))
        for proc in l_sim_proc:
            if proc.pid != os.getpid():
                proc.join(1)
            if not proc.is_alive():
                #print "[DBG]: " + str(proc.pid) + " is removed."
                l_sim_proc.remove(proc)
    #print "[DBG]: done a cool-down."

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
        print "[ERR]: Invalid input doc!"
        print "l_sent_treestr_1 = "
        print l_sent_treestr_1
        print "l_sent_treestr_2 = "
        print l_sent_treestr_2
        #print "create process: " + str(p.pid)
        if SAVE_CYCLES:
            out_json = {'sim': 0, 'sentence_pair': {}, 'word_list': []}
        else:
            out_json = [0, ""]
        write_intermedia_to_file(doc1[0],doc2[0], out_json)
        end = time.time()
        return 0
    #print "[DBG]: parent pid = " + str(os.getpid())
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
            #TODO:
            sim_res, min_cycle_basis, word_list = sent_pair_sim(sent_treestr_1, sent_treestr_2)
            #cycle_count += len(min_cycle_basis)
            #print "[DBG]: len of min_cycle_basis is %s" % len(min_cycle_basis)
            if len(min_cycle_basis) > 0:
                l_cyc_count_per_sent_pair.append(1)
            else:
                l_cyc_count_per_sent_pair.append(0)
            #print "[DBG]: l_cyc_count_per_sent_pair = "
            #print l_cyc_count_per_sent_pair

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
    #count_cycles_and_record(doc1[0], doc2[0], cycle_count)
    norm_cycle_count = (cycle_count / 150.0) * 5
    #doc_sim = doc_sim * math.exp(norm_cycle_count)
    #doc_sim += cycle_count
    # calculate the doc sim weight w.r.t. the distribution of cycles
    divgrad_cyc_count = []
    for i in range(len(l_cyc_count_per_sent_pair)):
        for j in range(i+1, len(l_cyc_count_per_sent_pair)):
            divgrad_cyc_count.append(math.pow(l_cyc_count_per_sent_pair[i] - l_cyc_count_per_sent_pair[j], 2))
    cyc_dist_weight = math.exp(- math.sqrt(sum(divgrad_cyc_count) / len(l_cyc_count_per_sent_pair)))
    old_doc_sim = doc_sim
    if CYC_DIST:
        doc_sim = old_doc_sim * cyc_dist_weight
        print "[DBG]: divgrad_cyc_count = "
        print divgrad_cyc_count
        print "[DBG]: cyc_dist_weight = %s, old_doc_sim = %s, new_doc_sim = %s" % (cyc_dist_weight, old_doc_sim, doc_sim)
        print "===================="
    if SAVE_CYCLES:
        out_json = {'sim': doc_sim, 'sentence_pair': sentence_pair, 'word_list': list(set(doc_word_list))}
    else:
        out_json = [doc_sim, ','.join(set(doc_word_list))]
    write_intermedia_to_file(doc1[0],doc2[0], out_json)
    end = time.time()
    print "[DBG]: Doc pair costs %s" % (end - start)
    # with open('/home/fcmeng/workspace/doc_pair_sim/%s-%s' % (, 'a+') as f_out:
    #     json.dumps(out_json,f_out)
    #     f_out.close()
            #remove for-loop
            #if (proc_id+5000*proc_batch) >= 4940000: 
            # RECV_PORT += 1
            # RECV_PORT = RECV_PORT % 50000
            # if RECV_PORT <= 2001:
            # #     RECV_PORT += 2001
            # p = multiprocessing.Process(target = sent_pair_sim, args=(sent_treestr_1, sent_treestr_2, sim_arr, sim_arr_i, RECV_PORT))
            # proc_id += 1
            # sim_procs.append(p)
            # sim_arr_i += 1
            # p.start()
            #else:
            #    proc_id += 1
            #    sim_arr_i += 1

            # if proc_id >= 5000:
            #     proc_batch += 1
            #     proc_id = 0
            #     print "[DBG]: task count = " + str(proc_batch*5000+proc_id)
            #     print "[DBG]: sim array ="
            #     print "----------------------------------------"
            #     print sum(sim_arr)
            #     print "----------------------------------------"
            #elif len(l_sent_treestr_1)*len(l_sent_treestr_2)-(proc_batch*5000+proc_id) <= 5000:
                #print "[DBG]: task count = " + str(proc_batch*5000+proc_id)
                #print "[DBG]: sim array ="
                #print "----------------------------------------"
                #print sim_arr[:]
                #print sum(sim_arr)
                #print "----------------------------------------"
            #print "create process: " + str(p.pid)
            # if len(sim_procs) >= MAX_PROC:
                #print "[DBG]: cool down 1"
                # sim_procs_cool_down(sim_procs)
                #print "[DBG]: sim procs:" + str(len(sim_procs)) 
            #p.join()
    #print "[DBG]: cool down 2"
    # sim_procs_cool_down(sim_procs)
    # print "[DBG]: sim array ="
    # print "----------------------------------------"
    # print sim_arr[:]
    # print sum(sim_arr)
    # print "----------------------------------------"
    
    #print "[DBG]: doc_pair_sim is done!"
    #print "[DBG]: " + " ".join(map(str, sim_arr))
    # ret = sum(sim_arr)
    # print "[DBG]: " + "final doc sim = " + str(ret)
    # return ret


def isValidTree(tree_str):
    if tree_str == "" or tree_str == "ROOT":
        return False
    return True


def validate_doc_trees(doc1_tree, doc2_tree):
    valid_doc1_tree = list(filter(lambda s: isValidTree(s), doc1_tree.split('|')))
    valid_doc2_tree = list(filter(lambda s: isValidTree(s), doc2_tree.split('|')))
    return valid_doc1_tree, valid_doc2_tree


# this function compute the text similarity between two users given a text data within a specified period for each user.
def text_sim(db_cur):
    # db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = 'talk.politics.mideast/77290' OR doc_id = 'talk.politics.mideast/77256'")
    rows = db_cur.fetchall()
    total_doc_pair_count = (len(rows)*(len(rows)-1))/2
    print "[INF]: Total doc-pairs = %s" % total_doc_pair_count 
    sim_procs = []
    proc_id = 0
    proc_batch = 0
    for i, doc1 in enumerate(rows):
        for j, doc2 in enumerate(rows):
            if i<j:
                valid_doc1, valid_doc2 = validate_doc_trees(doc1[1], doc2[1])
                print "[DBG]: doc pairs = %s : %s" % (doc1[0], doc2[0])
                p = multiprocessing.Process(target=doc_pair_sim, args=((doc1[0],valid_doc1), (doc2[0],valid_doc2)))
                proc_id += 1
                sim_procs.append(p)
                p.start()
                if proc_id >= PROC_BATCH_SIZE:
                    proc_batch += 1
                    proc_id = 0
                    #print "[DBG]: task count = " + str(proc_batch * PROC_BATCH_SIZE + proc_id)
                    #print "[DBG]: sim array ="
                if len(sim_procs) >= MAX_PROC:
                    # print "[DBG]: cool down 1"
                    sim_procs_cool_down(sim_procs)
                    print "[INF]: {0:.0%} done!".format(float(proc_batch*PROC_BATCH_SIZE) / float(total_doc_pair_count))
                # print "[DBG]: sim procs:" + str(len(sim_procs))
            # p.join()
        # print "[DBG]: cool down 2"
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count)/float(total_doc_pair_count))


    # print "[DBG]: total task: " + str(len(l_sent_treestr_1)*len(l_sent_treestr_2))
    # sim = doc_pair_sim(l_sent_treestr_1, l_sent_treestr_2, len(l_sent_treestr_1)*len(l_sent_treestr_2))
    # return sim

# this function is used to compare each lee doc with all leebg docs
# we use this function is to obtain GPs for lee docs
def text_sim_lee_vs_leebg(lee_db_cur, leebg_db_cur):
    print "[INF]: Lee vs LeeBG ..."
    lee_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    #db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '13' OR doc_id = '31'")
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
            p = multiprocessing.Process(target=doc_pair_sim, args=((doc1[0],valid_doc1), (doc2[0],valid_doc2)))
            proc_id += 1
            sim_procs.append(p)
            p.start()
            if proc_id >= PROC_BATCH_SIZE:
                proc_batch += 1
                proc_id = 0
            if len(sim_procs) >= MAX_PROC:
                # print "[DBG]: cool down 1"
                sim_procs_cool_down(sim_procs)
                print "[INF]: {0:.0%} done!".format(float(proc_batch*PROC_BATCH_SIZE) / float(total_doc_pair_count))
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count)/float(total_doc_pair_count))

# this function is used for Li65 sentence comparison
def text_sim_li65(col1_db_cur, col2_db_cur):
    print "[INF]: Li65 sentence comparison ..."
    col1_db_cur.execute('SELECT doc_id, parse_trees FROM docs WHERE parse_trees is NOT null order by doc_id')
    #db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '13' OR doc_id = '31'")
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
    for i in range(len(col1_rows)):
        valid_doc1, valid_doc2 = validate_doc_trees(col1_rows[i][1], col2_rows[i][1])
        p = multiprocessing.Process(target=doc_pair_sim, args=((col1_rows[i][0],valid_doc1), (col2_rows[i][0],valid_doc2)))
        proc_id += 1
        sim_procs.append(p)
        p.start()
        if proc_id >= PROC_BATCH_SIZE:
            proc_batch += 1
            proc_id = 0
        if len(sim_procs) >= MAX_PROC:
            # print "[DBG]: cool down 1"
            sim_procs_cool_down(sim_procs)
            print "[INF]: {0:.0%} done!".format(float(proc_batch*PROC_BATCH_SIZE) / float(total_doc_pair_count))
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count)/float(total_doc_pair_count))

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
        #msr_db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '1984954'")
        msr_row = msr_db_cur.fetchone()
        if msr_row[1] is None or len(msr_row[1]) == 0:
            print "[ERR]: An incorrect parse tree for %s" % sent_1_idx
            continue
        else:
           sent_pair.append(sent_1_idx)
           sent_pair.append(msr_row[1])
        #msr_db_cur.execute("SELECT doc_id, parse_trees FROM docs WHERE doc_id = '1984531'")
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
        p = multiprocessing.Process(target=doc_pair_sim, args=((sent_pair[0],valid_doc1), (sent_pair[2],valid_doc2)))
        proc_id += 1
        sim_procs.append(p)
        p.start()
        if proc_id >= PROC_BATCH_SIZE:
            proc_batch += 1
            proc_id = 0
        if len(sim_procs) >= MAX_PROC:
            # print "[DBG]: cool down 1"
            sim_procs_cool_down(sim_procs)
            print "[INF]: {0:.0%} done!".format(float(proc_batch*PROC_BATCH_SIZE) / float(total_doc_pair_count))
    sim_procs_cool_down(sim_procs)
    print "[INF]: {0:.0%} done!".format(float(total_doc_pair_count)/float(total_doc_pair_count))

def main():
#=========================================================
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
        li65_col1_db_conn = sqlite3.connect(LI65_COL1_CONN_STR)
        li65_col2_db_conn = sqlite3.connect(LI65_COL2_CONN_STR)
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
    #arc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #arc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #arc_sock.sendto('done', ('localhost', 9103))
    #arc_sock.close()
#=========================================================
    #err_sent_tree = Tree.fromstring(err_sent_tree_str)
    #print err_sent_tree
    #err_sent_tree_graph = treestr_to_graph(err_sent_tree_str, 's1')
    #print err_sent_tree_graph.edges()
    #con_sent_tree = Tree.fromstring(con_sent_tree_str)
    #t_production = con_sent_tree.productions()
    #print con_sent_tree
    #print t_production
    #sent_tree = treestr_to_graph(con_sent_tree_str, 's1')
    #print sent_tree
    #find_inter_edges(sent_tree, sent_tree)

    #tp_graph, inter_edges, tree_1, tree_2 = treestr_pair_to_graph(test_sent_tree_str_1, test_sent_tree_str_1, 's1', 's2')
    #sim = sim_from_tree_pair_graph(inter_edges, tp_graph, tree_1, tree_2)
    #arr = multiprocessing.Array(ctypes.c_double, 10)
    #sim = sent_pair_sim(test_sent_tree_str_1, test_sent_tree_str_2, arr, 0) 

    #db_conn = sqlite3.connect('/home/fcmeng/gh_data/Events/201708/user_text_clean_2017_08.db') 
    #l_sent_treestr_1 = fetchTreeStrFromDB(db_conn,"d--BOWLtOdsBjJ3gd6f6CQ", "2017-08-01T00:00:00Z", "2017-09-01T00:00:00Z")
    #l_sent_treestr_2 = fetchTreeStrFromDB(db_conn,"I6Q3h_7eGc8bBB6dC4oTxg", "2017-08-01T00:00:00Z", "2017-09-01T00:00:00Z")
    #print len(l_sent_treestr_1)*len(l_sent_treestr_2)
    #sim = doc_pair_sim(l_sent_treestr_1, l_sent_treestr_2, len(l_sent_treestr_1)*len(l_sent_treestr_2))

    #tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #tmp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #tmp_sock.bind((socket.gethostbyaddr("127.0.0.1")[0], RECV_PORT))
    #serv_addr = ('127.0.0.1', 8306)
    #try:
    #    tmp_sock.sendto('beautiful#kick', serv_addr)
    #    sim, serv = tmp_sock.recvfrom(4096)
    #    sim = float(sim)
    #except socket.error, msg:
    #    print "Cannot get word similarity!"
    #    print msg
    #finally:
    #    tmp_sock.close()
    
        
    
    #print "----------------------------------------"
    #print l_sent_treestr_1
    #print "----------------------------------------"
    #print l_sent_treestr_2
    #print "----------------------------------------"
    #print sim
    #print "----------------------------------------"


    #plt.subplot(111)
    #nx.draw(sent_tree, with_labels=True, font_weight='bold')
    #plt.show()

    #send_wordsim_request('oo', ['06387980n', '06388579n'], ['03588414n'])

main()
