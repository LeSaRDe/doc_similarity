import os
import json
import sqlite3
import pandas as pd
import build_single_doc_apv
import idx_bit_translate
import multiprocessing
import phrase_graph_utils

PHRASE_CLUSTER_METHOD = 'rbsc'
PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']
WORD_SIM_THRESHOLD = 0.3
TOP_SIM_DOCS = 5


def get_doc_sim_from_db(col):
    # cur.execute('''SELECT doc_id_pair, "'''+col+'''" from docs_sim
    #                 where doc_id_pair not like "%talk.politics.guns%"
    #                 and doc_id_pair not like "%alt.atheism%"
    #                 and doc_id_pair not like "%misc.forsale%"
    #                 and doc_id_pair not like "%sci.space%"
    #                 and doc_id_pair not like "%sci.electronics%"
    #                 order by doc_id_pair''')
    cur.execute('''SELECT doc_id_pair, "''' + col + '''" from docs_sim order by doc_id_pair''')
    rows = cur.fetchall()
    return rows


def get_doc_ids():
    # cur.execute('''SELECT doc_id from docs
    #                 where doc_id not like "%talk.politics.guns%"
    #                 and doc_id not like "%alt.atheism%"
    #                 and doc_id not like "%misc.forsale%"
    #                 and doc_id not like "%sci.space%"
    #                 and doc_id not like "%sci.electronics%"
    #                 order by doc_id''')
    cur.execute('''SELECT doc_id from docs order by doc_id ''')
    rows = cur.fetchall()
    return rows


# def get_word_pair_sims():
#     pairwise_sims = dict()
#     all_sims = cur.execute('SELECT * from words_sim WHERE sim >= %s order by word_pair_idx' % WORD_SIM_THRESHOLD).fetchall()
#     print "Sim >= %s, total word pairs=%s " % (WORD_SIM_THRESHOLD,len(all_sims))
#     all_words_idx = cur.execute('SELECT * from words_idx').fetchall()
#     all_idx_word = dict()
#     for word, idx in all_words_idx:
#         all_idx_word[idx] = word
#     for idx, sim in all_sims:
#         w1_idx, w2_idx = idx_bit_translate.key_to_keys(idx)
#         pairwise_sims[all_idx_word[w1_idx]+"#"+all_idx_word[w2_idx]] = sim
#     return pairwise_sims


def build_apv_matrix(phrase_cluster_by_clusterid, folder):
    global cluster_size
    cluster_ids = phrase_cluster_by_clusterid.keys()
    cluster_size = len(cluster_ids)
    print "Cluster size=%s" % cluster_size
    doc_ids = get_doc_ids()
    word_pair_sims = phrase_graph_utils.get_word_pair_sims()

    apv_df = pd.DataFrame(cluster_ids, columns=['cluster_id'])

    for i, each_doc in enumerate(doc_ids):
        if os.path.isfile(out_apv_files_path+each_doc[0].replace('/', '_')+'.json'):
            print "%s apv vec is already exist." % each_doc[0]
            continue
        top_sim_docs = find_top_sim_doc_pairs(each_doc[0])
        apv_vec = build_single_doc_apv.build_single_doc_apv(phrase_cluster_by_clusterid=phrase_cluster_by_clusterid,
                                                            word_pair_sims=word_pair_sims,
                                                            target_doc_id=each_doc[0],
                                                            compare_sim_docs=top_sim_docs,
                                                            json_files_path=folder,
                                                            out_files_path=out_apv_files_path,
                                                            in_cur=cur)
        apv_df[each_doc[0]] = apv_df['cluster_id'].map(apv_vec)
    return apv_df


def proc_cool_down(apv_processes, max_proc):
    # while len(apv_processes) >= multiprocessing.cpu_count():
    while len(apv_processes) >= max_proc:
        for proc in apv_processes:
            if proc.pid != os.getpid():
                proc.join(0.01)
            if not proc.is_alive():
                apv_processes.remove(proc)


def build_apv_matrix_parallel(phrase_cluster_by_clusterid, folder):
    global cluster_size
    cluster_ids = phrase_cluster_by_clusterid.keys()
    cluster_size = len(cluster_ids)
    print "Cluster size=%s" % cluster_size
    doc_ids = get_doc_ids()
    # doc_ids = [(u'talk.politics.mideast/77262',) ,
    # (u'talk.politics.mideast/77302',) ,
    # (u'talk.politics.mideast/77321',) ,
    # (u'talk.politics.mideast/77324',) ,
    # (u'talk.politics.mideast/77353',) ,
    # (u'talk.politics.mideast/77399',)]
    word_pair_sims = phrase_graph_utils.get_word_pair_sims()

    apv_processes = []
    for i, each_doc in enumerate(doc_ids):
        if os.path.isfile(out_apv_files_path+each_doc[0].replace('/', '_')+'.json'):
            print "%s apv vec is already exist." % each_doc[0]
            continue
        top_sim_docs = find_top_sim_doc_pairs(each_doc[0])
        p = multiprocessing.Process(target=build_single_doc_apv.build_single_doc_apv,
                                    args=(phrase_cluster_by_clusterid, word_pair_sims, each_doc[0], top_sim_docs,
                                          folder, out_apv_files_path, cur))
        apv_processes.append(p)
        p.start()
        proc_cool_down(apv_processes, 10)
    proc_cool_down(apv_processes, 1)


def find_top_sim_doc_pairs(doc_id):
    if TOP_SIM_DOCS == -1:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like doc_id_pair like "%s#%%" or doc_id_pair like "%%#%s" order by "%s"''' % (doc_id, doc_id, col_name)
    else:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like "%s#%%" or doc_id_pair like "%%#%s" order by "%s" DESC limit %s''' % (doc_id, doc_id, col_name, TOP_SIM_DOCS)
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def main(folder):
    global dataset, data_path, col_name, out_apv_files_path
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = folder[folder.find('_') + 1:]

    phrase_graph_utils.init(data_path, dataset)

    out_apv_files_path = data_path+folder+'_top5ws30_bi_apv_vec/'

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()



    # !!MUST confirm that the following 2 files are one to one match, otherwise the result is WRONG!!
    # with open(data_path+dataset+'_'+PHRASE_CLUSTER_METHOD+"_phrase_clusters_by_phrase.json",'r') as infile:
    #     phrase_cluster_by_phrase = json.load(infile)
    # infile.close()

    with open(data_path+folder+"_bi_phrase_clusters_by_clusterid.json",'r') as infile:
        phrase_cluster_by_clusterid = json.load(infile)
    infile.close()

    # ==>(Optional) Check the above 2 dicts match each other
    # len_values = sum([len(each_list) for each_list in phrase_cluster_by_clusterid.values()])
    # if len(phrase_cluster_by_phrase.keys()) != len_values:
    #     raise Exception("Phrases dont match!!")
    # if set(phrase_cluster_by_phrase.values()) != set(phrase_cluster_by_clusterid.keys()):
    #     raise Exception("Cluster keys dont match!!")
    # <====

    global full_doc_categories, doc_categories, TOP_SIM_DOCS
    # full_doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
    #                  'rec.motorcycles': 4, 'misc.forsale': 5,'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
    #                  'talk.politics.mideast': 9}
    # full_doc_categories = {'soybean': 0, 'gold': 1, 'crude': 2, 'livestock': 3, 'acq': 4, 'interest': 5, 'ship': 6}
    full_doc_categories = {'business':0, 'entertainment':1, 'politics':2, 'sport':3, 'tech':4}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.med': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
    #                   'talk.politics.mideast': 4}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
    #                   'rec.motorcycles': 4, 'misc.forsale': 5, 'sci.med': 6, 'sci.electronics': 7,
    #                   'rec.sport.hockey': 8, 'talk.politics.mideast': 9}
    # doc_categories = {'soybean': 0, 'gold': 1, 'crude': 2, 'livestock': 3, 'acq': 4, 'interest': 5, 'ship': 6}
    doc_categories = {'business': 0, 'entertainment': 1, 'politics': 2, 'sport': 3, 'tech': 4}
    # if TOP_SIM_DOCS is set to -1, then all documents are taken into account.
    # TOP_SIM_DOCS = -1
    # TOP_SIM_DOCS = 5

    parallel = True

    if parallel:
        build_apv_matrix_parallel(phrase_cluster_by_clusterid, data_path + folder)
    else:
        apv_matrix = build_apv_matrix(phrase_cluster_by_clusterid, data_path+folder)
        apv_matrix.to_csv(data_path+dataset+'_'+PHRASE_CLUSTER_METHOD+'_apv_matrix_pred.csv', sep=',', index=False)


if __name__ == '__main__':
    # must have the phrase cluster saved in json files
    # main("20news50short10_nasari_30_rmswcbwexpws_w3-3")
    main("bbc_nasari_30_rmswcbwexpws_w3-3")