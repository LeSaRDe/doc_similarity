import os
import json
import sqlite3
import pandas as pd
import build_single_doc_apv
import time
import idx_bit_translate
import multiprocessing

PHRASE_CLUSTER_METHOD = 'rbsc'
PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']


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
    cur.execute('''SELECT doc_id from docs order by doc_id''')
    rows = cur.fetchall()
    return rows


def get_word_pair_sims():
    pairwise_sims = dict()
    all_sims = cur.execute('SELECT * from words_sim WHERE sim > 0.4 order by word_pair_idx').fetchall()
    print "Sim > 0.4, total word pairs=%s " % len(all_sims)
    all_words_idx = cur.execute('SELECT * from words_idx').fetchall()
    all_idx_word = dict()
    for word, idx in all_words_idx:
        all_idx_word[idx] = word
    for idx, sim in all_sims:
        w1_idx, w2_idx = idx_bit_translate.key_to_keys(idx)
        pairwise_sims[all_idx_word[w1_idx]+"#"+all_idx_word[w2_idx]] = sim
    return pairwise_sims


def build_apv_matrix(phrase_cluster_by_clusterid, folder):
    global cluster_size
    cluster_ids = phrase_cluster_by_clusterid.keys()
    cluster_size = len(cluster_ids)
    print "Cluster size=%s" % cluster_size
    doc_ids = get_doc_ids()
    word_pair_sims = get_word_pair_sims()

    apv_df = pd.DataFrame(cluster_ids, columns=['cluster_id'])

    apv_processes = []
    for i, each_doc in enumerate(doc_ids):
        top_sim_docs = find_top_sim_doc_pairs(each_doc[0])
        start = time.time()
        apv_vec = build_single_doc_apv.build_single_doc_apv(phrase_cluster_by_clusterid=phrase_cluster_by_clusterid,
                                                            word_pair_sims=word_pair_sims,
                                                            target_doc_id=each_doc,
                                                            compare_sim_docs=top_sim_docs,
                                                            json_files_path=folder,
                                                            in_cur=cur)
        print "[%s]Time: %s" % (i, time.time()-start)
        apv_df[each_doc[0]] = apv_df['cluster_id'].map(apv_vec)
    return apv_df


def proc_cool_down(apv_processes):
    while len(apv_processes) >= multiprocessing.cpu_count():
        for proc in apv_processes:
            if proc.pid != os.getpid():
                proc.join(1)
            if not proc.is_alive():
                apv_processes.remove(proc)


def build_apv_matrix_parallel(phrase_cluster_by_clusterid, folder):
    global cluster_size
    cluster_ids = phrase_cluster_by_clusterid.keys()
    cluster_size = len(cluster_ids)
    print "Cluster size=%s" % cluster_size
    doc_ids = get_doc_ids()
    word_pair_sims = get_word_pair_sims()

    apv_processes = []
    for i, each_doc in enumerate(doc_ids):
        top_sim_docs = find_top_sim_doc_pairs(each_doc[0])
        p = multiprocessing.Process(target=build_single_doc_apv.build_single_doc_apv,
                                    args=(phrase_cluster_by_clusterid, word_pair_sims, each_doc, top_sim_docs, folder, cur))
        apv_processes.append(p)
        p.start()
        proc_cool_down(apv_processes)


def find_top_sim_doc_pairs(doc_id):
    if TOP_SIM_DOCS == -1:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like "%%%s%%" order by "%s" DESC''' % (doc_id, col_name)
    else:
        query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like "%%%s%%" order by "%s" DESC limit %s''' % (doc_id, col_name, TOP_SIM_DOCS)
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def main(folder):
    global dataset, data_path, col_name
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = folder[folder.find('_') + 1:]

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    # !!MUST confirm that the following 2 files are one to one match, otherwise the result is WRONG!!
    # with open(data_path+dataset+'_'+PHRASE_CLUSTER_METHOD+"_phrase_clusters_by_phrase.json",'r') as infile:
    #     phrase_cluster_by_phrase = json.load(infile)
    # infile.close()

    with open(data_path+folder+"_phrase_clusters_by_clusterid.json",'r') as infile:
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
    full_doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
                     'rec.motorcycles': 4, 'misc.forsale': 5,'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
                     'talk.politics.mideast': 9}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.med': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
    #                   'talk.politics.mideast': 4}
    doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
                      'rec.motorcycles': 4, 'misc.forsale': 5, 'sci.med': 6, 'sci.electronics': 7,
                      'rec.sport.hockey': 8, 'talk.politics.mideast': 9}
    # if TOP_SIM_DOCS is set to -1, then all documents are taken into account.
    TOP_SIM_DOCS = -1
    #TOP_SIM_DOCS = 30

    parallel = False

    if parallel:
        build_apv_matrix_parallel(phrase_cluster_by_clusterid, data_path + folder)
    else:
        apv_matrix = build_apv_matrix(phrase_cluster_by_clusterid, data_path+folder)
        apv_matrix.to_csv(data_path+dataset+'_'+PHRASE_CLUSTER_METHOD+'_apv_matrix_hard.csv', sep=',', index=False)


if __name__ == '__main__':
    # must have the phrase cluster saved in json files
    main("20news50short10_nasari_40_rmswcbwexpws_w3-3")