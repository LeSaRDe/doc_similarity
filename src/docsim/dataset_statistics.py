from gensim.parsing.preprocessing import preprocess_string
import sqlite3
import csv
import os
import json


def write_to_csv(header, all_docs):
    with open('%s_statistics.csv' % dataset, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header, delimiter=',')
        writer.writeheader()
        writer.writerows(all_docs)
    csvfile.close()


def count_words_by_gensim(pre_ner_txt):
    return len(preprocess_string(pre_ner_txt))


def count_vec(doc_id, path):
    cnt = 0
    with open(path + doc_id + ".json", "r") as apv_file:
        apv_bi = json.load(apv_file)
        for e in apv_bi.values():
            if e > 0:
                cnt+=1
    return cnt


def get_cluster_cnt(file_name):
    with open(file_name, "r") as cluster_file:
        cluster = json.load(cluster_file)
    cnt_clusters = len(cluster.keys())
    cnt_GPs = 0
    for nodes in cluster.values():
        cnt_GPs += len(nodes)
    return cnt_GPs, cnt_clusters


def main(dataset_name):
    global dataset
    dataset = dataset_name
    conn = sqlite3.connect("/home/fcmeng/workspace/data/%s.db" % dataset)
    cur = conn.cursor()

    all_docs = dict()

    keys = ["id", "#ofwords", "#ofGPs", "APV-BI", "APV-Pred", "GPGS"]

    all_sim_docs_path = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3/" % dataset
    all_sim_docs_files = os.listdir(all_sim_docs_path)

    apv_bi_path = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3_top5ws30_bi_apv_vec/" % dataset
    apv_pred_path = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3_top5ws30_pred_apv_vec/" % dataset
    gpgs_path = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3_doc_pv/" % dataset

    docs = cur.execute("SELECT doc_id, pre_ner FROM docs order by doc_id").fetchall()
    for d in docs:
        doc = dict.fromkeys(keys)
        if "/" in d[0]:
            doc['id'] = d[0].replace("/", "_")
        else:
            doc['id'] = d[0]
        doc['#ofwords'] = count_words_by_gensim(d[1])
        doc['APV-BI'] = count_vec(doc['id'], apv_bi_path)
        doc['APV-Pred'] = count_vec(doc['id'], apv_pred_path)
        doc['GPGS'] = count_vec(doc['id'], gpgs_path)
        all_docs[doc['id']] = doc

    for doc_pair in all_sim_docs_files:
        infile = open(all_sim_docs_path + doc_pair, "r")
        all_cycles = json.load(infile)['sentence_pair'].values()
        infile.close()
        doc1, doc2 = doc_pair.replace(".json", "").split("#")
        total_cnt_cycles = 0
        for each in all_cycles:
            total_cnt_cycles = total_cnt_cycles + len(each['cycles'])
        if all_docs[doc1]["#ofGPs"] is not None:
            all_docs[doc1]["#ofGPs"] += total_cnt_cycles
        else:
            all_docs[doc1]["#ofGPs"] = total_cnt_cycles
        if all_docs[doc2]["#ofGPs"] is not None:
            all_docs[doc2]["#ofGPs"] += total_cnt_cycles
        else:
            all_docs[doc2]["#ofGPs"] = total_cnt_cycles

    write_to_csv(keys, all_docs.values())

    print "Total # of words: %s" % cur.execute("SELECT count(word) FROM words_idx").fetchone()[0]

    apv_bi_cluster_file = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3_bi_phrase_clusters_by_clusterid.json" % dataset
    apv_pred_cluster_file = "/home/fcmeng/workspace/data/%s_nasari_30_rmswcbwexpws_w3-3_pred_phrase_clusters_by_clusterid.json" % dataset
    apv_bi_num_GPs, apv_bi_num_clusters = get_cluster_cnt(apv_bi_cluster_file)
    print "Total # of APV-BI GPs: %s" % apv_bi_num_GPs
    print "Total # of APV-BI GP Cluster: %s" % apv_bi_num_clusters
    apv_pred_num_GPs, apv_pred_num_clusters = get_cluster_cnt(apv_pred_cluster_file)
    print "Total # of APV-Pred GPs: %s" % apv_pred_num_GPs
    print "Total # of APV-Pred GP Cluster: %s" % apv_pred_num_clusters


main('20news50short10')