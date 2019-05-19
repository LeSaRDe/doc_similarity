import pandas as pd
import scipy.spatial.distance as scipyd
import sqlite3
import os
import time
import js_div
from scipy.special import softmax


def update_sim(doc_key, sim, count, doc_key1=None):
    if doc_key1:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=? or doc_id_pair=?' % col_name, (sim, doc_key, doc_key1))
    else:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=?' % col_name, (sim,doc_key))
    if count % 10000 == 0:
        conn.commit()
        print "%s/31125 done. Time: %s" % (count, time.time()-start)


def compare_two_apvs(col1_vect, col2_vect, dis_mode):
    if dis_mode == 'cosine':
        return 1.0-scipyd.cosine(col1_vect, col2_vect)
    elif dis_mode == 'euclidean':
        return 1.0 / (1.0 + scipyd.euclidean(col1_vect, col2_vect))
    elif dis_mode == 'js_div_rbf':
        return js_div.js_div_rbf(softmax(col1_vect).tolist(), softmax(col2_vect).tolist())


def main(apv_csv_file):
    global dataset, data_path, col_name
    dataset = apv_csv_file[:apv_csv_file.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = apv_csv_file[apv_csv_file.find('_') + 1:].replace('_matrix', '').replace('.csv', '')
    # col_name = col_name + '_ws30'

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    global start

    apv_df = pd.read_csv(data_path+apv_csv_file, index_col=0)
    #apv_df = pd.read_csv(data_path + apv_csv_file)
    doc_ids = list(apv_df.columns.values)
    print "Total %s documents" % len(doc_ids)
    cnt = 1
    start = time.time()
    for i, doc1 in enumerate(doc_ids):
        for j, doc2 in enumerate(doc_ids[i+1:]):
            sim = compare_two_apvs(apv_df[doc1], apv_df[doc2], 'cosine')
            #sim = compare_two_apvs(apv_df[doc1], apv_df[doc2], 'js_div_rbf')
            if '20news50short' in dataset:
                update_sim(doc1.replace("_", "/")+"#"+doc2.replace("_", "/"), sim, cnt, doc2.replace("_", "/")+"#"+doc1.replace("_", "/"))
            elif 'reuters' in dataset or 'bbc' in dataset:
                update_sim(doc1 + "#" + doc2, sim, cnt, doc2 + "#" + doc1)
            cnt += 1
    conn.commit()
    cur.close()
    conn.close()

    print "[DBG]: Total time elapse = %s" % (time.time() - start)
    print "ALL DONE!"


# main('20news50short10_nasari_30_rmswcbwexpws_w3-3_top5ws30_pred_apv_matrix.csv')
main('bbc_nasari_30_rmswcbwexpws_w3-3_top5ws30_bi_apv_matrix.csv')