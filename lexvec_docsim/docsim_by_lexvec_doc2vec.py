import pandas as pd
import scipy.spatial.distance as scipyd
import sqlite3
import os
import time


def update_sim(doc_key, sim, count, doc_key1=None):
    if doc_key1:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=? or doc_id_pair=?' % col_name, (sim, doc_key, doc_key1))
    else:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=?' % col_name, (sim,doc_key))
    if count % 10000 == 0:
        conn.commit()
        print("%s/31125 done. Time: %s" % (count, time.time()-start))


def compare_two_apvs(col1_vect, col2_vect, dis_mode):
    if dis_mode == 'cosine':
        return 1.0-scipyd.cosine(col1_vect, col2_vect)


def main(apv_csv_file):
    global dataset, data_path, col_name
    dataset = apv_csv_file[:apv_csv_file.find('_')]
    data_path = '%s/workspace/data/docsim/' % os.environ['HOME']
    # col_name = apv_csv_file[apv_csv_file.find('_') + 1:].replace('_matrix', '').replace('.csv', '')
    col_name = 'lexvec_context'

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    global start

    apv_df = pd.read_csv(data_path+apv_csv_file, index_col=0)
    doc_ids = list(apv_df.columns.values)
    print("Total %s documents" % len(doc_ids))
    cnt = 1
    start = time.time()
    for i, doc1 in enumerate(doc_ids):
        for j, doc2 in enumerate(doc_ids[i+1:]):
            sim = compare_two_apvs(apv_df[doc1], apv_df[doc2], 'cosine')
            if 'lee' in dataset:
                update_sim(str(int(doc1)+1) + "#" + str(int(doc2)+1), sim, cnt, str(int(doc2)+1) + "#" + str(int(doc1)+1))
            elif 'reuters' in dataset or 'bbc' in dataset:
                update_sim(doc1+"#"+doc2, sim, cnt, doc2+"#"+doc1)
            else:
                update_sim(doc1.replace("_", "/")+"#"+doc2.replace("_", "/"), sim, cnt, doc2.replace("_", "/")+"#"+doc1.replace("_", "/"))
            cnt += 1
    conn.commit()
    cur.close()
    conn.close()

    print("[DBG]: Total time elapse = %s" % (time.time() - start))
    print("ALL DONE!")


main('20news50short10_lexvec_doc2vec_matrix.csv')
# main('reuters_lexvec_doc2vec_matrix.csv')
# main('bbc_lexvec_doc2vec_matrix.csv')
# main('leefixsw_lexvec_doc2vec_matrix.csv')
