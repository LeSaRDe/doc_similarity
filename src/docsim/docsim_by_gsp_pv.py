import os
import json
import numpy as np
import scipy.spatial.distance as scipyd
import copy
import sqlite3
import time
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading
import multiprocessing
import ctypes


MAX_THREADS = 10


def update_sim(doc_key, sim, count, doc_key1=None, db_path=None):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    if doc_key1:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=? or doc_id_pair=?' % col_name, (sim, doc_key, doc_key1))
    else:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=?' % col_name, (sim,doc_key))
    if count >= 10000 and count % 10000 == 0:
        conn.commit()
        print "%s/124750 done. Time: %s" % (count, time.time()-start)
    conn.close()


def compare_two_pvs(doc1_vec_list, doc2_vec_list, dis_mode):
    doc1_np = np.asarray(doc1_vec_list)
    doc2_np = np.asarray(doc2_vec_list)
    if dis_mode == 'cosine':
        return 1.0-scipyd.cosine(doc1_np, doc2_np)
    elif dis_mode == 'euclidean':
        return 1.0 / (1.0 + scipyd.euclidean(doc1_np, doc2_np))


def get_unique_phrases(doc1_keys, doc2_keys):
    unique_keys = copy.deepcopy(doc1_keys)
    for each_key in doc2_keys:
        each_pair = each_key.split('#')
        if len(each_pair) != 2:
            raise Exception("Phrase pair length not equal to 2!!")
        if each_key in unique_keys or (each_pair[1] + '#' + each_pair[0]) in unique_keys:
            pass
        else:
            unique_keys.append(each_key)
    return unique_keys


def get_uniform_vecs_for_two_docs(doc1_dict, doc2_dict):
    doc1_uni_dict = copy.deepcopy(doc1_dict)
    doc2_uni_dict = dict.fromkeys(doc1_dict.keys(), 0)
    for key in doc2_dict.keys():
        if key not in doc1_uni_dict.keys():
            doc1_uni_dict[key] = 0
        doc2_uni_dict[key] = doc2_dict[key]
    doc1_uni_vec = []
    doc2_uni_vec = []
    for key in doc1_uni_dict:
        doc1_uni_vec.append(doc1_uni_dict[key])
        doc2_uni_vec.append(doc2_uni_dict[key])
    return doc1_uni_vec, doc2_uni_vec


def sim_thread_run(doc1_name, doc2_name, doc1_dict, doc2_dict, sim_array, index, db_path):
    doc1_uni_vec, doc2_uni_vec = get_uniform_vecs_for_two_docs(doc1_dict, doc2_dict)
    sim = compare_two_pvs(doc1_uni_vec, doc2_uni_vec, 'cosine')
    sim_array[index] = sim
    update_sim(doc1_name.replace("_", "/").replace('.json', '').strip() + "#" + doc2_name.replace("_", "/").replace('.json', '').strip(), sim, index,
               doc2_name.replace("_", "/").replace('.json', '').strip() + "#" + doc1_name.replace("_", "/").replace('.json', '').strip(), db_path)


def thread_cool_down(l_threads, max_threads_threshold):
    done_cnt = 0
    while len(l_threads) >= max_threads_threshold:
        for thread in l_threads:
            thread.join(0.01)
            if not thread.is_alive():
                l_threads.remove(thread)
                done_cnt += 1
    return done_cnt


def main(folder):
    global dataset, data_path, col_name, start
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = folder[folder.find('_') + 1:]

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    start = time.time()
    all_files = os.listdir(data_path+folder)
    size_all_files = len(all_files)
    # sim_array = multiprocessing.Array(ctypes.c_float, size_all_files * (size_all_files-1) / 2)
    sim_array_idx = 0
    l_threads = []
    # thread_done_cnt = 0
    for i, doc1 in enumerate(all_files):
        doc1_vec = json.load(open(data_path+folder+"/"+doc1, 'r'))
        for doc2 in all_files[i+1:]:
            doc2_vec = json.load(open(data_path+folder+"/"+doc2, 'r'))

            doc1_uni_vec, doc2_uni_vec = get_uniform_vecs_for_two_docs(doc1_vec, doc2_vec)
            sim = compare_two_pvs(doc1_uni_vec, doc2_uni_vec, 'cosine')
            update_sim(
                doc1.replace("_", "/").replace('.json', '').strip() + "#" + doc2.replace("_", "/").replace(
                    '.json', '').strip(), sim, sim_array_idx,
                doc2.replace("_", "/").replace('.json', '').strip() + "#" + doc1.replace("_", "/").replace(
                    '.json', '').strip(), "%s%s.db" % (data_path, dataset))

            # TODO
            # fix commit issue for multithreading
            # thread = _threading.Thread(target=sim_thread_run,
            #                            args=(doc1, doc2, doc1_vec, doc2_vec, sim_array, sim_array_idx, "%s%s.db" % (data_path, dataset)))
            # l_threads.append(thread)
            # thread.start()
            # thread_done_cnt += thread_cool_down(l_threads, MAX_THREADS)
            # if thread_done_cnt >= 500 and thread_done_cnt % 500 == 0:
            #     conn.commit()
            #     print "%s/%s done. Time: %s" % (thread_done_cnt, size_all_files * (size_all_files-1) / 2, time.time() - start)

            # print "%s - %s: %s" % (doc1, doc2, sim)
            sim_array_idx += 1

    # thread_done_cnt += thread_cool_down(l_threads, 1)
    conn.commit()
    print "%s/%s done. Time: %s" % (sim_array_idx, size_all_files * (size_all_files - 1) / 2, time.time() - start)
    conn.close()


main("20news50short10_nasari_30_rmswcbwexpws_w3-3_doc_pv")