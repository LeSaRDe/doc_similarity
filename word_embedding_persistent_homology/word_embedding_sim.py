import sqlite3
import os
import time
from gensim.models import KeyedVectors
import scipy.spatial.distance as scipyd
import threading
# import copy
import logging
import word_sim_utils
import math


g_db_path = '/home/fcmeng/workspace/word_embedding_persistent_homology/wordsim.db'
g_word_embedding_model_name = 'glove'


def get_words_from_db(db_cur):
    words = dict()
    db_cur.execute('SELECT * FROM words_idx')
    rows = db_cur.fetchall()
    for row in rows:
        words[row[1]] = row[0]
    return words


def rows_group(word_rows, full_word_dict):
    count = 0
    par = math.ceil(len(word_rows) / 10)
    l_groups = []
    if len(word_rows) > par * 10:
        par_count = 10 + 1
    else:
        par_count = 10
    for k in range(0, par_count):
        l_groups.append([])
    cur_group_idx = 0
    for k in range(0, len(word_rows)):
        word_pair_idx = word_rows[k][0]
        word_i_idx, word_j_idx = word_sim_utils.key_to_keys(word_pair_idx)
        word_i_str = full_word_dict[word_i_idx]
        word_j_str = full_word_dict[word_j_idx]
        if len(l_groups[cur_group_idx]) % par == 0:
            if len(l_groups[cur_group_idx]) != 0:
                # l_groups.append(copy.deepcopy(l_cur_group))
                cur_group_idx += 1
        l_groups[cur_group_idx].append((word_i_str, word_j_str, word_pair_idx))

    # for i in range(0, len(word_rows) - 1):
    #     word_i_str = word_rows[i][0].lower()
    #     word_i_idx = word_rows[i][1]
    #     for j in range(i, len(word_rows)):
    #         word_j_str = word_rows[j][0].lower()
    #         word_j_idx = word_rows[j][1]
    #         if len(l_cur_group) % par == 0:
    #             l_groups.append(l_cur_group)
    #             l_cur_group = []
    #         l_cur_group.append((word_i_str, word_j_str, word_i_idx, word_j_idx))
    # if len(l_cur_group) != 0:
    #     l_groups.append(l_cur_group)
    return l_groups


def word_sim_thread(t_name, model, db_lock, l_workload):
    logging.debug('Thread starts with %s word sims ...' % len(l_workload))
    # print '[INF]: Thread %s starts ...' % t_name
    l_wordsims = []
    word_sim = 0.0
    start = time.time()
    for k in range(0, len(l_workload)):
        word_i_str = l_workload[k][0].lower()
        word_j_str = l_workload[k][1].lower()
        word_pair_idx = l_workload[k][2]
        try:
            word_i_vec = model.wv[word_i_str]
            word_j_vec = model.wv[word_j_str]
            word_sim = 1.0 - scipyd.cosine(word_i_vec, word_j_vec)
        except:
            # print '[WRN]: Either %s or %s does not have any LexVec vector!' % (word_i_str, word_j_str)
            pass
        l_wordsims.append((word_sim, word_pair_idx))
    logging.debug('Done with cosine in %s secs.' % str(time.time() - start))
    # print '[INF]: Thread %s: Done with cosine in %s secs.' % (t_name, time.time() - start)

    db_conn = sqlite3.connect(g_db_path)
    db_cur = db_conn.cursor()
    with db_lock:
        start = time.time()
        count = 0
        for k in range(0, len(l_wordsims)):
            word_sim = l_wordsims[k][0]
            word_pair_idx = l_wordsims[k][1]
            try:
                db_cur.execute('update words_sim set %s = ? where word_pair_idx = ?' % g_word_embedding_model_name, (word_sim, word_pair_idx))
            except:
                logging.error('Word pair %s does not exist.' % str(word_pair_idx))
                # print '[ERR]: Word pair %s does not exist.' % str(word_pair_idx)
            count += 1
            if count % 20000 == 0:
                db_conn.commit()
                logging.debug('Finishes %s word sims in %s sec.' % (str(count), str(time.time() - start)))
                # print '[INF]: Thread %s finishes %s word sims in %s sec.' % (t_name, str(count), time.time() - start)
                start = time.time()
        db_conn.commit()
        logging.debug('Finishes %s word sims in %s sec.' % (str(count), str(time.time() - start)))
        # print '[INF]: Thread %s finishes %s word sims in %s sec.' % (t_name, str(count), time.time() - start)
    db_conn.close()



def load_word_embedding_model(name):
    if name == 'lexvec':
        model_file = '%s/workspace/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W.pos.vectors'
        model = KeyedVectors.load_word2vec_format(model_file, binary=False)
    elif name == 'glove':
        model_file = '%s/workspace/glove/' % os.environ['HOME'] + 'glove_word2vec.txt'
        model = KeyedVectors.load_word2vec_format(model_file, binary=False)
    elif name == 'fasttext':
        model_file = '%s/workspace/fasttext/' % os.environ['HOME'] + 'wiki-news-300d-1M.vec'
        model = KeyedVectors.load_word2vec_format(model_file, binary=False)
    logging.debug('Model loading is done.')
    return model


def main():
    logging.basicConfig(format='%(levelname)s:%(threadName)s:%(message)s', level=logging.DEBUG)
    model = load_word_embedding_model(g_word_embedding_model_name)
    # print '[INF]: Model loading is done.'
    db_conn = sqlite3.connect(g_db_path)
    db_cur = db_conn.cursor()
    db_cur.execute('select word_pair_idx from words_sim where %s is null' % g_word_embedding_model_name)
    word_rows = db_cur.fetchall()
    full_word_dict = get_words_from_db(db_cur)
    logging.debug('Word dict loading is done.')
    db_conn.close()
    l_groups = rows_group(word_rows, full_word_dict)
    l_threads = []
    db_lock = threading.Lock()

    # sim_count = 0
    start = time.time()

    for k in range(0, len(l_groups)):
        t_name = 'ws_thread_' + str(k)
        t = threading.Thread(target=word_sim_thread, args=(t_name, model, db_lock, l_groups[k]))
        t.start()
        l_threads.append(t)
    # for i in range(0, len(word_rows)-1):
    #     word_i_str = word_rows[i][0].lower()
    #     word_i_idx = word_rows[i][1]
    #     for j in range(i, len(word_rows)):
    #         word_j_str = word_rows[j][0].lower()
    #         word_j_idx = word_rows[j][1]
    #         word_sim = 0.0
    #         try:
    #             word_i_vec = model.wv[word_i_str]
    #             word_j_vec = model.wv[word_j_str]
    #             word_sim = 1.0 - scipyd.cosine(word_i_vec, word_j_vec)
    #         except:
    #             print '[WRN]: Either %s or %s does not have any LexVec vector!' % (word_i_str, word_j_str)
    #         try:
    #             db_cur.execute('update words_sim set lexvec = ? where word_pair_idx = ?',
    #                            (word_sim, word_sim_utils.keys_to_key(word_i_idx, word_j_idx)))
    #         except:
    #             db_cur.execute('update words_sim set lexvec = ? where word_pair_idx = ?',
    #                            (word_sim, word_sim_utils.keys_to_key(word_j_idx, word_i_idx)))
    #         sim_count += 1
    #         if sim_count % 5000 == 0:
    #             db_conn.commit()
    #             print '[INF]: Total committed sims = %s in %s secs' % (str(sim_count), time.time()-start)
    #             start = time.time()
    # db_conn.commit()

    # for k in range(0, len(l_threads)):
    #     l_threads[k].join()

    while len(l_threads) != 0:
        for t in l_threads:
        # for k in range(0, len(l_threads)):
            if t.is_alive():
                t.join(30)
            else:
                l_threads.remove(t)
                logging.debug('Thread done!')
                # print '[INF]: Thread %s is done!' % l_threads[k].name

    logging.debug('All done in %s sec' % str(time.time() - start))
    # print '[INF]: All done in % sec.' % time.time() - start


if __name__ == '__main__':
    main()