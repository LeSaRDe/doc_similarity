import socket
import sqlite3
import insert_words_idx
import multiprocessing
import ctypes
import idx_bit_translate
import os
import time
import numpy

MULTI_WS_SERV = False
MULTI_WS_SERV_MOD = 2


# def exe_query(query, query_data=None, need_return=False):
#     cur = conn.cursor()
#     if query_data:
#         cur.execute(query, query_data)
#     else:
#         cur.execute(query)
#     cur.close()


def get_words_from_db():
    cur = conn.cursor()
    words = dict()
    cur.execute('SELECT * FROM words_idx')
    rows = cur.fetchall()
    for row in rows:
        words[row[1]] = row[0]
    cur.close()
    return words


def words_sim_by_nasari(w1, w2):
    global MULTI_WS_SERV
    query = "%s#%s" % (w1, w2)

    send_port = 8306
    if MULTI_WS_SERV:
        millis = int(round(time.time() * 1000))
        millis &= 0xffffffffL
        numpy.random.seed(millis)
        send_port += numpy.random.randint(MULTI_WS_SERV_MOD)
    serv_addr = ('localhost', send_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(query, serv_addr)
    msg, addr = sock.recvfrom(4096)
    return float(msg)


def word_pair_sim(word_pair_idx, full_word_dict):
    w1_idx, w2_idx = idx_bit_translate.key_to_keys(word_pair_idx)
    word1 = full_word_dict[w1_idx]
    word2 = full_word_dict[w2_idx]
    sim = words_sim_by_nasari(str(word1), str(word2))
    # sim_arr[ii] = sim
    return sim


def words_pair_sim(full_word_dict):
    batch_size = 20000

    cur = conn.cursor()
    unsimed_words_pair_cnt = cur.execute("SELECT count(word_pair_idx) FROM words_sim where sim is null").fetchone()[0]
    if unsimed_words_pair_cnt == 0:
        cur.close()
        print "No unsimed word pair."
    else:
        unsimed_words_pair = cur.execute("SELECT word_pair_idx FROM words_sim where sim is null LIMIT ?", (batch_size,)).fetchall()

        if len(unsimed_words_pair) < batch_size:
            batch_size = len(unsimed_words_pair)

        total_finished_sim = 0
        start = time.time()
        while unsimed_words_pair:
            for sim_arr_i, word_pair_idx in enumerate(unsimed_words_pair):
                sim = word_pair_sim(word_pair_idx[0], full_word_dict)
                cur.execute('UPDATE words_sim SET sim=? where word_pair_idx=?', (sim, word_pair_idx[0]))
            conn.commit()

            # sim_procs = []
            # word_pair_pid_idx = dict()
            # sim_arr = multiprocessing.Array(ctypes.c_double, batch_size)
            # sim_arr =
            # for sim_arr_i, word_pair_idx in enumerate(unsimed_words_pair):
            #     word_pair_pid_idx[sim_arr_i] = word_pair_idx[0]
                # p = multiprocessing.Process(target=word_pair_sim, args=(word_pair_idx[0], full_word_dict, sim_arr, sim_arr_i))
                # sim_procs.append(p)
                # p.start()
                #
                # if len(sim_procs) == os_pro:
                #     for running_p in sim_procs:
                #         running_p.join()
                #     sim_procs = []
            # write_sim_batch_to_db(sim_arr, word_pair_pid_idx)
            total_finished_sim += batch_size

            unsimed_words_pair = cur.execute("SELECT word_pair_idx FROM words_sim where sim is null LIMIT ?", (batch_size,)).fetchall()

            if len(unsimed_words_pair) < batch_size:
                batch_size = len(unsimed_words_pair)

            print "Total %s/%s (%.1f %%) word pair sim is done. Time: %s" % (
                  total_finished_sim, unsimed_words_pair_cnt, float(total_finished_sim * 100) / unsimed_words_pair_cnt, time.time() - start)

        print "Word pair sim done!"
        cur.close()


def write_sim_batch_to_db(sim_arr, word_pair_pid_idx):
    cur = conn.cursor()
    for i in range(len(word_pair_pid_idx)):
        cur.execute('UPDATE words_sim SET sim=? where word_pair_idx=?', (sim_arr[i], word_pair_pid_idx[i]))
    conn.commit()
    cur.close()


def main(words_file):

    global conn
    dataset = words_file[:words_file.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))

    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS words_idx(word text not null, idx int)")
    cur.execute("CREATE TABLE IF NOT EXISTS words_sim(word_pair_idx int not null, sim real)")
    conn.commit()
    cur.close()

    try:
        total_num_words = insert_words_idx.insert_words_idx(conn, data_path + words_file)
    except Exception as e:
        print e
    else:
        try:
            insert_words_idx.insert_word_pair_idx(conn, total_num_words)
        except Exception as e:
            print e

    cur = conn.cursor()
    cur.execute('CREATE INDEX IF NOT EXISTS word_idx ON words_idx(word)')
    cur.execute('CREATE INDEX IF NOT EXISTS word_pair_idx ON words_sim(word_pair_idx)')
    conn.commit()
    cur.close()

    full_word_dict = get_words_from_db()
    words_pair_sim(full_word_dict)


if __name__ == "__main__":
    # main('lee_test_words.txt')
    # main('leefixsw_nasari_40_rmswcbwexpws_w3-3_words.txt')
    main('20news50short10_nasari_30_rmswcbwexpws_w3-3_words.txt')
