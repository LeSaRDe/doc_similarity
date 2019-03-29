import sqlite3
import idx_bit_translate

g_db_path = '/home/fcmeng/workspace/data/20news50short10.db'


def get_all_word_set(l_old_words, l_new_words):
    l_all_words = l_old_words + get_diff(l_old_words, l_new_words)
    l_all_word = list(set(l_all_words))
    return l_all_word

# l_new_words - l_old_words
def get_diff(l_old_words, l_new_words):
    l_diff = list(set(l_new_words) - set(l_old_words))
    return l_diff


def insert_diff_words(l_diff, db_path):
    db_conn = sqlite3.connect(db_path)
    db_cur = db_conn.cursor()
    # idx starts with 0 in 'words_idx'
    db_cur.execute('SELECT max(idx) from words_idx')
    start_idx = db_cur.fetchone()[0]+1
    idx = start_idx
    for diff_word in l_diff:
        db_cur.execute('INSERT INTO words_idx (word, idx) VALUES (?, ?)', (diff_word, idx))
        # db_cur.execute('UPDATE words_idx SET idx = ? WHERE word = ?', (idx, diff_word))
        idx += 1
    db_cur.execute('SELECT COUNT(*) from words_idx')
    db_conn.commit()
    print '[DBG]: %s words currently in words_idx' % db_cur.fetchone()[0]
    db_conn.close()


#def word_sim():


def get_current_word_n_idx(db_path):
    word_idx_dict = dict()
    db_conn = sqlite3.connect(db_path)
    db_cur = db_conn.cursor()
    db_cur.execute('SELECT word, idx from words_idx')
    recs = db_cur.fetchall()
    for rec in recs:
        word = rec[0]
        idx = rec[1]
        word_idx_dict[word] = idx
    db_conn.close()
    return word_idx_dict


def get_all_word_pairs():
    rows = cur.execute('SELECT word_pair_idx from words_sim order by word_pair_idx').fetchall()
    all_word_pairs = [i[0] for i in rows]
    return all_word_pairs


def get_all_new_words(start_idx):
    rows = cur.execute('SELECT word, idx from words_idx order by idx').fetchall()
    all = dict()
    new = dict()
    for word, idx in rows:
        if idx >= start_idx:
            new[word] = idx
        all[word] = idx
    return all, new


def indb(w_pair_idx):
    idx = cur.execute('SELECT count(*) from words_sim where word_pair_idx=?', (w_pair_idx,)).fetchone()[0]
    return idx


def get_valid_new_word_pairs(all_w_list, new_w_list):
    cnt = 0
    total_todo = 7672*7671/2 - 6727*6726/2
    for new_w, new_w_idx in new_w_list.items():
        for all_w, all_w_idx in all_w_list.items():
            if all_w_idx < new_w_idx:
                w_pair_idx = idx_bit_translate.keys_to_key(all_w_idx, new_w_idx)
                if indb(w_pair_idx) == 0:
                    cnt += 1
                    cur.execute('INSERT INTO words_sim (word_pair_idx) VALUES (?)', (w_pair_idx, ))
                else:
                    print "Word pair key %s already exist in db." % w_pair_idx
                if cnt % 20000 == 0:
                    conn.commit()
                    print "%s/%s finished" % (cnt, total_todo)
    conn.commit()
    print "Total %s word pair idx" % cnt


def main():
    global conn, cur
    conn = sqlite3.connect(g_db_path)
    cur = conn.cursor()

    # Step1: Insert new words idx
    # l_old_words = [word.strip() for word in old_words.split(',')]
    # old_word_n_idx_dict = get_current_word_n_idx(g_db_path)
    # l_old_words = old_word_n_idx_dict.keys()
    # print '[DBG]: old words count = %s' % len(l_old_words)
    # new_words = open('/home/fcmeng/workspace/data/20news50short10_nasari_30_rmswcbwexpws_w3-3_words.txt', 'r').read()
    # l_new_words = [word.strip() for word in new_words.split(',')]
    # print '[DBG]: new words count = %s' % len(l_new_words)
    # l_diff = get_diff(l_old_words, l_new_words)
    # print '[DBG]: %s new words to be added.' % len(l_diff)
    # insert_diff_words(l_diff, g_db_path)
    # print '[DBG]: insert diff words done!'
    # l_all_words = get_all_word_set(l_old_words, l_new_words)
    # print '[DBG]: all words count = %s' % len(l_all_words)

    # Step2: Insert word pair idx
    start_idx = len(open('/home/fcmeng/workspace/data/20news50short10_nasari_40_rmswcbwexpws_w3-3_words.txt', 'r').read().split(','))
    all_words, new_words = get_all_new_words(start_idx)
    # old_word_pairs_idx = get_all_word_pairs()
    get_valid_new_word_pairs(all_words, new_words)

    conn.close()
    print "Done"


main()