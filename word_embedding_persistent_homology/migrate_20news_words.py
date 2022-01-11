import sqlite3


g_in_db_path = '/home/fcmeng/workspace/data/20news50short10.db'
g_out_db_path = '/home/fcmeng/workspace/word_embedding_persistent_homology/wordsim.db'


def main():
    in_db_conn = sqlite3.connect(g_in_db_path)
    in_db_cur=  in_db_conn.cursor()
    out_db_conn = sqlite3.connect(g_out_db_path)
    out_db_cur = out_db_conn.cursor()

    # in_db_cur.execute('select word, idx from words_idx')
    in_db_cur.execute('select word_pair_idx, sim from words_sim')
    in_row = in_db_cur.fetchone()
    out_count = 0
    while in_row is not None:
        # out_db_cur.execute('insert into words_idx (word, idx) values (?, ?)', in_row)
        out_db_cur.execute('insert into words_sim (word_pair_idx, sim) values (?, ?)', in_row)
        out_count += 1
        if out_count % 1000 == 0:
            out_db_conn.commit()
        in_row = in_db_cur.fetchone()
    out_db_conn.commit()

    in_db_conn.close()
    out_db_conn.close()


if __name__ == '__main__':
    main()