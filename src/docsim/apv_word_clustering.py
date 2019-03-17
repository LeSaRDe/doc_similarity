import os
import sqlite3


def read_word_cluster(file_path):
    lines = open(file_path, "r").readlines()
    table_name = lines[0]
    word_cluster = eval(lines[1])
    return table_name, word_cluster


def insert_words_to_words_cluster_table(dataset):
    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn1 = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur1 = conn1.cursor()
    word_cnt = cur1.execute('SELECT count(word) FROM words_idx').fetchone()[0]

    cur = conn.cursor()
    exist_rows = cur.execute('SELECT count(word) FROM word_cluster').fetchone()[0]
    if exist_rows != word_cnt and exist_rows != 0:
        cur.close()
        raise Exception(
            "[Err]Words already exist in word_cluster table, but the number of words (%s) don't match words_idx table"
            "words (%s)! Need manual check!" % (exist_rows, word_cnt))
    elif exist_rows == word_cnt:
        print "Total %s words are already saved in word_cluster table, no need to insert." % exist_rows
        return
    elif exist_rows == 0:
        all_words = cur1.execute('SELECT word FROM words_idx').fetchall()
        cur.executemany('INSERT INTO word_cluster(word) VALUES (?)', all_words)
        conn.commit()

        exist_rows = cur.execute('SELECT count(word) FROM word_cluster').fetchone()[0]
        if exist_rows != word_cnt:
            raise Exception("[Err] Insert finished, but the number of words (%s) don't match words_idx table"
                            "words (%s)! Need manual check!" % (exist_rows, word_cnt))
        else:
            print "Insert finished. Total %s words inserted." % exist_rows
            return
    cur1.close()
    conn1.close()
    cur.close()


def insert_words_cluster_mapping(word_cluster, table):
    for k, v in word_cluster.items():



def main(dataset):
    global conn
    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn = sqlite3.connect("%s%sapvs.db" % (data_path, dataset))

    word_cluster, table = read_word_cluster("sample_word_cluster_labels.txt")
    insert_words_to_words_cluster_table(dataset)
    insert_words_cluster_mapping(word_cluster, table)


if __name__ == '__main__':
    main("20news50short10")