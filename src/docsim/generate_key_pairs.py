import sqlite3
import os


def insert_key_pairs_to_db(key_pair_txt_file):
    key_pairs = open(key_pair_txt_file, 'r').readlines()
    for key in key_pairs:
        cur.execute("INSERT INTO docs_sim (doc_id_pair) VALUES (?)", (key.strip(),))
    conn.commit()
    print "Insert done."


def main(db_name):


    # Generate key pairs and save to a txt file
    outfile_path = "/home/fcmeng/workspace/doc_similarity/res/reuters_all_keys.txt"

    global conn, cur
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    if not os.path.exists(outfile_path):
        outfile = open(outfile_path, "w+")
        doc_ids = cur.execute('SELECT doc_id from docs order by doc_id').fetchall()
        cnt = 0
        for i, doc_id1 in enumerate(doc_ids):
            for j, doc_id2 in enumerate(doc_ids):
                if i<j:
                    doc_id_pair = "%s#%s\n" % (doc_id1[0], doc_id2[0])
                    outfile.write(doc_id_pair)
                    cnt+=1

        outfile.close()
        print "Total keys:", cnt

    # Read key pairs from a txt file and insert to db
    insert_key_pairs_to_db(outfile_path)
    cur.execute("CREATE INDEX doc_id_pair_idx ON docs_sim (doc_id_pair)")
    conn.close()


if __name__ == '__main__':
    main('/home/fcmeng/workspace/data/reuters.db')