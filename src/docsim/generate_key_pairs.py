import sqlite3


def main(db_name):
    outfile = open("/home/fcmeng/workspace/doc_similarity/res/20news50short10_all_keys.txt", "w+")

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

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


if __name__ == '__main__':
    main('/home/fcmeng/workspace/data/20news50short10.db')