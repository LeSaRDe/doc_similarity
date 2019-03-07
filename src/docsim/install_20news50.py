import os, fnmatch
import time
import sqlite3


DEBUG = True
DOCS_ROOT = "/home/fcmeng/workspace/data/20news50/short10/"


def find_files(directory, pattern):
    all_doc_keys = []
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                all_doc_keys.append(filename[len(DOCS_ROOT):])
                # yield filename
    return all_doc_keys


def main():
    start = time.time()

    conn = sqlite3.connect("/home/fcmeng/workspace/data/20news50short10.db")
    cur = conn.cursor()

    conn18828 = sqlite3.connect("/home/fcmeng/workspace/data/20news18828_nice.db")
    cur18828 = conn18828.cursor()

    all_doc_keys = find_files(DOCS_ROOT, "*")
    cnt = 0
    for each in all_doc_keys:
        row = cur18828.execute("SELECT doc_id, pre_ner from docs where doc_id=?", (each, )).fetchone()
        cur.execute("INSERT INTO docs(doc_id, pre_ner) VALUES (?, ?)", row)
        cnt += 1
        if cnt % 5000 == 0:
            conn.commit()
    print cnt
    conn.commit()
    cur.close()
    conn.close()

    #     lines = open(filename, 'r').readlines()
    #     txt = cleanup_invalid_lines(lines)
    #     pre_ner_txt = txt_clean(txt)
    #     if DEBUG:
    #         print "PRE NER txt:\n%s" % pre_ner_txt
    #     if not DEBUG:
    #         insert_pre_ner_to_db((filename.replace(DOCS_ROOT, ''), pre_ner_txt), i)
    # if not DEBUG:
    #     conn.commit()
    #     cur.close()
    #     conn.close()

    print "\n\nTotal time: %s sec" % (time.time() - start)


if __name__ == "__main__":
    main()