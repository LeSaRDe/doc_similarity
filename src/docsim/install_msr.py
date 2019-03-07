import os
import fnmatch
import time
import sqlite3
import csv


DEBUG = False
DOCS_ROOT = "/home/%s/workspace/data/msr/" % os.environ['USER']


def main():
    start = time.time()

    conn = sqlite3.connect("/home/%s/workspace/data/msr.db" % os.environ['USER'])
    cur = conn.cursor()

    msr_reader = csv.DictReader(open("/home/%s/workspace/data/msr/msr_test.csv" % os.environ['USER'], 'r'))
    msr_all = dict()

    msr_sim = open("/home/%s/workspace/data/msr_sim.txt" % os.environ['USER'], "w+")

    for i, each in enumerate(msr_reader):
        if each['#1 ID'] not in msr_all.keys():
            msr_all[each['#1 ID']] = each['#1 String']
            if not DEBUG:
                cur.execute("INSERT INTO docs(doc_id, pre_ner) VALUES (?, ?)", (each['#1 ID'], sqlite3.Binary(each['#1 String'])))
        else:
            print "[%s] Doc1 %s already exits." % (i+1, each['#1 ID'])
        if each['#2 ID'] not in msr_all.keys():
            msr_all[each['#2 ID']] = each['#2 String']
            if not DEBUG:
                cur.execute("INSERT INTO docs(doc_id, pre_ner) VALUES (?, ?)", (each['#2 ID'], sqlite3.Binary(each['#2 String'])))
        else:
            print "[%s] Doc2 %s already exits." % (i+1, each['#2 ID'])
        msr_sim.write("%s#%s:%s\n"%(each['#1 ID'], each['#2 ID'], each['Quality']))

    conn.commit()
    cur.close()
    conn.close()

    msr_sim.close()

    print "Done. Total %s individual docs." % len(msr_all)

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