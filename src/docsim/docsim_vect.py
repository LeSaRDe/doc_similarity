import sqlite3
import numpy as np
import scipy as sp
from scipy.spatial.distance import pdist
from sklearn.metrics.pairwise import cosine_similarity
import os

DB_CONN_STR = '/home/fcmeng/workspace/data/leefixsw.db'

RUN_NAME = 'leefixswvsleebgfixsw_nasari_50_rmswcbwexpwsscyc_w3-3'
SIM_COL_NAME = 'vectnasari_50_rmswcbwexpwsscyc_w3-3'
INTER_RES_PATH = '/home/fcmeng/workspace/data/%s/' % RUN_NAME
VECTS_PATH = INTER_RES_PATH + 'vects.dump'


def read_vects():
    try:
        vects_fd = open(VECTS_PATH, 'r')
        vects_fd.seek(0, 0)
        l_vect_lines = vects_fd.readlines()
        d_id_vect = {}
        for vect_line in l_vect_lines:
            fields = vect_line.split(':')
            doc_id = str(fields[0]).strip()
            #print doc_id
            vect = np.array([float(str(ele).strip()) for ele in fields[1].strip().split(',')]).reshape((1, -1))
            print vect.shape
            #vect = np.fromstring(fields[1].strip())
            d_id_vect[doc_id] = vect
            #print d_id_vect[doc_id]
        vects_fd.close()
        #print d_id_vect
        return d_id_vect
    except Exception as e:
        print e
        return None

def id_to_vect(doc_id, d_id_vect):
    if d_id_vect is None:
        print '[ERR]: d_id_vect is None!'
        return
    if doc_id in d_id_vect:
        return d_id_vect[doc_id]
    else:
        print '[ERR]: Cannot find doc_id = %s' % doc_id
        return None

def compare_two_vects(vect1, vect2):
    #dist = pdist(vect1, vect2, 'cosine')
    #sim = 1 - dist
    sim = cosine_similarity(vect1, vect2)[0][0]
    print '[DBG]: sim = %s' % sim
    return sim

# ids start from 1
def output_doc_sim(doc_id_pair, sim, doc_db_cur):
    doc_db_cur.execute('UPDATE docs_sim SET "%s" = ? WHERE doc_id_pair = ?' % SIM_COL_NAME, (str(sim), doc_id_pair))

def main():
    db_conn = sqlite3.connect(DB_CONN_STR)
    db_cur = db_conn.cursor()
    db_cur.execute('SELECT doc_id_pair from docs_sim')
    # ids start from 1
    l_doc_id_pairs = [str(row[0]) for row in db_cur.fetchall()]
    d_id_vect = read_vects()
    #print d_id_vect
    count = 0
    for doc_id_pair in l_doc_id_pairs:
        doc_ids = doc_id_pair.split('#')
        doc_id1 = str(int(doc_ids[0].strip())-1)
        doc_id2 = str(int(doc_ids[1].strip())-1)
        doc_vect1 = id_to_vect(doc_id1, d_id_vect)
        doc_vect2 = id_to_vect(doc_id2, d_id_vect)
        sim = compare_two_vects(doc_vect1, doc_vect2)
        output_doc_sim(doc_id_pair, sim, db_cur)
        count += 1
        if count % 200 == 0:
            db_conn.commit()
    db_conn.commit()
    db_conn.close()

main()









