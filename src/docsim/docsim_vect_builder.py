import sqlite3
import numpy as np

ID1_DB_CONN_STR = '/home/fcmeng/workspace/data/leefixsw.db'
ID2_DB_CONN_STR = '/home/fcmeng/workspace/data/leebgfixsw.db'

RUN_NAME = 'leefixswvsleebgfixsw_nasari_50_rmswcbwexpwsscyc_w3-3'
INTER_RES_PATH = '/home/fcmeng/workspace/data/%s/' % RUN_NAME
VECTS_OUT_PATH = INTER_RES_PATH + 'vects.dump'

# these ids start from 0
def read_ids():
    id1_db_conn = sqlite3.connect(ID1_DB_CONN_STR)
    id2_db_conn = sqlite3.connect(ID2_DB_CONN_STR)
    id1_db_cur = id1_db_conn.cursor()
    id2_db_cur = id2_db_conn.cursor()
    id1_db_cur.execute('select doc_id from docs');
    id2_db_cur.execute('select doc_id from docs');
    l_id1s = [str(row[0]) for row in id1_db_cur.fetchall()]
    l_id2s = [str(row[0]) for row in id2_db_cur.fetchall()]
    print '[DBG]: l_id1s = '
    print l_id1s
    print '[DBG]: l_id2s = '
    print l_id2s
    id1_db_conn.close()
    id2_db_conn.close()
    return l_id1s, l_id2s

def output_vects(l_id1_v_id1s):
    try:
        out_fd = open(VECTS_OUT_PATH, 'w+')
        for id1_v_id1 in l_id1_v_id1s:
            id1 = id1_v_id1[0].strip()
            v_id1_str = ','.join([str(ele).strip() for ele in id1_v_id1[1].tolist()])
            out_fd.write(id1 + ':' + v_id1_str + '\n')
        out_fd.close()
    except Exception as e:
        print e

def read_one_sim(id1, id2):
    try:
        sim_fd = open(INTER_RES_PATH + id1 + '#' + id2 + '.json', 'r')
        sim = sim_fd.readlines()[1].replace(',','').strip()
        sim_fd.close()
        return sim
    except Exception as e:
        print e
        return None

def build_one_vect(id1, l_id2s):
    v_id1 = np.array([])
    for id2 in l_id2s:
        sim = read_one_sim(id1, id2)
        v_id1 = np.append(v_id1, [sim])
    print '[DBG]: a vect in shape of %s for %s = ' % (v_id1.shape, id1)
    print v_id1
    return v_id1

def main():
    l_id1s, l_id2s = read_ids()
    l_id1_v_id1s = []
    for id1 in l_id1s:
        v_id1 = build_one_vect(id1, l_id2s)
        l_id1_v_id1s.append([id1, v_id1])
    output_vects(l_id1_v_id1s)

main()




