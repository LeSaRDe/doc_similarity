import os
import json
import sqlite3
import math
import pandas as pd


def get_doc_sim_from_db(col):
    # cur.execute('''SELECT doc_id_pair, "'''+col+'''" from docs_sim
    #                 where doc_id_pair not like "%talk.politics.guns%"
    #                 and doc_id_pair not like "%alt.atheism%"
    #                 and doc_id_pair not like "%misc.forsale%"
    #                 and doc_id_pair not like "%sci.space%"
    #                 and doc_id_pair not like "%sci.electronics%"
    #                 order by doc_id_pair''')
    cur.execute('''SELECT doc_id_pair, "''' + col + '''" from docs_sim order by doc_id_pair''')
    rows = cur.fetchall()
    return rows


def get_doc_ids():
    # cur.execute('''SELECT doc_id from docs
    #                 where doc_id not like "%talk.politics.guns%"
    #                 and doc_id not like "%alt.atheism%"
    #                 and doc_id not like "%misc.forsale%"
    #                 and doc_id not like "%sci.space%"
    #                 and doc_id not like "%sci.electronics%"
    #                 order by doc_id''')
    cur.execute('''SELECT doc_id from docs order by doc_id''')
    rows = cur.fetchall()
    return rows


def cal_weight(w1, w2):
    return math.exp(3 / (math.pow(w1, 3) + math.pow(w2, 3)))


def build_apv_matrix(phrase_cluster, folder):
    global cluster_size
    cluster_ids = set(phrase_cluster.values())
    cluster_size = len(cluster_ids)
    print "Cluster size=%s" % cluster_size
    doc_ids = get_doc_ids()

    apv_df = pd.DataFrame(cluster_ids, columns=['cluster_id'])

    for each_doc in doc_ids:
        top_sim_docs = find_top_sim_doc_pairs(each_doc[0])
        apv_vec = dict.fromkeys(cluster_ids, 0.0)
        for each_f in top_sim_docs:
            with open(data_path + folder + '/' + each_f[0].replace('/', '_') +'.json', 'r') as infile:
                if each_doc[0] == each_f[0].split('#')[0]:
                    doc_key = 's1:'
                elif each_doc[0] == each_f[0].split('#')[1]:
                    doc_key = 's2:'
                else:
                    raise Exception("doc name not in the file name!")
                sent_pair_cycles = json.load(infile)['sentence_pair'].values()
                for cycles in sent_pair_cycles:
                    for each_c in cycles['cycles']:
                        this_arc = 0
                        that_arc = 0
                        ps = []
                        for each_n in each_c:
                            if each_n.startswith(doc_key):
                                if ':L:' in each_n:
                                    ps.append(each_n[5:].split('##')[0].lower())
                                else:
                                    this_arc += 1
                            else:
                                if ':L:' not in each_n:
                                    that_arc += 1
                        if len(ps) == 2:
                            try:
                                p_cluster_id = phrase_cluster[ps[0] + '#' + ps[1]]
                            except:
                                p_cluster_id = phrase_cluster[ps[1] + '#' + ps[0]]
                        else:
                            p_cluster_id = phrase_cluster[ps[0]]
                        if not p_cluster_id:
                            raise Exception('Cant find %s in the cluster!!' % ps)
                        weight = cal_weight(this_arc, that_arc)
                        apv_vec[p_cluster_id] += weight
        apv_df[each_doc[0]] = apv_df['cluster_id'].map(apv_vec)
    return apv_df


def find_top_sim_doc_pairs(doc_id):
    query = '''SELECT doc_id_pair FROM docs_sim WHERE doc_id_pair like "%%%s%%" order by "%s" DESC limit %s''' % (doc_id, col_name, TOP_SIM_DOCS)
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def main(folder):
    global dataset, data_path, col_name
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = folder[folder.find('_') + 1:]

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    with open(data_path+dataset+"_phrase_cluster.json",'r') as infile:
        phrase_cluster = json.load(infile)

    global full_doc_categories, doc_categories, TOP_SIM_DOCS
    full_doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
                     'rec.motorcycles': 4, 'misc.forsale': 5,'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
                     'talk.politics.mideast': 9}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.med': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
    #                   'talk.politics.mideast': 4}
    doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
                      'rec.motorcycles': 4, 'misc.forsale': 5, 'sci.med': 6, 'sci.electronics': 7,
                      'rec.sport.hockey': 8, 'talk.politics.mideast': 9}
    TOP_SIM_DOCS = 30

    apv_matrix = build_apv_matrix(phrase_cluster, folder)
    apv_matrix.to_csv(data_path+dataset+'_apv_matrix.csv', sep=',', index=False)


if __name__ == '__main__':
    # must have a phrase cluster saved in a json file
    main("20news50short10_nasari_40_rmswcbwexpws_w3-3")