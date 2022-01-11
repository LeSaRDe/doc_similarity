import os
import sqlite3
import gensim
from gensim.models import KeyedVectors
import scipy.spatial.distance as scipyd
# import glove_vectors
import numpy as np


def get_one_doc_vect(doc_text):
    global model
    doc_words = gensim.parsing.preprocessing.preprocess_string(doc_text)
    doc_vect = np.zeros((300,))
    for word in doc_words:
        try:
            word_vec = model.wv[word]
        except:
            word_vec = None
        if word_vec is not None:
            doc_vect += word_vec
    return doc_vect

def main():
    global model
    # data_path = '%s/workspace/data/Li65/' % os.environ['HOME']
    # data_path = '%s/workspace/data/stsbenchmark/' % os.environ['HOME']
    data_path = '%s/workspace/data/sick/' % os.environ['HOME']
    # col1_conn = sqlite3.connect("%s%s.db" % (data_path, 'col1_nosw'))
    col1_conn = sqlite3.connect("%s%s.db" % (data_path, 'col1'))
    col1_cur = col1_conn.cursor()
    # col2_conn = sqlite3.connect("%s%s.db" % (data_path, 'col2_nosw'))
    col2_conn = sqlite3.connect("%s%s.db" % (data_path, 'col2'))
    col2_cur = col2_conn.cursor()

    model_file = '%s/workspace/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W.pos.vectors'
    model = KeyedVectors.load_word2vec_format(model_file, binary=False)

    col1 = col1_cur.execute('SELECT doc_id, pre_ner FROM docs order by doc_id').fetchall()
    col1_docs = {doc_id: pre_ner for doc_id, pre_ner in col1}
    col2 = col2_cur.execute('SELECT doc_id, pre_ner FROM docs order by doc_id').fetchall()
    col2_docs = {doc_id: pre_ner for doc_id, pre_ner in col2}

    l_res = []
    for i in range(0, 9840):
        doc1_vec = get_one_doc_vect(col1_docs[str(i)])
        doc2_vec = get_one_doc_vect(col2_docs[str(i)])
        cosine_sim = 1.0-scipyd.cosine(doc1_vec, doc2_vec)
        l_res.append(cosine_sim)
        print "%s,%s" % (i, cosine_sim)

    out_fd = open('/home/fcmeng/workspace/data/sick_lexvec.txt', 'w')
    l_res = [str(sim) for sim in l_res]
    out_fd.write('\n'.join(l_res))
    out_fd.close()


main()