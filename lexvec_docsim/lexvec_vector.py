import sqlite3
import os
import json
import gensim
from gensim.models import KeyedVectors
import multiprocessing
import threading
import numpy as np
import time
import sys
sys.path.insert(1, '/home/mf3jh/workspace/lib/lexvec/lexvec/python/lexvec/')
import model as lexvec


lexvec_model = None
data_path = ''
data_set = ''
MULTIPROC = False

def get_one_doc_vect(doc_text):
    global lexvec_model
    doc_words = gensim.parsing.preprocessing.preprocess_string(doc_text)
    doc_vect = np.zeros((300,))
    for word in doc_words:
        try:
            word_vec = lexvec_model.wv[word]
            # word_vec = lexvec_model.word_rep(word)
        except:
            word_vec = None
        if word_vec is not None:
            doc_vect += word_vec
    return doc_vect


def output_one_doc_vect(doc_id, doc_text):
    start = time.time()
    doc_vec = get_one_doc_vect(doc_text)
    ret_vec = {i: float(v) for i, v in enumerate(doc_vec)}
    output_intermediate_dir_path = data_path + data_set + "_lexvec_doc2vec_vec_runtime/"
    if not os.path.exists(output_intermediate_dir_path):
        os.mkdir(output_intermediate_dir_path)
    with open(output_intermediate_dir_path + doc_id.replace("/", "_")+'.json', 'w') as outfile:
        json.dump(ret_vec, outfile, indent=4)
    outfile.close()
    elapse = time.time() - start
    print('[INF]: doc: %s done in %s secs.' % (doc_id, elapse))


def proc_cool_down(l_procs, max_procs):
    # while len(apv_processes) >= multiprocessing.cpu_count():
    while len(l_procs) >= max_procs:
        for proc in l_procs:
            if proc.pid != os.getpid():
                proc.join(1)
            if not proc.is_alive():
                l_procs.remove(proc)


def main(dataset):
    global lexvec_model, data_path, data_set

    data_set = dataset
    data_path = '%s/workspace/data/docsim/' % os.environ['HOME']
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()
    docs = cur.execute('SELECT doc_id, pre_ner FROM docs order by doc_id').fetchall()

    # model_file = '%s/workspace/lib/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W.pos.vectors'
    model_file = '%s/workspace/lib/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W+C.pos.vectors'
    # model_file = '%s/workspace/lib/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.ngramsubwords.300d.W.pos.bin'
    lexvec_model = KeyedVectors.load_word2vec_format(model_file, binary=False)
    # lexvec_model = lexvec.Model(model_file)

    # model = '20news50short10.model'
    # m = g.Doc2Vec.load(model)
    # start_alpha = 0.01
    # infer_epoch = 1000
    l_procs = []
    start = time.time()
    for doc_name, doc_pre_ner in docs:
        if MULTIPROC:
            p = multiprocessing.Process(target=output_one_doc_vect, args=(doc_name, doc_pre_ner))
            l_procs.append(p)
            p.start()
            proc_cool_down(l_procs, 10)
        else:
            output_one_doc_vect(doc_name, doc_pre_ner)
    if MULTIPROC:
        proc_cool_down(l_procs, 1)
    print("running time is %s" % str(time.time() - start))


if __name__ == '__main__':
    # main("reuters")
    main("20news50short10")
    # main("bbc")
    # main("leefixsw")
