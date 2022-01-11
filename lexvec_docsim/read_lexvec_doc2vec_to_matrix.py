import pandas as pd
import os
import json


def main(folder):
    global dataset, data_path
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/docsim/' % os.environ['HOME']

    cluster_keys = [str(i) for i in range(300)]

    apv_df = pd.DataFrame(cluster_keys, columns=['cluster_id'])

    for ff in os.listdir(data_path + folder):
        if ff.endswith(".json"):
            with open(data_path + folder + "/" + ff, 'r') as infile:
                apv_vec = json.load(infile)
                apv_df[ff.replace('.json', '')] = apv_df['cluster_id'].map(apv_vec)

    apv_df.to_csv(data_path + folder.replace('_vec_runtime', '') + '_matrix.csv', sep=',', index=False)


# main("reuters_lexvec_doc2vec_vec")
main("20news50short10_lexvec_doc2vec_vec_runtime")
# main("leefixsw_lexvec_doc2vec_vec")
# main("bbc_lexvec_doc2vec_vec")