import pandas as pd
import os
import json


def main(folder):
    global dataset, data_path
    dataset = folder[:folder.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']

    with open(data_path + folder.replace('_apv_vec', '') + "_phrase_clusters_by_clusterid.json",'r') as infile:
        phrase_cluster_by_clusterid = json.load(infile)
    infile.close()

    apv_df = pd.DataFrame(phrase_cluster_by_clusterid.keys(), columns=['cluster_id'])

    for ff in os.listdir(data_path+folder):
        if ff.endswith(".json"):
            with open(data_path+folder+"/"+ff, 'r') as infile:
                apv_vec = json.load(infile)
                apv_df[ff.replace('.json', '')] = apv_df['cluster_id'].map(apv_vec)

    apv_df.to_csv(data_path + folder.replace('_apv_vec', '') + '_apv_matrix.csv', sep=',', index=False)


main("20news50short10_nasari_40_rmswcbwexpws_w3-3_apv_vec")