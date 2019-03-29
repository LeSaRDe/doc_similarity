import os
import pandas as pd
from sklearn.decomposition import PCA


def main(apv_csv_file):
    global dataset, data_path, col_name
    dataset = apv_csv_file[:apv_csv_file.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = apv_csv_file[apv_csv_file.find('_') + 1:].replace('_matrix', '').replace('.csv', '')

    apv_df = pd.read_csv(data_path+apv_csv_file, index_col=0)
    apv_df_t = apv_df.T
    doc_ids = list(apv_df_t.index.values)
    print "Total %s documents" % len(doc_ids)

    pca_index = [i for i in range(500)]
    pca = PCA(copy=True, svd_solver='full')
    pca_array = pca.fit_transform(apv_df_t)
    df_pca = pd.DataFrame(pca_array, index=doc_ids, columns=pca_index).T
    df_pca.to_csv(data_path + '20news50short10_nasari_40_rmswcbwexpws_w3-3_pca_apv_matrix.csv', sep=',', index=False)

    print "Done."


main('20news50short10_nasari_40_rmswcbwexpws_w3-3_apv_matrix.csv')