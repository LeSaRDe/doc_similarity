import sqlite3
import os
import doc_clustering_utils
import copy


def main(data_name):
    global dataset, data_path, TOTAL_DOC
    dataset = data_name[:data_name.find('_')]
    data_path = '%s/workspace/data/' % os.environ['HOME']
    col_name = data_name[data_name.find('_') + 1:]
    TOTAL_DOC = doc_clustering_utils.get_total_doc_size(dataset)
    # dataset_flag = '20news50short10-250'
    # dataset_flag = '20news50short10'
    dataset_flag = 'reuters'

    global conn, cur
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    run_clustering = 'all'


    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.space': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
    #                   'talk.politics.mideast': 4}
    # doc_categories = {'talk.politics.guns': 0, 'alt.atheism': 1, 'misc.forsale': 2, 'sci.med': 3,
    #                   'sci.electronics': 4}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
    # 'rec.motorcycles': 4, 'misc.forsale': 5, 'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
    # 'talk.politics.mideast': 9}
    # doc_categories = {'talk.politics.guns': 0, 'alt.atheism': 1, 'talk.politics.mideast': 2}
    # doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.electronics': 1, 'misc.forsale': 2}
    doc_categories = {'soybean': 0, 'gold': 1, 'crude': 2, 'livestock': 3, 'acq': 4, 'interest': 5, 'ship': 6}

    doc_id_index, id_to_doc = doc_clustering_utils.get_doc_ids(cur=cur, dataset_flag=dataset_flag)
    org_doc_labels = doc_clustering_utils.label_org_doc_ids(doc_id_index, doc_categories, TOTAL_DOC)

    outfile = open('%s/workspace/data/%s_doc_cluster_%s.txt' % (os.environ['HOME'], dataset, col_name), 'w+')
    outfile.write(str(doc_id_index))

    aff_matrix = None
    kmeans_matrix = None

    for n_size in [7, 8, 9, 10, 11]:
        # ==>spectral_clustering
        if run_clustering == 'spectral' or run_clustering == 'all':
            if aff_matrix is None:
                full_doc_pair_list = doc_clustering_utils.get_doc_sim_from_db(col=col_name, cur=cur, dataset_flag=dataset_flag)
                if 'doc2vec' in col_name or 'pca' in col_name:
                    # std_value, mean_value, min_value, max_value = doc_clustering_utils.cal_std_mean(full_doc_pair_list)
                    aff_matrix = doc_clustering_utils.build_normed_aff_matrix(full_dicts=full_doc_pair_list,
                                                                              doc_ids=doc_id_index,size=TOTAL_DOC,
                                                                              minv=-1, maxv=1)
                else:
                    aff_matrix = doc_clustering_utils.build_normed_aff_matrix(full_dicts=full_doc_pair_list,
                                                                              doc_ids=doc_id_index, size=TOTAL_DOC)
            if aff_matrix[0][0] != 0:
                raise Exception("Aff matrix [i][i] not equal to 0!!")
            sc_labels = doc_clustering_utils.spectral_clustering(aff_matrix, n_size)

            if kmeans_matrix is None and aff_matrix is not None:
                kmeans_matrix = copy.deepcopy(aff_matrix)
                for i in range(TOTAL_DOC):
                    kmeans_matrix[i][i] = 1
            doc_clustering_utils.output_clustering_res(cluster_name="Spectral", n_size=n_size, res_labels=sc_labels,
                                                       id_to_doc=id_to_doc, outfile=outfile)
            doc_clustering_utils.print_cluster_perform(kmeans_matrix, sc_labels, 'precomputed', outfile)
            doc_clustering_utils.cluster_perf_evaluation(org_doc_labels, sc_labels, outfile)


# main("reuters_nasari_30_rmswcbwexpws_w3-3_doc_pv")
# main("20news50short10_doc2vec")
main("reuters_doc2vec")