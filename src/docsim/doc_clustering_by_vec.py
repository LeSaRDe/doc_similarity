import sqlite3
import os
import doc_clustering_utils
import copy


def main(data_name):
    global dataset, data_path, TOTAL_DOC
    dataset = data_name[:data_name.find('_')]
    data_path = '%s/workspace/data/docsim/' % os.environ['HOME']
    col_name = data_name[data_name.find('_') + 1:]
    TOTAL_DOC = doc_clustering_utils.get_total_doc_size(dataset)
    # TOTAL_DOC=250
    # dataset_flag = '20news50short10-250'
    # dataset_flag = '20news50short10'
    # dataset_flag = 'reuters'
    dataset_flag = 'bbc'

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
    # doc_categories = {'soybean': 0, 'gold': 1, 'crude': 2, 'livestock': 3, 'acq': 4, 'interest': 5, 'ship': 6}
    # doc_categories = {'soybean': 0, 'livestock': 3}
    doc_categories = {'business': 0, 'entertainment': 1, 'politics': 2, 'sport': 3, 'tech': 4}

    doc_id_index, id_to_doc = doc_clustering_utils.get_doc_ids(cur=cur, dataset_flag=dataset_flag)
    org_doc_labels = doc_clustering_utils.label_org_doc_ids(doc_id_index, doc_categories, TOTAL_DOC)

    outfile = open('%s/workspace/data/docsim/%s_doc_cluster_%s.txt' % (os.environ['HOME'], dataset, col_name), 'w+')
    outfile.write(str(doc_id_index))

    aff_matrix = None
    kmeans_matrix = None

    for n_size in [5, 6, 7, 8, 9, 10]:
    # for n_size in [7, 8, 9, 10, 11, 12, 13, 14]:
    # for n_size in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
    # for n_size in [2]:
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


# main("20news50short10_nasari_30_rmswcbwexpws_w3-3_top5ws30_pred_apv")
# main("20news50short10_nasari_30_rmswcbwexpws_w3-3_doc_pv")
# main("20news50short10_nasari_doc2vec")
# main("20news50short10_glove_doc2vec")
# main("20news50short10_doc2vec")
# main("20news50short10_lexvec_doc2vec")
# main("20news50short10_lexvec_subword")
# main("20news50short10_lexvec_context")
# main("20news50short10_fasttext_doc2vec")
# main("20news50short10_sent2vec_doc2vec")
# main("20news50short10_avg_nps_vect_lexvec")


# main("reuters_nasari_30_rmswcbwexpws_w3-3_top5ws30_apv")
# main("reuters_nasari_30_rmswcbwexpws_w3-3_top5ws30_bi_apv")
# main("reuters_nasari_30_rmswcbwexpws_w3-3_doc_pv")
# main("reuters_nasari_doc2vec")
# main("reuters_doc2vec")
# main("reuters_glove_doc2vec")
# main("reuters_fasttext_doc2vec")
# main("reuters_lexvec_doc2vec")
# main("reuters_sent2vec_doc2vec")
# main("reuters_avg_nps_vect_lexvec")
# main("reuters_avg_nps_vect_w_avg_edge_vect_lexvec")


# main("bbc_nasari_doc2vec")
# main("bbc_nasari_30_rmswcbwexpws_w3-3_top5ws30_pred_apv")
# main("bbc_nasari_30_rmswcbwexpws_w3-3_top5ws30_bi_apv")
# main("bbc_doc2vec")
# main("bbc_nasari_30_rmswcbwexpws_w3-3_doc_pv")
# main("bbc_glove_avg_doc2vec")
# main("bbc_fasttext_doc2vec")
# main("bbc_lexvec_doc2vec")
# main("bbc_sent2vec_doc2vec")
# main("bbc_avg_nps_vect_lexvec")
main("bbc_avg_nps_vect_w_avg_edge_vect_lexvec")