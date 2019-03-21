import sqlite3
import os
import copy
import doc_clustering_utils


# def main(dataset, c_size):
def main(folder):
    global conn, cur, TOTAL_DOC
    dataset = folder[:folder.find('_')]
    col_name = folder[folder.find('_')+1:]

    TOTAL_DOC = doc_clustering_utils.get_total_doc_size(dataset)
    # if dataset == 'lee' or dataset == 'leefixsw':
    #     TOTAL_DOC = 50
    # elif dataset == "20news18828":
    #     TOTAL_DOC = 18828
    # elif dataset == "20news50":
    #     TOTAL_DOC = 18828
    # elif dataset == "20news50short":
    #     TOTAL_DOC = 1000
    # elif dataset == "20news50short10":
    #     # for 10 classes
    #     #TOTAL_DOC = 500
    #     # for 5 classes
    #     TOTAL_DOC = 250


    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    #doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
    #                  'rec.motorcycles': 4, 'misc.forsale': 5,'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
    #                  'talk.politics.mideast': 9}
    doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.space': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
                      'talk.politics.mideast': 4}

    run_clustering = 'all'
    dataset_flag = '20news50short10-250'

    doc_id_index, id_to_doc = doc_clustering_utils.get_doc_ids(cur=cur, dataset_flag=dataset_flag)
    org_doc_labels = doc_clustering_utils.label_org_doc_ids(doc_id_index, doc_categories, TOTAL_DOC)

    outfile = open('%s/workspace/data/%s_scyc50_doc_cluster_res.txt' % (os.environ['HOME'], dataset), 'w+')
    outfile.write(str(doc_id_index))

    aff_matrix = None
    kmeans_matrix = None

    for n_size in [6]:
        # ==>spectral_clustering
        if run_clustering == 'spectral' or run_clustering == 'all':
            if aff_matrix is None:
                full_doc_pair_list = doc_clustering_utils.get_doc_sim_from_db(col=col_name, cur=cur, dataset_flag=dataset_flag)
                # use number of word_pair to recalulate number of words
                # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                std_value, mean_value, min_value, max_value = doc_clustering_utils.cal_std_mean(full_doc_pair_list)
                aff_matrix = doc_clustering_utils.build_normed_aff_matrix(full_doc_pair_list, doc_id_index, TOTAL_DOC, minv=min_value, maxv=max_value)
            # np.savetxt("foo.csv", X=aff_matrix, delimiter=",")
            # test for using sim vects to compute cosine sims for spectral's input
            #kmeans_matrix = build_kmeans_matrix_sim(full_doc_pair_list, size)
            #aff_matrix = build_aff_matrix_from_kmeans_matrix(kmeans_matrix)
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

        # ==> kmeans
        if run_clustering == 'kmeans' or run_clustering == 'all':
            if kmeans_matrix is None:
                if aff_matrix is None:
                    full_doc_pair_list = doc_clustering_utils.get_doc_sim_from_db(col=col_name, cur=cur, dataset_flag=dataset_flag)
                    # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                    std_value, mean_value, min_value, max_value = doc_clustering_utils.cal_std_mean(full_doc_pair_list)
                    kmeans_matrix = doc_clustering_utils.build_normed_aff_matrix(full_doc_pair_list, doc_id_index,
                                                                                 TOTAL_DOC, minv=min_value, maxv=max_value)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
                else:
                    kmeans_matrix = copy.deepcopy(aff_matrix)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Kmeans matrix [i][i] not equal to 1!!")
            km_labels = doc_clustering_utils.kmeans_clustering(kmeans_matrix, n_size)

            doc_clustering_utils.output_clustering_res(cluster_name="Kmeans", n_size=n_size, res_labels=km_labels,
                                                       id_to_doc=id_to_doc, outfile=outfile)
            doc_clustering_utils.print_cluster_perform(kmeans_matrix, km_labels, 'euclidean', outfile)
            doc_clustering_utils.cluster_perf_evaluation(org_doc_labels, km_labels, outfile)

        if run_clustering == 'agglo' or run_clustering == 'all':
            # if aff_matrix is None:
            #     full_doc_pair_list = get_word_sim_from_db()
            #     size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
            #     # use number of word_pair to recalulate number of words
            #     # std_value, mean_value, min_value, max_value = cal_std_mean(full_doc_pair_list)
            #     aff_matrix = build_aff_matrix(full_doc_pair_list, size, minv=-1, maxv=1)
            # if aff_matrix[0][0] != 0:
            #     raise Exception("Aff matrix [i][i] not equal to 0!!")
            # ac_precomputed_labels = agglomerative_clustering(aff_matrix, 'precomputed', n_size)
            # print "\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size
            # outfile.write("\n\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size)
            # outfile.write("\n%s\n" % print_words_by_labels(ac_precomputed_labels))
            # apss, apch, apdb = print_cluster_perform(aff_matrix, ac_precomputed_labels)
            # outfile.write(
            #     '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (apss, apch, apdb))
            # if aff_matrix_agglo is None:
            #     aff_matrix_agglo = aff_plus_degree(copy.deepcopy(aff_matrix), size)
            # ac_euclidean_labels = agglomerative_clustering(aff_matrix_agglo, 'euclidean', n_size)
            # print "\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size
            # outfile.write("\n\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size)
            # outfile.write("\n%s\n" % print_words_by_labels(ac_euclidean_labels))
            # aess, aech, aedb = print_cluster_perform(aff_matrix_agglo, ac_euclidean_labels)
            # outfile.write(
            #     '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (aess, aech, aedb))

            #==>Use kmeans matrix
            if kmeans_matrix is None:
                if aff_matrix is None:
                    full_doc_pair_list = doc_clustering_utils.get_doc_sim_from_db(col=col_name, cur=cur, dataset_flag=dataset_flag)
                    # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                    std_value, mean_value, min_value, max_value = doc_clustering_utils.cal_std_mean(full_doc_pair_list)
                    kmeans_matrix = doc_clustering_utils.build_normed_aff_matrix(full_doc_pair_list, doc_id_index,
                                                                                 TOTAL_DOC, minv=min_value, maxv=max_value)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
                else:
                    kmeans_matrix = copy.deepcopy(aff_matrix)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Aff matrix [i][i] not equal to 1!!")
            # Agglomerative - precomputed
            ac_precomputed_labels = doc_clustering_utils.agglomerative_clustering(kmeans_matrix, 'precomputed', n_size)
            doc_clustering_utils.output_clustering_res(cluster_name="Agglomerative", n_size=n_size, res_labels=ac_precomputed_labels,
                                                       id_to_doc=id_to_doc, outfile=outfile)
            doc_clustering_utils.print_cluster_perform(kmeans_matrix, ac_precomputed_labels, 'precomputed', outfile)
            doc_clustering_utils.cluster_perf_evaluation(org_doc_labels, ac_precomputed_labels, outfile)

            # Agglomerative - euclidean
            ac_euclidean_labels = doc_clustering_utils.agglomerative_clustering(kmeans_matrix, 'euclidean', n_size)
            doc_clustering_utils.output_clustering_res(cluster_name="Agglomerative", n_size=n_size, res_labels=ac_euclidean_labels,
                                                       id_to_doc=id_to_doc, outfile=outfile)
            doc_clustering_utils.print_cluster_perform(kmeans_matrix, ac_euclidean_labels, 'euclidean', outfile)
            doc_clustering_utils.cluster_perf_evaluation(org_doc_labels, ac_euclidean_labels, outfile)

    outfile.close()
    # print "\nstsc autograd clustering\n"
    # print self_tuning_spectral_clustering_autograd(aff_matrix, min_n_cluster=c_size, max_n_cluster=c_size)

    #
    # print "\n\nagglomerative_clustering - precomputed\n\n", ac_labels
    # print_docs_by_labels(ac_labels)
    #
    # aff_matrix_1 = aff_plus_degree(aff_matrix, size)
    # ac_labels_1 = agglomerative_clustering(aff_matrix_1, 'euclidean')
    # print "\n\nagglomerative_clustering - euclidean\n\n", ac_labels_1
    # print_docs_by_labels(ac_labels_1)

    # for i, label in enumerate(cluster_labels):
    #     print "%s - %s" % (label, full_doc_list[idx_map[i]])


# main('leefixsw', c_size)
#main("20news50short10_nasari_40_rmswcbwexpws_w3-3")
main("20news50short10_nasari_50_rmswcbwexpwsscyc_w3-3")
