import sqlite3
import numpy as np
from sklearn.cluster import SpectralClustering, AgglomerativeClustering, KMeans
from sklearn import metrics
# from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import cosine_distances
import os
import math
import idx_bit_translate
# from stsc import self_tuning_spectral_clustering_autograd
# from multiprocessing import process
# import sys
import copy


def get_doc_sim_from_db(col):
    cur.execute('''SELECT doc_id_pair, "'''+col+'''" from docs_sim 
                    where doc_id_pair not like "%talk.politics.guns%" 
                    and doc_id_pair not like "%alt.atheism%" 
                    and doc_id_pair not like "%misc.forsale%" 
                    and doc_id_pair not like "%sci.space%" 
                    and doc_id_pair not like "%sci.electronics%" 
                    order by doc_id_pair''')
    rows = cur.fetchall()
    return rows


def get_doc_ids():
    cur.execute('''SELECT doc_id from docs
                    where doc_id not like "%talk.politics.guns%" 
                    and doc_id not like "%alt.atheism%" 
                    and doc_id not like "%misc.forsale%" 
                    and doc_id not like "%sci.space%" 
                    and doc_id not like "%sci.electronics%" 
                    order by doc_id''')
    rows = cur.fetchall()
    doc_ids = dict()
    id_to_doc = dict()
    for i, row in enumerate(rows):
        doc_ids[row[0]] = i
        id_to_doc[i] = row[0]
    return doc_ids, id_to_doc


def label_org_doc_ids(doc_ids, doc_categories, size):
    doc_labels = [-1] * size
    for doc_id, loc in doc_ids.items():
        doc_labels[loc] = doc_categories[doc_id.split('/')[0]]
    if -1 in doc_labels:
        raise Exception("Org doc labels has -1!!")
    return doc_labels


def build_normed_aff_matrix(full_dicts, doc_ids, size, stdv=None, meanv=None, minv=None, maxv=None):
    aff_mat = np.zeros([size, size], dtype=float)
    for word_idx, sim in full_dicts:
        # normed_sim = (sim-meanv)/stdv
        normed_sim = (sim-minv)/(maxv-minv)
        xidx = doc_ids[word_idx.split('#')[0]]
        yidx = doc_ids[word_idx.split('#')[1]]
        aff_mat[xidx][yidx] = normed_sim
        aff_mat[yidx][xidx] = normed_sim
    return aff_mat


def build_aff_matrix_from_kmeans_matrix(kmeans_mat):
    ret_mat = np.zeros(kmeans_mat.shape, dtype=float)
    for id1, vect1 in enumerate(kmeans_mat[:,:]):
        for id2, vect2 in enumerate(kmeans_mat[:,:]):
            if id2 < id1:
                sim = cosine_distances(vect1.reshape(1, -1), vect2.reshape(1, -1))
                sim = (sim + 1)/2
                ret_mat[id1][id2] = sim
                ret_mat[id2][id1] = sim
            elif id2 == id1:
                ret_mat[id1][id2] = 1.0
                ret_mat[id2][id1] = 1.0
    return ret_mat


def aff_plus_degree(matrix, size):
    new_matrix = np.zeros(shape=(size, size))
    for i in range(size):
        row = matrix[i]
        row[i] = sum(matrix[i])
        new_matrix[i] = row
    return new_matrix


def spectral_clustering(mat, n_size):
    sc = SpectralClustering(n_clusters=n_size, eigen_solver=None, random_state=None, n_init=10, affinity='precomputed',
                            assign_labels='kmeans', n_jobs=-1).fit_predict(mat)
    return sc


def kmeans_clustering(mat, n_size):
    km = KMeans(n_clusters=n_size).fit_predict(mat)
    return km


# def build_kmeans_matrix_sim(full_dicts, size):
#     aff_mat = np.zeros([size, size], dtype=float)
#     for word_idx, sim in full_dicts:
#         xidx, yidx = word_idx.split('#')
#         aff_mat[int(xidx)][int(yidx)] = sim
#         aff_mat[int(yidx)][int(xidx)] = sim
#     for i in range(size):
#         aff_mat[i][i] = 1
#     return aff_mat


def agglomerative_clustering(mat, affinity, n_size):
    if affinity == 'precomputed':
        ac = AgglomerativeClustering(n_clusters=n_size, affinity=affinity, linkage='average').fit_predict(mat)
    else:
        ac = AgglomerativeClustering(n_clusters=n_size, affinity=affinity).fit_predict(mat)
    return ac


def print_words_by_labels(labels):
    label_list = dict()
    for i, k in enumerate(labels):
        if k not in label_list.keys():
            label_list[k] = [i+1]
        else:
            label_list[k].append(i+1)
    return label_list

def print_docs_by_labels(labels, id_to_doc):
    label_list = dict()
    for i, k in enumerate(labels):
        if k not in label_list.keys():
            label_list[k] = [id_to_doc[i]]
        else:
            label_list[k].append(id_to_doc[i])
    return label_list


def cal_std_mean(full_list):
    all_sim = [each[1] for each in full_list]
    std = np.std(all_sim)
    mean = np.mean(all_sim)
    return std, mean, min(all_sim), max(all_sim)


def print_cluster_perform(aff_mat, cluster_labels, ss_metric):
    ss = metrics.silhouette_score(aff_mat, cluster_labels, metric=ss_metric)
    print '[INF]: Sihouette Score = %s' % ss
    ch = metrics.calinski_harabaz_score(aff_mat, cluster_labels)
    print '[INF]: Calinski-Harabaz Index = %s' % ch
    db = metrics.davies_bouldin_score(aff_mat, cluster_labels)
    print '[INF]: Davies-Bouldin Index = %s' % db
    return ss, ch, db


def cluster_perf_evaluation(org_labels, res_labels, outfile):
    ars = metrics.adjusted_rand_score(org_labels, res_labels)
    print '[INF]: Adjusted Rand Score = %s' % ars
    amis = metrics.normalized_mutual_info_score(org_labels, res_labels)
    print '[INF]: Normalized Mutual Info Score = %s' % amis
    hs = metrics.homogeneity_score(org_labels, res_labels)
    print '[INF]: Homogeneity Score = %s' % hs
    cs = metrics.completeness_score(org_labels, res_labels)
    print '[INF]: Completeness Score = %s' % cs
    vms = metrics.v_measure_score(org_labels, res_labels)
    print '[INF]: V-measure Score = %s' % vms
    fmi = metrics.fowlkes_mallows_score(org_labels, res_labels)
    print '[INF]: Fowlkes-Mallows Score = %s' % fmi
    outfile.write("""\n\tAdjusted Rand Score = %s\n\tNormalized Mutual Info Score = %s\n\tHomogeneity Score = %s
                  \n\tCompleteness Score = %s\n\tV-measure Score = %s\n\tFowlkes-Mallows Score = %s\n""" % (ars, amis,hs,cs,vms, fmi))

# def main(dataset, c_size):
def main(folder):
    global conn, cur, TOTAL_DOC
    dataset = folder[:folder.find('_')]
    col_name = folder[folder.find('_')+1:]

    if dataset == 'lee' or dataset == 'leefixsw':
        TOTAL_DOC = 50
    elif dataset == "20news18828":
        TOTAL_DOC = 18828
    elif dataset == "20news50":
        TOTAL_DOC = 18828
    elif dataset == "20news50short":
        TOTAL_DOC = 1000
    elif dataset == "20news50short10":
        # for 10 classes
        #TOTAL_DOC = 500
        # for 5 classes
        TOTAL_DOC = 250


    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn = sqlite3.connect("%s%s.db" % (data_path, dataset))
    cur = conn.cursor()

    #doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'talk.politics.guns': 1, 'alt.atheism': 2, 'sci.space': 3,
    #                  'rec.motorcycles': 4, 'misc.forsale': 5,'sci.med': 6, 'sci.electronics': 7, 'rec.sport.hockey': 8,
    #                  'talk.politics.mideast': 9}
    doc_categories = {'comp.sys.ibm.pc.hardware': 0, 'sci.med': 1, 'rec.motorcycles': 2, 'rec.sport.hockey': 3,
                      'talk.politics.mideast': 4}
    doc_id_index, id_to_doc = get_doc_ids()
    org_doc_labels = label_org_doc_ids(doc_id_index, doc_categories, TOTAL_DOC)

    run_clustering = 'all'

    outfile = open('%s/workspace/data/%s_doc_cluster_res7.txt' % (os.environ['HOME'], dataset), 'w+')
    outfile.write(str(doc_id_index))

    aff_matrix = None
    kmeans_matrix = None

    for n_size in [6]:
        # ==>spectral_clustering
        if run_clustering == 'spectral' or run_clustering == 'all':
            if aff_matrix is None:
                full_doc_pair_list = get_doc_sim_from_db(col_name)
                # use number of word_pair to recalulate number of words
                # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                std_value, mean_value, min_value, max_value = cal_std_mean(full_doc_pair_list)
                aff_matrix = build_normed_aff_matrix(full_doc_pair_list, doc_id_index, TOTAL_DOC, minv=min_value, maxv=max_value)
            # np.savetxt("foo.csv", X=aff_matrix, delimiter=",")
            # test for using sim vects to compute cosine sims for spectral's input
            #kmeans_matrix = build_kmeans_matrix_sim(full_doc_pair_list, size)
            #aff_matrix = build_aff_matrix_from_kmeans_matrix(kmeans_matrix)
            if aff_matrix[0][0] != 0:
                raise Exception("Aff matrix [i][i] not equal to 0!!")
            sc_labels = spectral_clustering(aff_matrix, n_size)
            print "\nSpectral Clustering [n=%s]\n" % n_size
            outfile.write("\n\nSpectral Clustering [n=%s]\n" % n_size)
            # print_docs_by_labels(sc_labels)
            if kmeans_matrix is None and aff_matrix is not None:
                kmeans_matrix = copy.deepcopy(aff_matrix)
                for i in range(TOTAL_DOC):
                    kmeans_matrix[i][i] = 1
            outfile.write("\n%s\n" % print_docs_by_labels(sc_labels, id_to_doc))
            sss, sch, sdb = print_cluster_perform(kmeans_matrix, sc_labels, 'precomputed')
            outfile.write('\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (sss, sch, sdb))

            cluster_perf_evaluation(org_doc_labels, sc_labels, outfile)

        # ==> kmeans
        if run_clustering == 'kmeans' or run_clustering == 'all':
            if kmeans_matrix is None:
                if aff_matrix is None:
                    full_doc_pair_list = get_doc_sim_from_db()
                    # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                    std_value, mean_value, min_value, max_value = cal_std_mean(full_doc_pair_list)
                    kmeans_matrix = build_normed_aff_matrix(full_doc_pair_list, doc_id_index, TOTAL_DOC, minv=min_value, maxv=max_value)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
                else:
                    kmeans_matrix = copy.deepcopy(aff_matrix)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Kmeans matrix [i][i] not equal to 1!!")
            km_labels = kmeans_clustering(kmeans_matrix, n_size)
            print "\nKmeans Clustering [n=%s]\n" % n_size
            outfile.write("\n\nkmeans_clustering [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_docs_by_labels(km_labels, id_to_doc))
            kss, kch, kdb = print_cluster_perform(kmeans_matrix, km_labels,'euclidean')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (kss, kch, kdb))

            cluster_perf_evaluation(org_doc_labels, km_labels, outfile)

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
                    full_doc_pair_list = get_doc_sim_from_db()
                    # size = int((math.sqrt(1 + 8 * len(full_doc_pair_list)) + 1) / 2)
                    std_value, mean_value, min_value, max_value = cal_std_mean(full_doc_pair_list)
                    kmeans_matrix = build_normed_aff_matrix(full_doc_pair_list, doc_id_index, TOTAL_DOC, minv=min_value, maxv=max_value)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
                else:
                    kmeans_matrix = copy.deepcopy(aff_matrix)
                    for i in range(TOTAL_DOC):
                        kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Aff matrix [i][i] not equal to 1!!")
            ac_precomputed_labels = agglomerative_clustering(kmeans_matrix, 'precomputed', n_size)
            print "\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size
            outfile.write("\n\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_docs_by_labels(ac_precomputed_labels, id_to_doc))
            apss, apch, apdb = print_cluster_perform(kmeans_matrix, ac_precomputed_labels, 'precomputed')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (apss, apch, apdb))

            cluster_perf_evaluation(org_doc_labels, ac_precomputed_labels, outfile)

            ac_euclidean_labels = agglomerative_clustering(kmeans_matrix, 'euclidean', n_size)
            print "\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size
            outfile.write("\n\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_docs_by_labels(ac_euclidean_labels, id_to_doc))
            aess, aech, aedb = print_cluster_perform(kmeans_matrix, ac_euclidean_labels, 'euclidean')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (aess, aech, aedb))

            cluster_perf_evaluation(org_doc_labels, ac_euclidean_labels, outfile)

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
main("20news50short10_nasari_40_rmswcbwexpws_w3-3")
