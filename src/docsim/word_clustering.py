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
import socket
import copy


def get_word_sim_from_db():
    cur.execute('SELECT * from words_sim')
    rows = cur.fetchall()
    return rows


def get_words_from_db():
    cur.execute('SELECT word from words_idx')
    rows = cur.fetchall()
    return rows


def build_aff_matrix(full_dicts, size, stdv=None, meanv=None, minv=None, maxv=None):
    aff_mat = np.zeros([size, size], dtype=float)
    for word_idx, sim in full_dicts:
        # normed_sim = (sim-meanv)/stdv
        normed_sim = (sim-minv)/(maxv-minv)
        xidx, yidx = idx_bit_translate.key_to_keys(word_idx)
        aff_mat[int(xidx)][int(yidx)] = normed_sim
        aff_mat[int(yidx)][int(xidx)] = normed_sim
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


def build_kmeans_matrix_nasari(full_word_list):
    pre_matrix = []
    for word in full_word_list:
        try:
            w_v = list(map(float, get_vector_for_nasari(word[0]).split(',')))
        except Exception as e:
            print "%s, err=%s" % (word[0], e)
            raw_input("Enter...")
        else:
            pre_matrix.append(w_v)
    return np.asarray(pre_matrix)


def build_kmeans_matrix_sim(full_dicts, size):
    aff_mat = np.zeros([size, size], dtype=float)
    for word_idx, sim in full_dicts:
        xidx, yidx = idx_bit_translate.key_to_keys(word_idx)
        aff_mat[int(xidx)][int(yidx)] = sim
        aff_mat[int(yidx)][int(xidx)] = sim
    for i in range(size):
        aff_mat[i][i] = 1
    return aff_mat


def get_vector_for_nasari(ww):
    c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    send_str = ww
    send_port = 8306
    send_addr = 'localhost'
    c_sock.sendto(send_str, (send_addr, send_port))
    while True:
        try:
            c_sock.settimeout(1.0)
            ret_str, serv_addr = c_sock.recvfrom(8192)
            ret = ret_str
            # print "[DBG]: send_word_sim_request:" + str(ret)
            c_sock.close()
            return ret
        # except socket.error, msg:
        except Exception as e:
            print "[ERR]: Cannot get word similarity! Resend!"
            print e.message
            c_sock.close()
            c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            c_sock.sendto(send_str, (send_addr, send_port))
            # time.sleep(random.randint(1, 6))


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


# def main(dataset, c_size):
def main():
    dataset = '20news50short10'
    # c_size = int(sys.argv[1])

    run_clustering = 'all'

    global conn, cur
    conn = sqlite3.connect('%s/workspace/data/%s.db' % (os.environ['HOME'], dataset))
    cur = conn.cursor()

    outfile = open('%s/workspace/data/%s_word_cluster_res1.txt' % (os.environ['HOME'], dataset), 'a+')

    aff_matrix = None
    kmeans_matrix = None
    size = 0
    aff_matrix_agglo = None

    for n_size in [1000, 1500, 2000, 2500]:
        # ==>spectral_clustering
        if run_clustering == 'spectral' or run_clustering == 'all':
            if aff_matrix is None:
                full_word_pair_list = get_word_sim_from_db()
                # use number of word_pair to recalulate number of words
                size = int((math.sqrt(1 + 8 * len(full_word_pair_list)) + 1) / 2)
                # std_value, mean_value, min_value, max_value = cal_std_mean(full_word_pair_list)
                aff_matrix = build_aff_matrix(full_word_pair_list, size, minv=-1, maxv=1)
            # np.savetxt("foo.csv", X=aff_matrix, delimiter=",")
            # test for using sim vects to compute cosine sims for spectral's input
            #kmeans_matrix = build_kmeans_matrix_sim(full_word_pair_list, size)
            #aff_matrix = build_aff_matrix_from_kmeans_matrix(kmeans_matrix)
            if aff_matrix[0][0] != 0:
                raise Exception("Aff matrix [i][i] not equal to 0!!")
            sc_labels = spectral_clustering(aff_matrix, n_size)
            print "\nSpectral Clustering [n=%s]\n" % n_size
            outfile.write("\n\nSpectral Clustering [n=%s]\n" % n_size)
            # print_docs_by_labels(sc_labels)
            if kmeans_matrix is None and aff_matrix is not None:
                kmeans_matrix = copy.deepcopy(aff_matrix)
                for i in range(size):
                    kmeans_matrix[i][i] = 1
            outfile.write("\n%s\n" % print_words_by_labels(sc_labels))
            sss, sch, sdb = print_cluster_perform(kmeans_matrix, sc_labels, 'precomputed')
            outfile.write('\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (sss, sch, sdb))

        # ==> kmeans
        if run_clustering == 'kmeans' or run_clustering == 'all':
            if False:
                # Method1: Use nasari
                full_word_list = get_words_from_db()
                kmeans_matrix = build_kmeans_matrix_nasari(full_word_list)
            else:
                # Method2: use word pair sim
                if kmeans_matrix is None:
                    if aff_matrix is None:
                        full_word_pair_list = get_word_sim_from_db()
                        size = int((math.sqrt(1 + 8 * len(full_word_pair_list)) + 1) / 2)
                        kmeans_matrix = build_kmeans_matrix_sim(full_word_pair_list, size)
                    else:
                        kmeans_matrix = copy.deepcopy(aff_matrix)
                        for i in range(size):
                            kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Kmeans matrix [i][i] not equal to 1!!")
            km_labels = kmeans_clustering(kmeans_matrix, n_size)
            print "\nKmeans Clustering [n=%s]\n" % n_size
            outfile.write("\n\nkmeans_clustering [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_words_by_labels(km_labels))
            kss, kch, kdb = print_cluster_perform(kmeans_matrix, km_labels,'euclidean')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (kss, kch, kdb))

        if run_clustering == 'agglo' or run_clustering == 'all':
            # if aff_matrix is None:
            #     full_word_pair_list = get_word_sim_from_db()
            #     size = int((math.sqrt(1 + 8 * len(full_word_pair_list)) + 1) / 2)
            #     # use number of word_pair to recalulate number of words
            #     # std_value, mean_value, min_value, max_value = cal_std_mean(full_word_pair_list)
            #     aff_matrix = build_aff_matrix(full_word_pair_list, size, minv=-1, maxv=1)
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
                    full_word_pair_list = get_word_sim_from_db()
                    size = int((math.sqrt(1 + 8 * len(full_word_pair_list)) + 1) / 2)
                    kmeans_matrix = build_kmeans_matrix_sim(full_word_pair_list, size)
                else:
                    kmeans_matrix = copy.deepcopy(aff_matrix)
                    for i in range(size):
                        kmeans_matrix[i][i] = 1
            if kmeans_matrix[0][0] != 1:
                raise Exception("Aff matrix [i][i] not equal to 1!!")
            ac_precomputed_labels = agglomerative_clustering(kmeans_matrix, 'precomputed', n_size)
            print "\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size
            outfile.write("\n\nAgglomerative Clustering - precomputed [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_words_by_labels(ac_precomputed_labels))
            apss, apch, apdb = print_cluster_perform(kmeans_matrix, ac_precomputed_labels, 'precomputed')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (apss, apch, apdb))

            ac_euclidean_labels = agglomerative_clustering(kmeans_matrix, 'euclidean', n_size)
            print "\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size
            outfile.write("\n\nAgglomerative Clustering - euclidean [n=%s]\n" % n_size)
            outfile.write("\n%s\n" % print_words_by_labels(ac_euclidean_labels))
            aess, aech, aedb = print_cluster_perform(kmeans_matrix, ac_euclidean_labels, 'euclidean')
            outfile.write(
                '\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (aess, aech, aedb))

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
main()
