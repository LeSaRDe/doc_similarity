import numpy as np
from sklearn.cluster import SpectralClustering, AgglomerativeClustering, KMeans
from sklearn import metrics
from sklearn.metrics.pairwise import cosine_distances


def get_total_doc_size(dataset):
    if dataset == 'lee' or dataset == 'leefixsw':
        return 50
    elif dataset == "20news18828":
        return 18828
    elif dataset == "20news50":
        return 18828
    elif dataset == "20news50short":
        return 1000
    elif dataset == "20news50short10":
        # for 10 classes
        # TOTAL_DOC = 500
        # for 5 classes
        return 250


def get_doc_ids(cur, dataset_flag):
    if dataset_flag == '20news50short10-250':
        cur.execute('''SELECT doc_id from docs
                        where doc_id not like "%talk.politics.guns%" 
                        and doc_id not like "%alt.atheism%" 
                        and doc_id not like "%misc.forsale%" 
                        and doc_id not like "%sci.med%" 
                        and doc_id not like "%sci.electronics%" 
                        order by doc_id''')
    else:
        cur.execute("SELECT doc_id from docs order by doc_id")
    rows = cur.fetchall()
    doc_ids = dict()
    id_to_doc = dict()
    for i, row in enumerate(rows):
        doc_ids[row[0]] = i
        id_to_doc[i] = row[0]
    return doc_ids, id_to_doc


def get_doc_sim_from_db(col, cur, dataset_flag):
    if dataset_flag == '20news50short10-250':
        cur.execute('''SELECT doc_id_pair, "'''+col+'''" from docs_sim 
                        where doc_id_pair not like "%talk.politics.guns%" 
                        and doc_id_pair not like "%alt.atheism%" 
                        and doc_id_pair not like "%misc.forsale%" 
                        and doc_id_pair not like "%sci.med%" 
                        and doc_id_pair not like "%sci.electronics%" 
                        order by doc_id_pair''')
    else:
        cur.execute('''SELECT doc_id_pair, "''' + col + '''" from docs_sim order by doc_id_pair''')
    rows = cur.fetchall()
    return rows


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
        if minv is not None and maxv is not None:
            normed_sim = (sim-minv)/(maxv-minv)
        else:
            normed_sim = sim
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


def output_clustering_res(cluster_name, n_size, res_labels, id_to_doc, outfile):
    print "\n%s Clustering [n=%s]\n" % (cluster_name, n_size)
    outfile.write("\n\n%s Clustering [n=%s]\n" % (cluster_name, n_size))
    outfile.write("\n%s\n" % print_docs_by_labels(res_labels, id_to_doc))


def print_cluster_perform(aff_mat, cluster_labels, ss_metric, outfile):
    ss = metrics.silhouette_score(aff_mat, cluster_labels, metric=ss_metric)
    print '[INF]: Sihouette Score = %s' % ss
    ch = metrics.calinski_harabaz_score(aff_mat, cluster_labels)
    print '[INF]: Calinski-Harabaz Index = %s' % ch
    db = metrics.davies_bouldin_score(aff_mat, cluster_labels)
    print '[INF]: Davies-Bouldin Index = %s' % db
    # return ss, ch, db
    outfile.write('\tSihouette Score = %s\n\tCalinski-Harabaz Index = %s\n\tDavies-Bouldin Index = %s' % (ss, ch, db))


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