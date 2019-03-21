import json


# file_name format: dataset_<para1_para2_..>_phrase_clusters_by_phrase.json
def transfer_to_cluster_by_cluster_id(path_to_cluster_files, file_name):
    if "_by_phrase" not in file_name:
        raise Exception("File name is wrong!!")
    else:
        cluster_by_clusterid = dict()
        indata = json.load(open(path_to_cluster_files+file_name, 'r'))
        for phrase, c_id in indata.items():
            if c_id not in cluster_by_clusterid.keys():
                cluster_by_clusterid[c_id] = [phrase]
            else:
                cluster_by_clusterid[c_id].append(phrase)
        with open(path_to_cluster_files+file_name.replace('_by_phrase', '_by_clusterid'), 'w') as outfile:
            json.dump(cluster_by_clusterid, outfile, indent=4)


# file_name format: dataset_<para1_para2_..>_phrase_clusters_by_clusterid.json
def transfer_to_cluster_by_phrase(path_to_cluster_files, file_name, reorder_cluster_id=False):
    if "_by_clusterid" not in file_name:
        raise Exception("File name is wrong!!")
    else:
        cluster_by_phrase = dict()
        indata = json.load(open(path_to_cluster_files+file_name, 'r'))
        if reorder_cluster_id:
            cluster_id = 0
            for c_id, phrases in indata.items():
                for each_phrase in phrases:
                    if each_phrase not in cluster_by_phrase.keys():
                        cluster_by_phrase[each_phrase] = cluster_id
                    else:
                        print "[ERR] Phrase %s:%s already in the cluster. Duplicated phrase!!" % (
                            each_phrase, cluster_by_phrase[each_phrase])
                cluster_id += 1
        else:
            for c_id, phrases in indata.items():
                for each_phrase in phrases:
                    if each_phrase not in cluster_by_phrase.keys():
                        cluster_by_phrase[each_phrase] = c_id
                    else:
                        print "[ERR] Phrase %s:%s already in the cluster. Duplicated phrase!!" % (
                            each_phrase, cluster_by_phrase[each_phrase])

        with open(path_to_cluster_files+file_name.replace('_by_clusterid', '_by_phrase'), 'w') as outfile:
            json.dump(cluster_by_phrase, outfile, indent=4)


# transfer_to_cluster_by_cluster_id('%s/workspace/data/' % os.environ['HOME'], '20news50short10_rbsc_phrase_clusters_by_phrase.json')
# transfer_to_cluster_by_phrase('%s/workspace/data/' % os.environ['HOME'], '20news50short10_rbsc_phrase_clusters_by_clusterid.json')