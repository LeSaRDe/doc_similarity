import os
import sqlite3
import json
import time


class myNode(object):
    def __init__(self, p):
        self.parent = None
        self.rank = None
        self.cid = None
        self.pstr = p


class UnionFind(object):

    def __init__(self):
        # To hold the clusters
        self.clusters = []

    # create a new set(cluster) with a node
    def makeSet(self, node):
        # set the nodes parent to the node itself
        node.parent = node
        # set initial rank of node to 0
        node.rank = 0
        # add the node to cluster list
        self.clusters.append(node)

    # union the nodeA and nodeB clusters
    def union(self, nodeA, nodeB):

        self.link(self.findSet(nodeA), self.findSet(nodeB))

    # link the nodeA to nodeB or vice versa based upon the rank(number of nodes in the cluster) of the cluster
    def link(self, nodeA, nodeB):

        if nodeA.rank > nodeB.rank:
            nodeB.parent = nodeA
            # remove the nodeB from the cluster list, since it is merged with nodeA
            self.clusters.remove(nodeB)
        else:
            nodeA.parent = nodeB
            # remove the nodeA from the cluster list, since it is merged with nodeB
            self.clusters.remove(nodeA)
            # increase the rank of the cluster after merging the cluster
            if nodeA.rank == nodeB.rank:
                nodeB.rank = nodeB.rank + 1

    # find set will path compress(makes the nodes in cluster points to single leader/parent)
    # and returns the leader/parent of the cluster
    def findSet(self, node):

        if node != node.parent:
            node.parent = self.findSet(node.parent)
        return node.parent

    # get cluster size
    def clusterSize(self):
        return len(self.clusters)


def read_word_cluster(file_path):
    lines = open(file_path, "r").readlines()
    table_name = lines[0]
    word_cluster = eval(lines[1])
    return table_name, word_cluster


def insert_words_to_words_cluster_table(cluster):
    cur = conn.cursor()
    cnt = 0
    for key, value in cluster.items():
        cur.execute('INSERT INTO phrase_cluster(phrase, cluster_id) VALUES (?, ?)', (key, value))
        cnt += 1
    if cnt % 5000 == 1:
        conn.commit()
    cur.close()


def phrase_in_cluster(ps1, ps2, cluster):
    ps1_cluster_key = None
    ps2_cluster_key = None
    if len(ps1) == 2:
        if ps1[0]+"#"+ps1[1] in cluster.keys() or ps1[1]+"#"+ps1[0] in cluster.keys():
            try:
                ps1_cluster_key = cluster[ps1[0]+"#"+ps1[1]]
            except:
                ps1_cluster_key = cluster[ps1[1]+"#"+ps1[0]]
    elif len(ps1) == 1:
        if ps1[0] in cluster.keys():
            ps1_cluster_key = cluster[ps1[0]]
    else:
        raise Exception('Phrase %s length larger than 2 %s' % (ps1, len(ps1)))
    if len(ps2) == 2:
        if ps2[0]+"#"+ps2[1] in cluster.keys() or ps2[1]+"#"+ps2[0] in cluster.keys():
            try:
                ps2_cluster_key = cluster[ps2[0]+"#"+ps2[1]]
            except:
                ps2_cluster_key = cluster[ps2[1] + "#" + ps2[0]]
    elif len(ps2) == 1:
        if ps2[0] in cluster:
            ps2_cluster_key = cluster[ps2[0]]
    else:
        raise Exception('Phrase %s length larger than 2 %s' % (ps2, len(ps2)))
    return ps1_cluster_key, ps2_cluster_key


def is_in_cluster_keys(phrase_list, cluster_keys):
    # Find if a key exist in the cluster with a phrase list, say ['A', 'B']
    # Return False if not found
    if len(phrase_list) == 2:
        if (phrase_list[0]+"#"+phrase_list[1]) in cluster_keys:
            return True
        elif (phrase_list[1]+"#"+phrase_list[0]) in cluster_keys:
            return True
    elif len(phrase_list) == 1:
        if phrase_list[0] in cluster_keys:
            return True
    return False


def get_node_by_phrase_list(phrase_list, cluster):
    # Find a node with a phrase list, say ['A', 'B']
    # Raise error if none is found,
    # because when calling the function, the key has already been checked in the previous step
    if len(phrase_list) == 2:
        try:
            return cluster[phrase_list[0]+"#"+phrase_list[1]][0]
        except:
            try:
                return cluster[phrase_list[1]+"#"+phrase_list[0]][0]
            except:
                raise Exception("%s not found in cluster keys." % phrase_list)
    elif len(phrase_list) == 1:
        try:
            return cluster[phrase_list[0]][0]
        except:
            raise Exception("%s not found in cluster keys." % phrase_list)


def create_phrase_cluster(files_path):
    cluster = dict()
    max_cluster_id = 0
    uf = UnionFind()

    start = time.time()
    for i, each_f in enumerate(os.listdir(files_path)):
        if each_f.endswith(".json"):
            with open(files_path + each_f, 'r') as infile:
                sent_pair_cycles = json.load(infile)['sentence_pair'].values()
                for cycles in sent_pair_cycles:
                    for each_c in cycles['cycles']:
                        ps1 = []
                        ps2 = []
                        for each_n in each_c:
                            if ':L:' in each_n:
                                if each_n[:2] == 's2':
                                    ps2.append(each_n[5:].split('##')[0].lower())
                                elif each_n[:2] == 's1':
                                    ps1.append(each_n[5:].split('##')[0].lower())
                        if not is_in_cluster_keys(ps1, cluster.keys()):
                            if not is_in_cluster_keys(ps2, cluster.keys()):
                                ps1_node = myNode(ps1)
                                ps2_node = myNode(ps2)
                                uf.makeSet(ps1_node)
                                uf.makeSet(ps2_node)
                                uf.union(ps1_node, ps2_node)
                                ps1_node.cid = max_cluster_id
                                ps2_node.cid = max_cluster_id
                                cluster['#'.join(ps1)] = (ps1_node.parent, ps1_node.cid)
                                cluster['#'.join(ps2)] = (ps2_node.parent, ps2_node.cid)
                                max_cluster_id += 1
                            else:
                                ps1_node = myNode(ps1)
                                ps2_node = get_node_by_phrase_list(ps2, cluster)
                                ps2_root = uf.findSet(ps2_node)
                                uf.makeSet(ps1_node)
                                uf.union(ps1_node, ps2_root)
                                ps1_node.cid = ps2_root.cid
                                cluster['#'.join(ps1)] = (uf.findSet(ps1_node), ps1_node.cid)
                        else:
                            if not is_in_cluster_keys(ps2, cluster.keys()):
                                ps2_node = myNode(ps2)
                                ps1_node = get_node_by_phrase_list(ps1, cluster)
                                ps1_root = uf.findSet(ps1_node)
                                uf.makeSet(ps2_node)
                                uf.union(ps1_root, ps2_node)
                                ps2_node.cid = ps1_root.cid
                                cluster['#'.join(ps2)] = (uf.findSet(ps2_node), ps2_node.cid)
                            else:
                                ps1_node = get_node_by_phrase_list(ps1, cluster)
                                ps2_node = get_node_by_phrase_list(ps2, cluster)
                                ps1_root = uf.findSet(ps1_node)
                                ps2_root = uf.findSet(ps2_node)
                                if ps1_root == ps2_root:
                                    continue
                                uf.union(ps1_root, ps2_root)
        if i % 5000 == 0:
            print "%s/124750(%s%%) Time: %s." % (i, float(i)/124750, time.time()-start)

    for p_i in cluster.keys():
        p_i_root = uf.findSet(cluster[p_i][0])
        new_cid = p_i_root.cid
        cluster[p_i] = (p_i_root, new_cid)
                        # ps1_cluster_key, ps2_cluster_key = phrase_in_cluster(ps1, ps2, cluster)
                        # if not ps1_cluster_key:
                        #     if not ps2_cluster_key:
                        #         cluster['#'.join(ps1)] = max_cluster_id
                        #         cluster['#'.join(ps2)] = max_cluster_id
                        #         max_cluster_id += 1
                        #     else:
                        #         cluster['#'.join(ps1)] = ps2_cluster_key
                        # else:
                        #     if not ps2_cluster_key:
                        #         cluster['#'.join(ps2)] = ps1_cluster_key
                        #     else:
                        #         if ps1_cluster_key != ps2_cluster_key:
                        #             raise Exception("Phrase1 (%s, %s) and Phrase2 (%s, %s) are all in cluster, but cluster id don't match!" % (ps1, ps1_cluster_key, ps2, ps2_cluster_key))

    return cluster


def main(folder):
    dataset = folder[:folder.find('_')]
    global conn
    data_path = '%s/workspace/data/' % os.environ['HOME']
    conn = sqlite3.connect("%s%sphraseapvs.db" % (data_path, dataset))

    cluster = create_phrase_cluster(data_path+folder+'/')
    print "Total %s phrases" % len(cluster)

    clean_cluster = dict()
    for key, value in cluster.items():
        clean_cluster[key] = value[1]
    # insert_words_to_words_cluster_table(cluster)
    # insert_words_cluster_mapping(word_cluster, table)
    with open(data_path+dataset+"_phrase_cluster.json", "w") as outfile:
        json.dump(clean_cluster, outfile, indent=4)


if __name__ == '__main__':
    main("20news50short10_nasari_40_rmswcbwexpws_w3-3")