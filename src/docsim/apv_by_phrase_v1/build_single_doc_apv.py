import json
import math
import time

PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']


def parse_cycle(each_cycle, doc_key):
    this_arc = 1
    that_arc = 1
    ps = []
    this_phrase_in_sent_locs = []
    for each_n in each_cycle:
        if each_n.startswith(doc_key):
            if ':L:' in each_n:
                phrase_line = each_n[5:].split('#')
                this_phrase_in_sent_locs.append(int(phrase_line[2]))
                if phrase_line[3].strip() in PRESERVED_NER_LIST:
                    ps.append(phrase_line[0].strip())
                else:
                    ps.append(phrase_line[0].strip().lower())
            else:
                this_arc += 1
        else:
            if ':L:' not in each_n:
                that_arc += 1
    this_phrase_in_sent_locs.sort()
    return ps, this_arc, '-'.join(str(loc) for loc in this_phrase_in_sent_locs), that_arc


def find_word_pair_sim(w1, w2, word_pair_sims):
    if w1 == w2:
        return 1.0
    else:
        try:
            return word_pair_sims[w1+"#"+w2]
        except:
            return word_pair_sims[w2+"#"+w1]
        finally:
            return 0.0


def cal_weight(this_arc, c_node_attrs):
    w = 0.0
    for i in range(int(c_node_attrs['cnt'])):
        w = w + math.exp(3.0 / (math.pow(this_arc, 3) + math.pow(c_node_attrs['arc'][i], 3)))
    return w


# calculate sim of 1 phrase vs 1 cluster (with lots of phrases)
def calculate_phrase_vs_cluster_sim(this_phrase, this_arc, c_nodes, word_pair_sims):

    # def find_word_pair_sim(w1, w2):
    #     if w1 == w2:
    #         return 1.0
    #     else:
    #         try:
    #             return word_pair_sims[w1 + "#" + w2]
    #         except:
    #             return word_pair_sims[w2 + "#" + w1]
    #         finally:
    #             return 0.0

    this_p_c_sim_list = []
    for c_node_str, c_node_attrs in c_nodes:
        if int(c_node_attrs['cnt']) != len(c_node_attrs['arc']):
            raise Exception('Node %s cnt %s doesnt match arc length %s' % (c_node_str, c_node_attrs['cnt'], c_node_attrs['arc']))
        each_p = c_node_str.split('#')
        sim = 0.0
        if len(each_p) == 1 and len(this_phrase) == 2:
            sim = min(find_word_pair_sim(each_p[0], this_phrase[0], word_pair_sims),find_word_pair_sim(each_p[0], this_phrase[1],word_pair_sims))
        elif len(this_phrase) == 1 and len(each_p) == 2:
            sim = min(find_word_pair_sim(each_p[0], this_phrase[0],word_pair_sims), find_word_pair_sim(each_p[1], this_phrase[0],word_pair_sims))
        elif len(each_p) == 2 and len(this_phrase) == 2:
            sim1 = min(find_word_pair_sim(each_p[0], this_phrase[0],word_pair_sims), find_word_pair_sim(each_p[1], this_phrase[1],word_pair_sims))
            sim2 = min(find_word_pair_sim(each_p[0], this_phrase[1],word_pair_sims), find_word_pair_sim(each_p[1], this_phrase[0],word_pair_sims))
            sim = max(sim1, sim2)
        elif len(this_phrase) == 1 and len(each_p) == 1:
            # TODO
            # sim = 0? pass for DSCTP
            pass
        else:
            raise Exception("Phrases lengths are wrong!!")
        if sim > 0:
            weight = cal_weight(this_arc, c_node_attrs)
            this_p_c_sim_list.append(weight * sim)
    # return SUM of phrase pair sims
    # TODO: change the return if not using SUM as the phrase-vs-cluster sim
    return sum(this_p_c_sim_list)


def collect_all_phrases(target_doc_id, compare_sim_docs, json_files_path):
    all_phrases = dict()
    for each_compare in compare_sim_docs:
        with open(json_files_path + '/' + each_compare[0].replace('/', '_') + '.json', 'r') as infile:
            sent_pair_cycles = json.load(infile)['sentence_pair']
        infile.close()

        if target_doc_id == each_compare[0].split('#')[0]:
            doc_key = 's1:'
            sent_idx_loc = 0
        elif target_doc_id == each_compare[0].split('#')[1]:
            doc_key = 's2:'
            sent_idx_loc = 1
        else:
            raise Exception("doc name not in the file name!")
        for sent_pair_key, cycles in sent_pair_cycles.items():
            sent_idx = sent_pair_key.split('-')[sent_idx_loc]
            for each_c in cycles['cycles']:
                this_phrase, this_arc, this_phrase_locs, that_arc = parse_cycle(each_cycle=each_c, doc_key=doc_key)
                if sent_idx+"-"+this_phrase_locs not in all_phrases.keys():
                    all_phrases[sent_idx+"-"+this_phrase_locs] = (this_phrase, this_arc)
                # else:
                #     print "%s: %s already exists" % (sent_idx+"-"+this_phrase_locs, this_phrase)
    return all_phrases.values()


def build_single_doc_apv(phrase_cluster_by_clusterid, word_pair_sims, target_doc_id, compare_sim_docs, json_files_path, in_cur):
    start  = time.time()
    global cur
    cur = in_cur

    print "\n[DOC] %s" % target_doc_id
    apv_vec = dict.fromkeys(phrase_cluster_by_clusterid.keys())
    all_phrases = collect_all_phrases(target_doc_id=target_doc_id, compare_sim_docs=compare_sim_docs,
                                      json_files_path=json_files_path)

    print "\tTotal %s phrases." % len(all_phrases)
    for this_phrase, this_arc in all_phrases:
        for c_id, c_nodes in phrase_cluster_by_clusterid.items():
            if isinstance(apv_vec[c_id], list):
                apv_vec[c_id].append(calculate_phrase_vs_cluster_sim(this_phrase, this_arc, c_nodes, word_pair_sims))
            else:
                apv_vec[c_id] = [calculate_phrase_vs_cluster_sim(this_phrase, this_arc, c_nodes, word_pair_sims)]

    for each_c_key, each_sim_list in apv_vec.items():
        apv_vec[each_c_key] = sum(each_sim_list)

    with open(json_files_path+'_apv_vec/'+target_doc_id.replace('/', '_')+'.json', 'w') as outfile:
        json.dump(apv_vec, outfile, indent=4)
    outfile.close()

    print "Total time:", time.time() - start
    return apv_vec
