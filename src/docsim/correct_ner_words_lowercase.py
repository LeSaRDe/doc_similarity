import os
import json

PRESERVED_NER_LIST = ['ORGANIZATION', 'LOCATION', 'MISC']


def main(project_name):
    in_files_path = '/home/%s/workspace/data/%s/' % (os.environ['USER'], project_name)
    out_files_path = '/home/%s/workspace/data/%s/' % (os.environ['USER'], project_name+"_1")

    all_words = set()

    w_cnt = 0
    doc_cnt = 1
    for ff in os.listdir(in_files_path):
        if ff.endswith(".json"):
            with open(in_files_path + ff, 'r') as infile:
                org_data = json.load(infile)
                old_words = org_data['word_list']
                sent_pair_cycles = org_data['sentence_pair'].values()
            infile.close()
            if sent_pair_cycles:
                new_words = set()
                for cycles in sent_pair_cycles:
                    for each_c in cycles['cycles']:
                        for each_n in each_c:
                            if ':L:' in each_n:
                                n_list = each_n[5:].split('#')
                                ner = n_list[3].strip()
                                if ner in PRESERVED_NER_LIST:
                                    word = n_list[0]
                                else:
                                    word = n_list[0].lower()
                                new_words.add(word)
                                if word not in all_words:
                                    all_words.add(word)
                                    w_cnt += 1
                org_data['word_list'] = list(new_words)
                print "[%s][%s] Word list diff:" % (doc_cnt, ff), list(set(old_words) - new_words)
            with open(out_files_path + ff, 'w') as outfile:
                json.dump(org_data, outfile, indent=4)
            outfile.close()
            doc_cnt += 1

    print "Total words:%s" % w_cnt
    with open('/home/%s/workspace/data/%s_words.txt' % (os.environ['USER'], project_name), 'w') as outfile:
        outfile.write(','.join(all_words))
    outfile.close()


if __name__ == '__main__':
    # Give a folder name, the folder name must match the column name
    # This program only works with json files with cycles
    main('20news50short10_nasari_40_rmswcbwexpws_w3-3')