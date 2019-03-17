import os
import json


def main(project_name, cycle):
    # dataset_name, col_name = project_name[:project_name.find('_')], project_name[project_name.find('_') + 1:]
    all_words = set()

    files_path = '/home/%s/workspace/data/%s/' % (os.environ['USER'], project_name)
    cnt = 0
    for ff in os.listdir(files_path):
        if ff.endswith(".json"):
            with open(files_path + ff, 'r') as infile:
                if cycle == 'cycle':
                    words = json.load(infile)['word_list']
                else:
                    words = eval(infile.readlines()[2]).split(",")
                for w in words:
                    if w and w not in all_words:
                        all_words.add(w)
                        cnt += 1
                infile.close()
    with open('/home/%s/workspace/data/%s_words.txt' % (os.environ['USER'], project_name), 'w') as outfile:
        outfile.write(','.join(all_words))
        outfile.close()

    print "Done. Total %s words." % cnt


if __name__ == '__main__':
    # Give a folder name, the folder name must match the column name
    # Use "cycle" when the doc_sim txt file contains cycles; otherwise, use "no cycle"
    main('leefixsw_nasari_40_rmswcbwexpws_w3-3', 'no cycle')
