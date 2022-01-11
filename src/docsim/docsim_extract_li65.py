import os
import json

#run_name = 'li65_nasari_30_rmswcbwexpws_w3-3'
#run_name = 'sts_adw_10_rmswcbwexpws_w3-3'
run_name = 'sick_lexvec_50_rmswcbwexpwspcomb_w3-3'


def main():
    files_path = '/home/%s/workspace/data/docsim/%s/' % (os.environ['USER'], run_name)
    cnt = 0
    out_file = open(files_path + run_name + 'dump.txt', 'w+')
    for file in os.listdir(files_path):
        if file.endswith(".json"):
            with open(files_path+file, 'r') as infile:
                keys = file.replace('.json', '').split('#')
                json_data = json.load(infile)
                sim  = json_data['sim']
                # sim = infile.readlines()[1].replace(',','').strip()
                print(sim)
                out_file.write(str(keys[0]) + '_' + str(sim) + '\n')
                infile.close()
                cnt+=1
    out_file.close()


if __name__ == '__main__':
    main()
