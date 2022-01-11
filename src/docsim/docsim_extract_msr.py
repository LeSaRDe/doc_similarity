import os

run_name = 'msr_nasari_45_rmswcbwexpws_w3-3'

def main():
    files_path = '/home/%s/workspace/data/%s/' % (os.environ['USER'], run_name)
    cnt = 0
    out_file = open(files_path + 'dump.txt', 'w+')
    for file in os.listdir(files_path):
        if file.endswith(".json"):
            with open(files_path+file, 'r') as infile:
                keys = file.replace('.json', '').split('#')
                sim = infile.readlines()[1].replace(',','').strip()
                print sim
                out_file.write(str(keys[0]) + '#' + str(keys[1]) + ':' + sim + '\n')
                infile.close()
                cnt+=1
    out_file.close()

main()
