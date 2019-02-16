import sqlite3
import os

INSERT_DOCS_PAIR_KEYS = True
TOTAL_DOC = 50


def update_sim(doc_key,  col, sim, count, doc_key1=None):
    if doc_key1:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=? or doc_id_pair=?' % col, (sim,doc_key,doc_key1))
    else:
        cur.execute('UPDATE docs_sim set "%s"=? WHERE doc_id_pair=?' % col, (sim,doc_key))
    if count % 5000 == 0:
        conn.commit()


def main(project_name):
    dataset_name, col_name = project_name[:project_name.find('_')], project_name[project_name.find('_')+1:]
    print dataset_name, col_name

    global conn, cur
    conn = sqlite3.connect("/home/fcmeng/workspace/data/%s.db" % dataset_name)
    cur = conn.cursor()

    if INSERT_DOCS_PAIR_KEYS:
        cur.execute('SELECT count(*) from docs_sim')
        num_rows = cur.fetchone()[0]
        if num_rows != TOTAL_DOC * (TOTAL_DOC-1) / 2:
            if num_rows != 0:
                print "Total Doc pair keys don't match the num of docs, something is wrong. Need to empty the keys and re-insert."
            else:
                key_file = '/home/fcmeng/workspace/doc_similarity/res/lee_all_keys.txt'
                with open(key_file, 'r') as infile:
                    keys = infile.readlines()
                    for i, line in enumerate(keys):
                        line = eval(line)
                        key = "%s#%s" % (line[0], line[1])
                        # print key
                        cur.execute('INSERT INTO docs_sim (doc_id_pair) VALUES (?)', (key,))
                        if i+1 % 5000 == 0:
                            conn.commit()
                    conn.commit()
                    cur.close()
                    conn.close()
                    return
    else:
        print "Doc pair keys are already inserted."


    files_path = '/home/fcmeng/workspace/data/%s/' % project_name
    cnt = 0
    for file in os.listdir(files_path):
        if file.endswith(".json"):
            with open(files_path+file, 'r') as infile:
                if 'lee' in dataset_name:
                    keys = file.replace('.json', '').split('#')
                    key = "%s#%s" % (int(keys[0])+1,int(keys[1])+1)
                    key1 = "%s#%s" % (int(keys[1])+1,int(keys[0])+1)
                else:
                    key = file.replace('_', '/').replace('.json', '')
                    key1 = None
                sim = eval(infile.readlines()[1].replace(',',''))
                print key, sim
                infile.close()
                cnt+=1
                update_sim(key, col_name, sim, cnt, key1)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    # doc pair wise sim column format: nasari_<word_sim_threshold>_<stopwords>_<weighta-b>
    # Step1: Create the table if the table not exist
    # CREATE TABLE docs_sim (doc_id_pair text primary key not null, "nasari_30_rmsw_w3-2" real);
    # Step2: Add column if the column doesn't exist
    # ALTER TALBE docs_sim ADD <column_name> real;

    # Give a folder name, the folder name must match the column name
    main('lee_nasari_50_rmswcbwexpwsn30_w3-2')
