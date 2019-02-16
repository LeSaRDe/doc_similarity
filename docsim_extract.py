import sqlite3
import csv
from operator import itemgetter

run_name = 'nasari_40_rmswcbwexpwscycrbf_w3-2'

def dump_dsctp_results():
    try:
        db_conn = sqlite3.connect('/home/fcmeng/workspace/data/lee.db')
        db_cur = db_conn.cursor()
        db_cur.execute('select doc_id_pair, [{0}] from docs_sim'.format(run_name))
        outfile = open('/home/fcmeng/workspace/data/{0}_dump.txt'.format(run_name), 'w+')
        compfile = open('/home/fcmeng/workspace/data/{0}_compare.txt'.format(run_name), 'w+')
        count = 0
        doc_sims = []
        for i in range(1, 51):
            for j in range(i+1, 51):
                sql_str = 'select "{0}" from docs_sim where doc_id_pair = "{1}"'.format(run_name, str(i)+"#"+str(j))
                db_cur.execute(sql_str)
                rec = db_cur.fetchone()
                outfile.write("{0}#{1}:{2}".format(i, j, str(rec[0])) + '\n')
                doc_sims.append(str(rec[0]))
                count += 1
        print count
        doc_sims_str = ','.join(doc_sims)
        compfile.write(doc_sims_str + '\n')
        outfile.close()
        compfile.close()
        db_conn.close()
    except Exception as e:
        print e

def dump_lee_results():
    try:
        lee_sim_file = open('/home/fcmeng/workspace/data/lee_sim.csv', 'r')
        reader = csv.reader(lee_sim_file, delimiter=',')
        count = 0
        outfile = open('/home/fcmeng/workspace/data/lee_sim_dump.txt', 'w+')
        compfile = open('/home/fcmeng/workspace/data/{0}_compare.txt'.format(run_name), 'a+')
        doc_sims = []
        for i, row in enumerate(reader):
            for j in range(i+1, 50):
                count += 1
                outfile.write("{0}#{1}:{2}".format(i+1, j+1, row[j]) + '\n')
                doc_sims.append(str(row[j]))
                #print "{0}#{1}:{2}".format(i+1, j+1, row[j])
        print count
        doc_sims_str = ','.join(doc_sims)
        compfile.write(doc_sims_str + '\n')
        outfile.close()
        compfile.close()
    except Exception as e:
        print e

def dump_cycle_count():
    ret = []
    try:
        count_file = open('/home/fcmeng/workspace/data/cycle_count.txt', 'r')
        count_file.seek(0, 0)
        recs = count_file.readlines()
        for rec in recs:
            arr1 = rec.split(':')
            arr2 = arr1[0].split('#')
            doc1 = int(arr2[0].strip())+1
            doc2 = int(arr2[1].strip())+1
            count = float(arr1[1].strip())
            ret.append([doc1, doc2, count])
        ret2 = []
        for i in range(1, 50):
            for j in range(i+1, 51):
                ret2.append([ ele for ele in ret if (ele[0]==i and ele[1]==j) or (ele[0]==j and ele[1]==i) ][0])
        outfile = open('/home/fcmeng/workspace/data/cycle_count_dump.txt', 'w')
        count_15 = 0
        for item in ret2:
            if item[2] >= 15:
                count_15 += 1
            outfile.write("{0}#{1}:{2}\n".format(item[0], item[1], item[2]))
        outfile.close()
        print count_15
    except Exception as e:
        print e

def main():
    dump_dsctp_results()
    #dump_lee_results()
    #dump_cycle_count()

main()
