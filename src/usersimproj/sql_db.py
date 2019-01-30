import sqlite3
import sys
import json
import os
import time

g_conn = None
g_cur = None
g_total_ln_count = 0
g_count_1 = 0
g_count_2 = 0


def setup_sqlite(db_name):
    try:
        #db_name = raw_input("Please input the DB name:")
        #if db_name == None or db_name == "":
        #   print "[ERR]: DB name is invalid!"
        #   return None
        #db_name = folder_path + '_step_2.db.tmp'
        db_conn = sqlite3.connect(db_name)
    except sqlite3.Error as e:
        print(e)
        db_conn.close()
        return None
    return db_conn

def create_user_text_tb():
    g_cur.execute(''' CREATE TABLE tb_user_text (user_id text, time_stamp text, type text, text_str text) ''')
    g_conn.commit()


def insert_data_from_json(file_name):
    global g_count_1
    global g_count_2
    with open(file_name, 'r') as in_file:
        in_file.seek(0, 0)
        for line in in_file:
            json_line_data = json.loads(line)
            #print(json_line_data['id_h'])
            insert_data = [json_line_data['user'], json_line_data['created_at'], json_line_data['type'], json_line_data['text_m']]
            g_cur.execute(' INSERT INTO tb_user_text VALUES (?, ?, ?, ?) ', insert_data)
            g_count_1 += 1
            if g_count_1 >= 100.0:
                g_count_1 = 0
                g_count_2 += 1
            prog = "{:.0%}".format((g_count_1 + g_count_2*100.0)/g_total_ln_count)
            if (int(g_count_1 + g_count_2*100.0) % 50000) == 0:
                g_conn.commit()
            sys.stdout.write("\r")
            sys.stdout.write(prog)
            sys.stdout.flush()

# argv[1]: folder_path
def main():
    global g_conn
    global g_total_ln_count

    #g_conn = sqlite3.connect(sys.argv[1])
    db_tmp_name = sys.argv[1] + '_step_2.db.tmp'
    db_name = sys.argv[1] + '_step_2.db'
    g_conn = setup_sqlite(db_tmp_name)
    #print(sys.argv[1])
    #if g_conn == None:
    #    print('[ERR]:' + 'Failed to connect to ' + sys.argv[1])
    #    return

    global g_cur
    g_cur = g_conn.cursor()
    if g_cur == None:
        print('[ERR]:' + 'DB cursor is None!')
        return

    in_json_file = sys.argv[1] + '_step_1.json'
    while not os.path.exists(in_json_file):
        time.sleep(2)
    g_total_ln_count = len(open(in_json_file, 'r').readlines())
    create_user_text_tb()
    insert_data_from_json(in_json_file)
    g_conn.commit()

    g_cur.execute(''' SELECT * FROM tb_user_text ''')
    #print(g_cur.fetchone())
    g_conn.close()
    os.rename(db_tmp_name, db_name)
    print db_name + " is done!"

main()
