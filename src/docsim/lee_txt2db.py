import sqlite3

# the original dataset file
# ORIG_DATA_FILE_PATH = "/home/fcmeng/workspace/data/lee_background.cor"
# ORIG_DATA_FILE_PATH = "/home/fcmeng/workspace/data/stsbenchmark/col2.txt"
ORIG_DATA_FILE_PATH = "/home/fcmeng/workspace/data/sick/col2.txt"
# the output db 
# DB_PATH = "/home/fcmeng/workspace/data/lee_bg.db"
# DB_PATH = "/home/fcmeng/workspace/data/stsbenchmark/col2.db"
DB_PATH = "/home/fcmeng/workspace/data/sick/col2.db"
COMMIT_MAX = 100

# 'docs' table:
# the schema of this table is fixed so far
# (doc_id text primary key not null, 
#  pre_ner text not null,
#  word_list text,
#  parse_trees text)

def main():
    rec_file = open(ORIG_DATA_FILE_PATH, "r")
    db_conn = connect_db(DB_PATH)
    if not create_docs_table(db_conn):
        print "[ERR]: Failed at creating docs table!"
        return
    #update_all_rec_word_list_to_docs(db_conn)
    insert_recs_to_docs(db_conn, rec_file)
    db_conn.close()
    print "ALL DONE!"

def connect_db(db_path):
    try:
        db_conn = sqlite3.connect(db_path)
        return db_conn
    except sqlite3.Error as e:
        print e
    return None

def create_docs_table(db_conn):
    try:
        sql_str = '''create table if not exists docs 
        (doc_id text primary key not null, 
        pre_ner text not null, 
        word_list text, 
        tagged_text text, 
        parse_trees text)'''
        if db_conn is not None:
            db_cur = db_conn.cursor()
            db_cur.execute(sql_str)
            db_conn.commit()
            return True
    except sqlite3.Error as e:
        print e
    return False

# by this point, we leave 'tagged_text' and 'parse_trees' to our Java code.
def insert_recs_to_docs(db_conn, rec_file):
    try:
        sql_str = '''insert into docs(doc_id, pre_ner, word_list, tagged_text, parse_trees) 
        values(?, ? ,? , ?, ?)'''
        db_cur = db_conn.cursor()
        rec_count = 0
        rec_batch_count = 0
        total_rec_count = sum(1 for ln in rec_file)
        print "[INF]: total record count = ", total_rec_count
        rec_file.seek(0, 0)
        for index, line in enumerate(rec_file):
            if line is None or len(str(line)) == 0:
                print "[DBG]: current line is empty!"
                continue
            doc_id = str(index)
            pre_ner = str(line)
            word_list = get_word_list_from_txt(pre_ner)
            tagged_text = None
            parse_trees = None
            db_cur.execute(sql_str, (doc_id, pre_ner, word_list, tagged_text, parse_trees))
            rec_count += 1
            if rec_count >= COMMIT_MAX:
                db_conn.commit()
                rec_batch_count += 1
                print "[DBG]: {0:.0%}".format(float(rec_batch_count*COMMIT_MAX) / float(total_rec_count))
                rec_count = 0
        db_conn.commit()
        print "[DBG]: {0:.0%}".format(float(total_rec_count) / float(total_rec_count))
    except sqlite3.Error as e:
        print "[ERR]: ", e
                

def update_one_rec_to_docs(db_conn, doc_id, field, new_val):
    try:
        sql_str = "update docs set {0} = ? where doc_id = ?".format(field)
        db_cur = db_conn.cursor()
        db_cur.execute(sql_str, (new_val, doc_id))
        db_conn.commit()
        print "[DBG]: Updated rec = "
        sql_str = "select doc_id, {0} from docs where doc_id = ?".format(field)
        db_cur.execute(sql_str, (doc_id,))
        print db_cur.fetchone()
    except sqlite3.Error as e:
        print "[ERR]: ", e

def update_all_rec_word_list_to_docs(db_conn):
    try:
        sql_str = "select doc_id, pre_ner from docs"
        db_cur = db_conn.cursor()
        db_cur.execute(sql_str)
        all_recs = db_cur.fetchall()
        total_rec_count = len(all_recs)
        done_count = 0
        batch_count = 0
        print "[DBG]: To be updated: ", total_rec_count
        for (doc_id, pre_ner) in all_recs:
            update_one_rec_to_docs(db_conn, doc_id, "word_list", get_word_list_from_txt(pre_ner))
            if done_count >= COMMIT_MAX:
                db_conn.commit()
                batch_count += 1
                print "[DBG]: {0:.0%}".format(float(batch_count*COMMIT_MAX) / float(total_rec_count))
                done_count = 0
        db_conn.commit()
        print "[DBG]: {0:.0%}".format(float(total_rec_count) / float(total_rec_count))
    except sqlite3.Error as e:
        print "[ERR]: ", e

# TODO: implement this function
# Input: a text string
# Output: a string composed by a set of words and separators
def get_word_list_from_txt(txt):
    return None

main()

