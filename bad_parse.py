import sqlite3
import sys

def main():
    db_conn = sqlite3.connect('/home/fcmeng/workspace/data/20news18828_nice.db')
    db_cur = db_conn.cursor()
    doc_id = sys.argv[1]
    db_cur.execute("update docs set parse_trees = 'bad_parse' where doc_id = ?", [doc_id])
    db_conn.commit()
    db_cur.execute("select * from docs where doc_id = ?", [doc_id])
    print db_cur.fetchone()
    print "Done!"
    db_conn.close()

main()
