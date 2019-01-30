
import sqlite3
import sys

g_db_name = '/home/fcmeng/gh_data/clean_text.db'

def main():
    # open db
    try:
        db_conn = sqlite3.connect(g_db_name)
    except sqlite3.Error as e:
        print(e)
        db_conn.close()
    db_cur = db_conn.cursor()
    if db_cur == None:
        print('[ERR]:' + 'DB cursor is None!')
        return

    # read text rec
    file_path = sys.argv[1]
    with open(file_path, 'r') as in_file:
        in_file.seek(0, 0)
        for line in in_file:
            fields = line.split('=')
            user_id = fields[0].strip()
            time = fields[1].strip()
            tagged_sent = fields[2].strip()
            parse_trees = fields[3].strip()
            update_data = [tagged_sent, parse_trees, user_id, time] 
            db_cur.execute(' UPDATE tb_user_text_full SET tagged_text=?, parse_trees=? WHERE user_id=? AND time=?', update_data)
        db_conn.commit()
        in_file.close()
    db_conn.close()

main()
