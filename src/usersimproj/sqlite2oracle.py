import sqlite3
import cx_Oracle

def main():
    try:
        sqlite_db_name = '/home/fcmeng/gh_data/clean_text.db'
        sqlite_db_conn = sqlite3.connect(sqlite_db_name)
        sqlite_db_cur = sqlite_db_conn.execute(''' SELECT * FROM tb_user_text_full; ''')

        oracle_db_name = 'socsimtext/CoBD4uNdsslprdexT@ndsslprd'
        oracle_db_conn = cx_Oracle.connect(oracle_db_name)
        oracle_db_cur = oracle_db_conn.cursor()

        rec_count = 0
        l_rec = []
        for row in sqlite_db_cur:
            l_rec.append((row[0], row[1], row[2], "", ""))
            rec_count += 1
            if rec_count > 1000:
                oracle_db_cur.bindarraysize = len(l_rec)
                oracle_db_cur.setinputsizes(22, 20, 4000, 4000, 4000)
                oracle_db_cur.executemany("INSERT INTO tb_user_text_full(user_id, time, clean_text, tagged_text, parse_trees) VALUES (:1, :2, :3, :4, :5)", l_rec)
                oracle_db_conn.commit()
                rec_count = 0
                del l_rec[:]
    except sqlite3.Error as e:
        print(e)
        sqlite_db_conn.close()
        oracle_db_conn.close()
        return None
    finally:
        sqlite_db_conn.close()
        oracle_db_conn.close()
    
main()
