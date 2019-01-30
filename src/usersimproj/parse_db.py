#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
from sqlite3 import Error
import re
# import nltk
import contractions
# import inflect
from bs4 import BeautifulSoup
from nltk import word_tokenize, sent_tokenize
# from nltk.corpus import stopwords
# from nltk.stem import LancasterStemmer, WordNetLemmatizer, PorterStemmer
# import inspect
import json
import sys
import os
import time

# wordnet_lemmatizer = WordNetLemmatizer()
# print wordnet_lemmatizer.lemmatize("wamon")

# lancaster_stemmer = LancasterStemmer()
# print lancaster_stemmer.stem("learned")

# porter_stemmer = PorterStemmer()
# print porter_stemmer.stem("worn")


p = re.compile("[a-zA-Z0-9_]")
pp = re.compile("^[A-Za-z-,]*$")


def strip_html(text):
    #soup = BeautifulSoup(text, "html.parser")
    soup = BeautifulSoup(text, "lxml")
    return soup.get_text()


def remove_between_square_brackets(text):
    return re.sub('\[[^]]*\]', '', text)


def remove_between_brackets(text):
    return re.sub(r'\([^)]*\)', '', text)


def remove_between_quotes(ss):
    ignore = False
    new_ss = ''
    for c in ss:
        if c is not '`' and ignore is False:
            new_ss = new_ss + str(c)
        elif c is '`' and ignore is False:
            ignore = True
        elif c is '`' and ignore is True:
            ignore = False
    return new_ss
# print remove_between_quotes("hello `this` is another test `string`")


def replace_contractions(text):
    """Replace contractions in string of text"""
    return contractions.fix(text)


def denoise_text(text):
    text = strip_html(text)
    text = remove_between_square_brackets(text)
    text = remove_between_brackets(text)
    text = text.encode("ascii", errors="ignore").decode()
    text = replace_contractions(text)
    return text


def pre_clean(text):
    # separate by space and compose to 1 sentence
    # mainly to remove 'url:https....'
    parts = re.split(" +", text)
    new_text = ''
    for part in parts:
        if 'url:' not in part and 'gs:' not in part and ('http' not in part and '/' not in part) and len(part) < 15:
            new_text = new_text + part + ' '
    return new_text


def add_to_output_json(user, timestamp, sentences):
    if user not in output_json.keys():
        output_json[user] = dict()
        output_json[user][timestamp] = sentences
    else:
        if timestamp not in output_json[user].keys():
            output_json[user][timestamp] = sentences
        else:
            output_json[user][timestamp] += sentences

def setup_sqlite(db_name):
    try:
        #db_name = raw_input("Please input the DB name:")
        #if db_name == None or db_name == "":
        #   print "[ERR]: DB name is invalid!"
        #   return None
        db_conn = sqlite3.connect(db_name)
        db_cur = db_conn.cursor()
        db_cur.execute(''' CREATE TABLE tb_user_text_full (user_id text, time text, clean_text text, tagged_text text, parse_trees text) ''')
        db_conn.commit()
    except Error as e:
        print(e)
        db_conn.close()
        return None
    return db_conn

def add_to_output_sqlite(db_conn, user, timestamp, sentences):
    clean_text = " ".join(sentences)
    insert_data = [user, timestamp, clean_text, "", ""]
    db_cur = db_conn.cursor()
    db_cur.execute(' INSERT INTO tb_user_text_full VALUES (?, ?, ?, ?, ?) ', insert_data)
    return db_cur

# argv[1]: folder path
def main():
    #input_db = raw_input("Please input the source db's name:")
    input_db = sys.argv[1] + '_step_2.db'
    while not os.path.exists(input_db):
        time.sleep(2)
    conn = sqlite3.connect(input_db)
    cursor = conn.execute('''select count(*) from tb_user_text;''')
    total_rec_count = cursor.fetchone()[0]
    print "total record count = " + str(total_rec_count)
    cursor = conn.execute('''select user_id, time_stamp, text_str from tb_user_text;''')
    output_json = dict()

    mode = 'sqlite'
    db_tmp_name = sys.argv[1] + '_step_3.db.tmp'
    db_name = sys.argv[1] + '_step_3.db'
    db_conn = setup_sqlite(db_tmp_name)
    #mode = raw_input("Enter 'json' or 'sqlite' for output:")
    #if mode == 'sqlite':
    #    db_conn = setup_sqlite()
    db_rec_count_1 = 0
    db_rec_count_2 = 0
    db_rec_set = []

    for row in cursor:
        row_len = len(row[2])
        sent_list = []
        if 15 <= row_len <= 500:
            #print "Text length: %s\n\tOrg text:%s" % (row_len, row[2])
            txt = denoise_text(row[2])
            text_str = sent_tokenize(txt)
            for i, sent in enumerate(text_str, start=1):
                sent = remove_between_quotes(sent)
                sent = pre_clean(sent)
                words = word_tokenize(sent)
                if len(words) > 1:
                    new_w_list = []
                    for w in words:
                        if pp.match(w) is not None and str(w) != 'un' and len(w) < 15:
                            new_w_list.append(str(w))
                    if len(new_w_list) > 1:
                        new_sent = ' '.join(ww for ww in new_w_list)
                        new_sent = new_sent + '.'
                        #print "\tNew sents[%s]：%s" % (i, new_sent)
                        sent_list.append(new_sent)
            if len(sent_list) > 0:
                if mode == 'json':
                    add_to_output_json(str(row[0]), str(row[1]), sent_list)
                    with open('output.json', 'w') as outfile:
                        json.dump(output_json, outfile)
                    outfile.close()
                else:
                    db_rec_set.append([str(row[0]), str(row[1]), sent_list])

    if mode == 'sqlite':
        for rec in db_rec_set:
            add_to_output_sqlite(db_conn, rec[0], rec[1], rec[2])
            db_rec_count_1 += 1
            if (int(db_rec_count_1 + db_rec_count_2*100.0) % 10000) == 0:
                db_conn.commit()
                #print "[DBG]: write to db!"
            if db_rec_count_1 >= 100.0:
                db_rec_count_1 = 0
                db_rec_count_2 += 1
            prog = "{:.0%}".format((db_rec_count_1 + db_rec_count_2*100.0)/total_rec_count)
            sys.stdout.write("\r")
            sys.stdout.write(prog)
            sys.stdout.flush()
        db_conn.commit()
        db_conn.close()
        os.rename(db_tmp_name, db_name)

    conn.close()

main()
