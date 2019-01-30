#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
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

# wordnet_lemmatizer = WordNetLemmatizer()
# print wordnet_lemmatizer.lemmatize("wamon")

# lancaster_stemmer = LancasterStemmer()
# print lancaster_stemmer.stem("learned")

# porter_stemmer = PorterStemmer()
# print porter_stemmer.stem("worn")


p = re.compile("[a-zA-Z0-9_]")
pp = re.compile("^[A-Za-z-,]*$")


def strip_html(text):
    soup = BeautifulSoup(text, "html.parser")
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


conn = sqlite3.connect('user_text_20170801_20170802.db')
cursor = conn.execute('''select user_id, time_stamp, text_str from tb_user_text limit 1000;''')
output_json = dict()


def add_to_output_json(user, timestamp, sentences):
    if user not in output_json.keys():
        output_json[user] = dict()
        output_json[user][timestamp] = sentences
    else:
        if timestamp not in output_json[user].keys():
            output_json[user][timestamp] = sentences
        else:
            output_json[user][timestamp] += sentences


for row in cursor:
    row_len = len(row[2])
    sent_list = []
    if 15 <= row_len <= 500:
        print "Text length: %s\n\tOrg text:%s" % (row_len, row[2])
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
                    print "\tNew sents[%s]：%s" % (i, new_sent)
                    sent_list.append(new_sent)
        if len(sent_list) > 0:
            add_to_output_json(str(row[0]), str(row[1]), sent_list)
conn.close()

with open('output.json', 'w') as outfile:
    json.dump(output_json, outfile)
outfile.close()
