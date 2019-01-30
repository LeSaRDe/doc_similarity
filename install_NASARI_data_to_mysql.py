#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import json

"""
CREATE DATABASE nasari;
CREATE TABLE nasari.unified (
    synset_id VARCHAR(255) NOT NULL,
    wiki varchar(255) NOT NULL,
	vector JSON NOT NULL ,
	PRIMARY KEY (synset_id)
);
CREATE TABLE nasari.lexical (
    synset_id VARCHAR(255) NOT NULL,
    wiki varchar(255) NOT NULL,
	vector JSON NOT NULL ,
	PRIMARY KEY (synset_id)
);
"""

import mysql.connector
db = mysql.connector.connect(host="localhost", user="root", passwd="PassWord", db="nasari")
cur = db.cursor()

# with open("NASARI_lexical_english/NASARI_lexical_english.txt", 'r') as outfile:
with open("NASARI_unified_english/NASARI_unified_english.txt", 'r') as outfile:
    for i, line in enumerate(outfile):
        line = re.split(r'\t+', line.rstrip('\n'))
        # query = "INSERT INTO nasari.lexical (synset_id, wiki, vector) VALUES (%s, %s, %s)"
        query = "INSERT INTO nasari.unified (synset_id, wiki, vector) VALUES (%s, %s, %s)"
        q_data = (line[0], line[1], json.dumps(line[2:]))
        cur.execute(query, q_data)
        if i % 5000 == 0:
            db.commit()

db.commit()
cur.close()
outfile.close()
