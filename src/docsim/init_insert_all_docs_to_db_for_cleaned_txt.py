import os, fnmatch
# import re
import contractions
import time
import sqlite3
# from nltk.tokenize import sent_tokenize

DEBUG = False
# DOCS_ROOT = "/home/fcmeng/workspace/data/reuters21578/picked_docs/"
# conn = sqlite3.connect("/home/fcmeng/workspace/data/reuters.db")
DOCS_ROOT = "/home/fcmeng/workspace/data/bbc/picked_docs/"
conn = sqlite3.connect("/home/fcmeng/workspace/data/bbc.db")
cur = conn.cursor()

# pattern1 = re.compile("^[A-Za-z0-9]*$")
# pattern2 = re.compile(r'([\W0-9]+[\w]*|[\w]*[\W0-9]+)')
# pattern3 = re.compile(r'[\W0-9]+([\w]*[\W0-9]+')
# pattern3 = re.compile(r'[\W0-9]*([\W0-9]+[\w]+|[\w]+[\W0-9]+)+[\W0-9]*')


def insert_pre_ner_to_db(values, cnt):
    cur.execute("INSERT INTO docs(doc_id, pre_ner) VALUES (?, ?)", values)
    if cnt % 100 == 0:
        conn.commit()
        print "%s docs is processed." % cnt


def cleanup_invalid_lines(lines):
    cleaned_txt = ''
    for i, line in enumerate(lines):
        if i == 0:
            line = line.strip()+"."
        # The first or second line is "From:", remove this line, but keep the "Subject" line
        contractioned_line = contractions.fix(line)
        cleaned_txt = cleaned_txt + contractioned_line.replace('\n', ' ')
        cleaned_txt = cleaned_txt.decode("ascii", errors="ignore").encode()
    return cleaned_txt


def raw_txt_cleanup(path_to_txt_files):
    for i, filename in enumerate(find_files(path_to_txt_files, "*")):
        if DEBUG:
            print "\n\n==[Doc %s]==\n" % filename
        lines = open(filename, 'r').readlines()
        pre_ner_txt = cleanup_invalid_lines(lines)
        if DEBUG:
            print "PRE NER txt:\n%s" % pre_ner_txt
        if not DEBUG:
            insert_pre_ner_to_db((filename.replace(DOCS_ROOT, '').replace('/', '_').replace('.txt', ''), pre_ner_txt), i)
    if not DEBUG:
        conn.commit()
        cur.close()
        conn.close()


def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename


def main():
    start = time.time()
    raw_txt_cleanup(DOCS_ROOT)
    print "\n\nTotal time: %s sec" % (time.time() - start)


if __name__ == "__main__":
    main()