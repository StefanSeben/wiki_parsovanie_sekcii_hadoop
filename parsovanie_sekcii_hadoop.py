############
############ DOWNLOAD
############

import os, os.path
import urllib.request
import bz2

if not os.path.exists("tmp"):
    os.mkdir("tmp")

print("checking wiki dump file...")

if not os.path.exists("tmp/skwiki-latest-pages-articles-multistream.xml.bz2"):
    print("downloading wiki dump file...")
    urllib.request.urlretrieve("https://dumps.wikimedia.org/skwiki/latest/skwiki-latest-pages-articles-multistream.xml.bz2", "tmp/skwiki-latest-pages-articles-multistream.xml.bz2")

if not os.path.exists("tmp/skwiki-20201001-pages-articles-multistream.xml"):
    print("extracting wiki dump file...")
    with open("tmp/skwiki-20201001-pages-articles-multistream.xml", 'wb') as new_file, bz2.BZ2File("tmp/skwiki-latest-pages-articles-multistream.xml.bz2", 'rb') as file:
        for data in iter(lambda : file.read(100 * 1024), b''):
            new_file.write(data)

############
############ PARSOVANIE
############

import re
import urllib.request
from pyspark.sql import SparkSession
import pyspark

def parse_sec_redirs(text):
    found = re.findall('{{((Main\|)|(Hlavný článok\|))(.*)}}', text)
    sec_names = []
    for f in found:
        sec_names.append(f[3]);
    return sec_names

def parse_redirs(redirect):
    return redirect._title if redirect != None else ()

def get_from(ptitle, redirs):
    r_from = []
    for redir in redirs:
        if ptitle == redirs[1] or ptitle in redirs[2]:
            r_from.append(redirs[0])
    return r_from

print("creating spark session...")

spark = SparkSession\
        .builder\
        .appName("PythonSort")\
        .config("packages", "com.databricks:spark-xml_2.11:0.10.0") \
        .getOrCreate()

print("parsing dump file...")

wikidump_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "mediawiki") \
    .option("rowTag", "page") \
    .load("tmp/skwiki-20201001-pages-articles-multistream.xml")

print("removing empty pages...")

wikidump_rdd = wikidump_df.rdd.filter(lambda row: row.revision != None and row.revision.text != None and row.revision.text._VALUE != None)

print("parsing sections and redirects...")

sec_redirs = wikidump_rdd \
             .map(lambda row: (row.title, row.redirect, parse_sec_redirs(row.revision.text._VALUE))) \
             .map(lambda row: (row[0], parse_redirs(row[1]), row[2])) \
             .filter(lambda row: row[2] != [] or row[1] != ()) \
             .collect()
        
print("extracting texts...")

texts_rdd = wikidump_rdd \
        .map(lambda row: (row.title, get_from(row.title, sec_redirs), row.revision.text._VALUE)) \
        .filter(lambda row: row[1] != [])

texts = texts_rdd.collect()

############
############ VYHLADAVANIE
############

from whoosh import index    
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser

schema = Schema(title=ID(stored=True),
                r_from=TEXT(stored=True),
                text=TEXT(analyzer=StemmingAnalyzer()),
                tags=KEYWORD)

print("creating reversed index...")

if not os.path.exists("indexdir"):
    os.mkdir("indexdir")
    
ix = index.create_in("indexdir", schema)
writer = ix.writer()

for text in texts: 
    writer.add_document(title=text[0], r_from=' '.join(map(str, text[1] + ", ")), text=text[2], tags=u"short")
writer.commit()

print("prepared to parse query")

qp = QueryParser("text", schema=ix.schema)

while True:
    q_input = input("Vyhladavanie: ")
    q = qp.parse(q_input)

    with ix.searcher() as s:
        results = s.search(q)

        for r in results:
            print(r)
