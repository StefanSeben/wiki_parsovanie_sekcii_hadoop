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

def parse_redirs(row):
    return row.redirect._title if row.redirect != None else ()

print("creating spark session...")

spark = SparkSession\
        .builder\
        .appName("PythonSort")\
        .config("packages", "com.databricks:spark-xml_2.11:0.10.0") \
        .getOrCreate()

print("reading data...")

wikidump_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "mediawiki") \
    .option("rowTag", "page") \
    .load("tmp/skwiki-20201001-pages-articles-multistream.xml")

print("removing empty pages...")

wikidump_rdd = wikidump_df.rdd.filter(lambda row: row.revision != None and row.revision.text != None and row.revision.text._VALUE != None)

print("parsing sections...")

sec_redir_aa = wikidump_rdd \
               .map(lambda row: parse_sec_redirs(row.revision.text._VALUE)) \
               .filter(lambda row: row != []) \
               .collect()

print("parsing redirects...")

redirs = wikidump_rdd \
        .map(lambda x: parse_redirs(x)) \
        .filter(lambda row: row != ()) \
        .collect()

print("merging parsed sections and redirects...")

redir_secs = []
for aa in sec_redir_aa:
    for a in aa:
        redir_secs.append(a)
        
for redir in redirs:
    redir_secs.append(redir)
        
print("extracting texts...")

texts_rdd = wikidump_rdd \
        .filter(lambda row: row.title in redir_secs) \
        .map(lambda row: (row.title, row.revision.text._VALUE))

texts = texts_rdd.collect()

############
############ VYHLADAVANIE
############

from whoosh import index    
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser

schema = Schema(title=ID(stored=True),
                text=TEXT(analyzer=StemmingAnalyzer()),
                tags=KEYWORD)

print("creating reversed index...")

if not os.path.exists("indexdir"):
    os.mkdir("indexdir")
    
ix = index.create_in("indexdir", schema)
writer = ix.writer()

for text in texts: 
    writer.add_document(title=text[0], text=text[1], tags=u"short")
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
