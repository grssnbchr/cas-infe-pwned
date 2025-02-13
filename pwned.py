# !/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function  # necessary because Dataproc runs Python 2.7
import sys
import time
import logging
from contextlib import contextmanager
from hashlib import sha1
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row

from pyspark.sql.functions import broadcast

# Constants
LOCAL = True  # TODO: set dynamically

BUCKET_PATH = 'gs://dataproc-ec96d46c-3f60-46a2-acb4-066fe551dff8-europe-west2/'

# Password list taken from https://haveibeenpwned.com/Passwords, SHA 1, 9.18 GB, containing over
# half a billion pwned passwords

#PWS_PATH = 'first_64M_pwned-passwords-ordered-by-hash.txt/'
#PWS_PATH = 'first_512M_pwned-passwords-ordered-by-hash.txt/'

PWS_PATH = 'pwned-passwords-ordered-by-hash.txt'
COMMONWORDS_PATH = '10000-most-common-words.txt'

# if script is executed on Dataproc, bucket path needs to be prepended
if LOCAL is False:
    PWS_PATH = BUCKET_PATH + PWS_PATH
    COMMONWORDS_PATH = BUCKET_PATH + COMMONWORDS_PATH

# Configure logging
logging.basicConfig(level=logging.INFO)


# Utilities
# Taken from https://github.com/databricks/benchmarks/blob/master/pandas/pyspark_benchmark.py
@contextmanager
def time_usage(name=""):
    """log the time usage in a code block
    name: the prefix text to show
    """
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float('%.4f' % (end - start))
    logging.info('%s: elapsed seconds: %s', name, elapsed_seconds)

# Approach using df and sql
def df_sql_approach():
    print('DF SQL APPROACH ----------------')
    print('Reading in files...')
    pw_schema = StructType([
        StructField("hashedpw", StringType(), False),
        StructField("h", IntegerType(), True)
    ])

    pw_df = sqlc.read.csv(PWS_PATH, header=False, schema=pw_schema, sep=':')
    common_words_schema = StructType([
        StructField("word", StringType(), False)
    ])
    # common_words_df = sqlc.createDataFrame(sc.textFile(COMMONWORDS_PATH), schema=common_words_schema)
    common_words_df = sqlc.read.csv(COMMONWORDS_PATH, header=False, schema=common_words_schema, sep='|')
    with time_usage('DF SQL APPROACH'):

        # The hashes of the common words are only needed when comparing
        # Idea: Use a function during sql query that hashes the words on-demand

        sqlc.registerFunction("sha1hash", lambda x: sha1(x).hexdigest().upper())
        sqlc.registerDataFrameAsTable(pw_df, "pw_df")
        sqlc.registerDataFrameAsTable(common_words_df, "common_words_df")

        # example of how to apply the hash function within sql
        # print('Example of executing hash function within sql:')
        # print(sqlc.sql("SELECT *, sha1hash(word) as hash FROM common_words_df").take(2))

        # we add the hashed column to each word first
        with time_usage('hashing common words by adding an extra column'):
            common_words_df = common_words_df.rdd \
                .map(lambda x: (x['word'], sha1(x['word']).hexdigest().upper())) \
                .toDF(['word', 'hashedword'])

        # print("search for single word...")
        # with time_usage("search for love ❤"):
        #     j = pw_df.filter(pw_df.hashedpw == sha1('love').hexdigest().upper())
        #     j.show(10000)
        #     print("Count: " + str(j.count()))

        print("joining tables...")
        with time_usage('Joining tables on the hash [forced broadcast]'):
            j = broadcast(common_words_df).join(pw_df, common_words_df.hashedword == pw_df.hashedpw) \
                .select(common_words_df['word'], pw_df['h'])
            j.orderBy("h", ascending=False).show(100)

#        with time_usage('Joining tables on the hash [no broadcast]'):
#            j = common_words_df.join(pw_df, common_words_df.hashedword == pw_df.hashedpw) \
#                .select(common_words_df['word'], pw_df['h'])
#            j.orderBy("h", ascending=False).show(100)
        print("Count: " + str(j.count()))

# Approach using lower level RDD
def rdd_approach():
    print('RDD APPROACH ----------------')
    print('Reading in files...')
    rawPasswordRDD = sc.textFile(PWS_PATH)
    rawCommonWordsRDD = sc.textFile(COMMONWORDS_PATH)
    with time_usage('RDD APPROACH'):

        print('Files read.')

        # ---------------------------------- PREPROCESSING

        print('Preprocessing...')

        # Function that parses an element of the raw RDD and returns a tuple (password, count)
        # Also, it lowercases the hash digest
        # If the given entry is not a valid string of the form 'password:count', it returns None
        def parse_password_entry(entry):
            try:
                (entry, index) = entry
                entry = entry.split(':')
                if len(entry) < 2:
                    raise Exception('RDD element %s cannot be parsed!' % index)
                return entry[0].lower(), entry[1]
            except Exception as e:
                print(e)

        # Function that parses an element of the raw most-common-word RDD and returns a tuple (word, sha1)
        def parse_common_word_entry(entry):
            return entry, sha1(entry).hexdigest()

        # Parse password list
        parsedPasswordRDD = rawPasswordRDD \
            .zipWithIndex() \
            .map(parse_password_entry) \
            .filter(lambda el: el is not None)

        # Parse common words list and compute SHA1 hash
        parsedCommonWordsRDD = rawCommonWordsRDD \
            .map(parse_common_word_entry)

        print('Preprocessing completed.')

        # ---------------------------------- SEARCH

        # search for a single word
        # with time_usage('Searching for \'love\''):
        #     print('Searching for \'love\'')
        #     res = parsedPasswordRDD \
        #         .filter(lambda (pw, count): pw == sha1('love').hexdigest())
        #     print('Found %s entries' % res.count())

        # search if hash exists in most common words
        # for this, the most-common-words rdd needs to be broadcast
        broadcastCommonWordsRDD = sc.broadcast(parsedCommonWordsRDD.map(lambda word: (word[1], word[0])).collectAsMap())
        with time_usage('Searching for every most common word [broadcasted]'):
            # helper function
            def search_for_word(word):
                # print('Searching for %s' % word[0])
                # broadcastPasswordRDD.value is a dict!
                if word[0] in broadcastCommonWordsRDD.value:
                    # return tuple of word, count
                    return broadcastCommonWordsRDD.value[word[0]], word[1]
                else:
                    return word[0], False
            res = parsedPasswordRDD \
                .map(search_for_word)
            print('Found these words:')
            res = res.filter(lambda (el, value): value is not False)
            res_list = res.collect()
            print('There are %s common English words occurring in passwords.' % len(res_list))

        # Convert to DataFrame for better print output
        res = res.map(lambda (el, value): Row(hashedpw=el, h=int(value)))
        res_df = sqlc.createDataFrame(res)
        res_df.orderBy('h', ascending=False).show(100)


def count_all_occurrences():
    pw_schema = StructType([
        StructField("hashedpw", StringType(), False),
        StructField("h", IntegerType(), True)
    ])
    pw_df = sqlc.read.csv(PWS_PATH, header=False, schema=pw_schema, sep=':')
    common_words_schema = StructType([
        StructField("word", StringType(), False)
    ])
    sqlc.registerDataFrameAsTable(pw_df, "pw_df")
    sqlc.sql('Select sum(h) from pw_df').show()


# MAIN

sc_conf = SparkConf()
sc_conf.setAppName("pwned")
# sc_conf.set('spark.executor.memory', '2g')
# sc_conf.set('spark.driver.memory', '4g')
# sc_conf.set('spark.cores.max', '4')

sc_conf.set('spark.sql.crossJoin.enabled', True)

sc = SparkContext(conf=sc_conf)
sqlc = SQLContext(sc)
print(sys.version_info)

print('Spark version %s running.' % sc.version)

print('Config values of Spark context: ')
print(sc.getConf().getAll())


runner = rdd_approach, df_sql_approach, count_all_occurrences
for f in runner:
    f()

print('Finished.')
# hack to keep spark ui alive
raw_input("Press ctrl+c to exit")
