from __future__ import print_function # necessary because Dataproc runs Python 2.7
import sys
import time
import logging
from contextlib import contextmanager
from hashlib import sha1
from pyspark import SparkContext

# Constants
LOCAL = True  # TODO: set dynamically

BUCKET_PATH = 'gs://dataproc-ec96d46c-3f60-46a2-acb4-066fe551dff8-europe-west2/'


# Password list taken from https://haveibeenpwned.com/Passwords, SHA 1, 9.18 GB, containing over
# half a billion pwned passwords

PWS_PATH = 'first_64M_pwned-passwords-ordered-by-hash.txt/'
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
    prefix: the prefix text to show
    """
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float('%.4f' % (end - start))
    logging.info('%s: elapsed seconds: %s', name, elapsed_seconds)


sc = SparkContext()
# stagemetrics = StageMetrics(spark)
print(sys.version_info)

print('Spark version %s running.' % sc.version)

# print 'Config values:'
# print sc.getConf().getAll()

print('Reading in files...')
with time_usage('Reading in files'):
    rawPasswordRDD = sc.textFile(PWS_PATH)
    rawCommonWordsRDD = sc.textFile(COMMONWORDS_PATH)

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

# parsedPasswordRDD.cache()
# parsedCommonWordsRDD.cache()

# ---------------------------------- SEARCH

print('Searching for words...')

# search for a single word
with time_usage('Searching for \'love\''):
    print('Searching for \'love\'')
    res = parsedPasswordRDD \
        .filter(lambda (pw, count): pw == sha1('love').hexdigest())
    print('Found %s entries' % res.count())

# search for a random word in most common words list
# insight: if parsedPasswordRDD is cached above, this operation takes factor 10 less time
# caching parsedCommonWordsRDD doesn't result in a performance improvement though
with time_usage('Searching for a random most common word'):
    mostCommonWord = parsedCommonWordsRDD.takeSample(False, 1)
    print('Searching for %s' % mostCommonWord[0][0])
    res = parsedPasswordRDD \
        .filter(lambda (pw, count): pw == mostCommonWord[0][1])
    print('Found %s entries' % res.count())

# search for every word in parsedCommonWordsRDD
# for this, the passwords rdd needs to be broadcast
# TODO: is this the only and most performant way of doing this?
broadcastPasswordRDD = sc.broadcast(parsedPasswordRDD.collectAsMap())
with time_usage('Searching for every most common word'):
    # helper function
    def search_for_word(word):
        # print('Searching for %s' % word[0])
        # broadcastPasswordRDD.value is a dict!
        if word[1] in broadcastPasswordRDD.value:
            return word[0], True
        else:
            return word[0], False
    res = parsedCommonWordsRDD \
        .map(search_for_word)
    print('Found these words:')
    print(res.filter(lambda (el, value): value is True).map(lambda (el, value): el).collect())

print('Finished.')
