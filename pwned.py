from __future__ import print_function # necessary because Dataproc runs Python 2.7
# Constants
LOCAL = True # TODO: set dynamically

BUCKET_PATH = 'gs://dataproc-ec96d46c-3f60-46a2-acb4-066fe551dff8-europe-west2/'

PWS_PATH = 'first_64M_pwned-passwords-ordered-by-hash.txt/'
COMMONWORDS_PATH = '10000-most-common-words.txt'

# if script is executed on Dataproc, bucket path needs to be prepended
if LOCAL is False:
    PWS_PATH = BUCKET_PATH + PWS_PATH
    COMMONWORDS_PATH = BUCKET_PATH + COMMONWORDS_PATH

import sys
from pyspark import SparkConf, SparkContext, SQLContext

sc = SparkContext()

print(sys.version_info)

print('Spark version %s running.' % sc.version)

# print 'Config values:'
# print sc.getConf().getAll()

print('Reading in files...')

rawPasswordList = sc.textFile(PWS_PATH)
rawCommonWordsList = sc.textFile(COMMONWORDS_PATH)

print(rawPasswordList.take(1))

print('Files read.')




