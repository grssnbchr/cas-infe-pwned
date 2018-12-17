# Constants
BUCKET_PATH = ''


from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from pyspark.context import SparkContext

sc = SparkContext()hereis
# spark = SparkSession(sc)



data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
dataRDD = sc.parallelize(data, 2)
dataRDD.cache()
print('Count: %s ' % dataRDD.count())
print( 'Take(5): %s' % dataRDD.take(5))
